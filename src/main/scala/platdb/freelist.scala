package platdb

import java.nio.ByteBuffer
import scala.collection.mutable.{ArrayBuffer,Map}
import scala.util.control.Breaks._

val metaPageSize = 32

private[platdb] object Meta:
	def read(bk:Block):Option[Meta] =
		if bk.header.flag!= metaType then 
			return None 
		bk.tail match
			case None => None 
			case Some(data) => 
				var meta = new Meta(bk.pgid)
				if data.length!=metaPageSize then
					return None 
				var arr = new Array[Int](6)
				var s:Long = 0
				for i <- 0  until 6 do
					var n  = 0 
					for j <- 0 to 3 do
                        n = n << 8
                        n = n | (data(4*i+j) & 0xff)
                    arr(i) = n
				for i <- 0 to 7 do 
					s = s << 8
					n = n | (data(24+i) & 0xff)
				
				meta.pageSize = arr(0)
				meta.freelistId = arr(1)
				meta.blockId = arr(2)
				meta.txid = arr(3)
				meta.root = new bucketValue(arr(4),s,arr(5))
				Some(meta)

	def marshal(meta:Meta):Array[Byte] =
		var buf:ByteBuffer = ByteBuffer.allocate(metaPageSize)
        buf.putInt(meta.pageSize)
        buf.putInt(meta.freelistId)
        buf.putInt(meta.blockId)
        buf.putInt(meta.txid)
        buf.putInt(meta.root.id)
		buf.putInt(meta.root.count)
		buf.putLong(meta.root.sequence)
        buf.array()

private[platdb] class Meta(val id:Int) extends Persistence:
    var pageSize:Int
    var flags:Int
    var freelistId:Int // 记录freelist block的pgid
    var blockId:Int
    var txid:Int
	var root:bucketValue

	def size:Int = blockHeaderSize+metaPageSize
	def block():Block = None 
	def writeTo(bk:Block):Int =
		bk.header.pgid = id 
		bk.header.flag = flags
		bk.header.overflow = 0
		bk.header.count = 1
		bk.header.size = size
        bk.append(Block.marshalHeader(bk.header))
        bk.write(bk.size,Meta.marshal(this))
		bk.size

val freelistHeaderSize = 8
val freelistElementSize = 8
val freelistTypeList = 0
val freelistTypeHash = 1

// record freelist basic info, for example, count | type
private[platdb] case class FreelistHeader(count:Int,ftype:Int)
// record a file free page fragement
private[platdb] case class FreeFragment(start:Int,end:Int,length:Int)
// record a file pages free claim about a version txid
private[platdb] case class FreeClaim(txid:Int, ids:ArrayBuff[FreeFragment])

// freelist implement
private[platdb] class Freelist(val oldid:Int) extends Persistence:
    var idle:ArrayBuffer[FreeFragment] = _
	var unleashing:ArrayBuff[FreeClaim] = _ 
	var allocated:Map[Int,ArrayBuff[FreeFragment]] = _ // 记录事务的page分配情况

	def size():Int = 
		var sz = blockHeaderSize + freelistHeaderSize + idle.length*freelistElementSize
		for fc <- unleashing do
			sz+= fc.ids.length*freelistElementSize
		sz 
	
	// 将unleashing集合中所有<=txid的freeClaim的ids移动到idle队列
	// 下一个写事务在开始执行之前会尝试调用freelist释放pending中的page, 只要pending中待释放的版本小于当前打开的只读事务持有的最小版本
	// ，那末就可以释放这些pending元素 （表示当前肯定已经没有只读事务在持有这些待释放pages了）
    // 同时当前已经打开的只读事务持有的版本可能跨度较大， 那末对于两个相邻版本之间的版本， 如果已经没有事务在持有它，那末也是可以释放的
	def unleash(txid:Int):Unit =
		unleashing.sortWith((c1:FreeClaim,c2:FreeClaim)=> c1.txid < c2.txid)
		var i = 0
		while i<unleashing.length && unleashing(i).txid<=txid do
			i++
		for j <- 0 to i do
			idle++=unleashing(i).ids
		unleashing = unleashing.takeRight(unleashing.length-i-1)
		idle = reform()

	// 释放以startid为起始pageid的(tail+1)个连续的page空间
	// 写事务提交后可能会释放一些page, 这会在freelist的pending中添加一条记录 'txid: pageids' 表示txid版本的这些pageids需要释放
	def free(txid:Int,startid:Int,tail:Int):Unit = 
		val ff = FreeFragment(startid,startid+tail,tail+1)
		var idx = -1
		breakable(
			for (i,fc) <- unleashing do
			    if fc.txid == txid then
                    idx = i 
                    break()
        )
		if idx >=0 then 
			unleashing(idx).ids+=ff 
		else
			var fc = FreeClaim(txid,new ArrayBuff[FreeFragment](ff))
			unleashing+=fc 

	// 分配大小为n*pageSize的连续空间，并返回第一个page空间的pgid, 如果当前idle列表内没有满足条件的空间，那么应该尝试增长文件，并将新增长的
	def allocate(txid:Int,n:Int):Int = 
		var i = 0
		while i<idle.length && n>idle(i).length do
			i++
		if i>=idle.length then
			// TODO 应该将最大的pgid+1分配给当前请求
			return -1
		val ff = idle(i)
		if n == ff.length then
			idle = idle.slice(0,i) ++ idle.takeRight(i+1)
			if !allocated.contains(txid) then
				allocated.addOne((txid,new ArrayBuff[FreeFragment]()))
			allocated(txid) += ff 
	    else
		    val start = ff.start + n
			idle(i) = FreeFragment(start,ff.end,ff.end-start+1)
			if !allocated.contains(txid) then
				allocated.addOne((txid,new ArrayBuff[FreeFragment]()))
			allocated(txid) +=  FreeFragment(ff.start,start-1,n)
		ff.start  

	// 回滚对txid相关的释放/分配操作
	def rollback(txid:Int):Unit =
		// 归还已经分配的pages到idle
		if allocated.contains(txid) then
			val Some(fs) = allocated.remove(txid)
			idle++=fs
			idle = reform()
		
		// 撤销已经释放的声明
		var idx = -1
		breakable(
			for (i,fc) <- unleashing do
			    if fc.txid == txid then
                    idx = i 
                    break()
        )
		if idx >=0 then
			unleashing = unleashing.slice(0,idx)++unleashing.takeRight(idx+1)

    // 当释放一个版本的page,需要将对应的freefragment 插入idle，并排序
	private def reform():ArrayBuff[FreeFragment] =
		var arr = new Array[FreeFragment](idle.length)
		idle.copyToArray(arr,0)
        arr.sortWith((f1:FreeFragment,f2:FreeFragment) => f1.start < f2.start)
		makeIdle(arr)
    // merge unleashing and idle:
	private def merge():ArrayBuffer[FreeFragment] = 
		var arr = new Array[FreeFragment](idle.length)
		idle.copyToArray(arr,0)
		for fc <- unleashing do
			arr++=fc.ids 
		makeIdle(arr) 
    // merge FreeFragment array and sort it by pgid
	private def makeIdle(arr:Array[FreeFragment]):ArrayBuff[FreeFragment] =
		arr.sortWith((f1:FreeFragment,f2:FreeFragment) => f1.start < f2.start)
		// merge
		var mg = new ArrayBuff[FreeFragment]()
		var i = 0
		while i<arr.length do
			var j = i+1
			while j<arr.length && arr(j-1).end == arr(j).start -1 do
				j++
			if j!=i+1 then
                mg+=FreeFragment(arr(i).start,arr(j).end,arr(j).end-arr(i).start+1)
			else
				mg+=arr(i)
			i = j+1 
		mg.sortWith((f1:FreeFragment,f2:FreeFragment) => f1.length < f2.length || (f1.length == f2.length && f1.start < f2.start))
		mg
	//
	def block():Block = None
	def writeTo(bk:Block):Int =
		bk.header.flag = freelistType
		bk.header.count = 1
		bk.header.size = size
		var ovf = (size/osPageSize)-1
		if size%osPageSize!=0 then 
			ovf++
		bk.header.overflow = ovf

		val ids = mergeIds 
		bk.append(Block.marshalHeader(bk.header))
		bk.append(Freelist.marshalHeader(new FreelistHeader(ids.length,freelistTypeList)))
		for ff <- ids do
			bk.append(Freelist.marshalElement(ff))
		size

object Freelist:
	def read(bk:Block):Option[Freelist] = 
		if bk.header.flag!=freelistType then 
			return None 
		bk.tail match
			case None => None 
			case Some(data) => 
				var freelist = new Freelist(bk.pgid)
				if data.length < freelistHeaderSize then
					return None 
				unmarshalHeader(data.slice(0,freelistHeaderSize)) match
					case None => return None 
					case Some(hd) =>
						if data.length != freelistHeaderSize+(hd.count*freelistElementSize) then
							return None 
						freelist.idle = new ArrayBuffer[FreeFragment]()
						freelist.unleashing = new ArrayBuff[FreeClaim]()
						freelist.allocated = new Map[Int,ArrayBuffer[FreeFragment]]()

						var idx = freelistHeaderSize+freelistElementSize
						while idx <= data.length do
							unmarshalElement(data.slice(idx-freelistElementSize,idx)) match
								case None => return None 
								case Some(ff) =>
									freelist.idle+=ff 
						return Some(freelist)
		None 
    //
    def unmarshalHeader(data:Array[Byte]):Option[FreelistHeader] =
        var count = 0
		var ftype = 0
		for i <- 0 to 3 do
            count = count << 8
            count = count | (data(i) & 0xff)
			ftype = ftype << 8
			ftype = ftype | (data(4+i) & 0xff)
        Some(FreelistHeader(count,ftype))

	def marshalHeader(hd:FreelistHeader):Array[Byte] = 
		// TODO not use bytebuffer
		var buf:ByteBuffer = ByteBuffer.allocate(freelistHeaderSize)
        buf.putInt(hd.count)
        buf.putInt(hd.ftype)
        buf.array()
	//
	def unmarshalElement(data:Array[Byte]):Option[FreelistFragment] =
		var s = 0
		var e = 0
		for i <- 0 to 3 do
            s = s << 8
            s = s | (data(i) & 0xff)
			e = e << 8
			e = e | (data(4+i) & 0xff)
        Some(FreeFragment(s,e,e-s+1))
    //
	def marshalElement(ff:FreelistFragment):Array[Byte] = 
		// TODO not use bytebuffer
		var buf:ByteBuffer = ByteBuffer.allocate(freelistElementSize)
        buf.putInt(ff.start)
        buf.putInt(ff.end)
		//buf.putInt(ff.length)
        buf.array()
