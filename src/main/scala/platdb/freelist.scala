package platdb

import scala.collection.mutable.{ArrayBuffer}

val metaPageSize = 100

private[platdb] object Meta:
	def read(bk:Block):Option[Meta] =
		if bk.header.flag!= metaType then 
			return None 
		bk.tail match
			case None => None 
			case Some(data) => 
				var meta = new Meta(bk.id)
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
	var root: bucketValue 

	def size:Int = blockHeaderSize+metaPageSize
	def block():Block = None 
	def writeTo(bk:Block):Int =
		bk.header.id = id 
		bk.header.flag = flags
		bk.header.overflow = 0
		bk.header.count = 1
		bk.header.size = size
        bk.append(Block.marshalHeader(bk.header))
        bk.write(bk.size,Meta.marshal(this))
		bk.size
		

val freelistHeaderSize = 100
val freelistElementSize = 100
val freelistTypeList = 0
val freelistTypeHash = 0


// record freelist basic info, for example, count | type
private[platdb] case class FreelistHeader(count:Int,ftype:Int)
// record a file free page fragement
private[platdb] case class FreeFragment(start:Int,end:Int,length:Int)
// record a file pages free claim about a version txid
private[platdb] case class FreeClaim(txid:Int, ids:ArrayBuff[FreeFragment])

// freelist implement
private[platdb] class Freelist extends Persistence:
    var idle:ArrayBuffer[FreeFragment] = _
	var unleashing:ArrayBuff[FreeClaim] = _ 

	def size():Int = 
		var sz = blockHeaderSize + freelistHeaderSize + idle.length*freelistElementSize
		for fc <- unleashing do
			sz+= fc.ids.length*freelistElementSize
		sz 
	
	// 将unleashing集合中所有<=txid的freeClaim的ids移动到idle队列
	def unleash(txid:Int):Unit 

	//  释放以startid为起始pageid的(tail+1)个连续的page空间
	def free(txid:Int,startid:Int,tail:Int):Unit = None 

	// 回滚对txid相关的释放
	def rollbackFree(txid:Int):Uint 

	// 分配大小为n*pageSize的连续空间，并返回第一个page空间的pgid, 如果当前idle列表内没有满足条件的空间，那么应该尝试增长文件，并将新增长的
	def allocate(txid:Int,n:Int):Int = 0 
    // 合并unleashing和idle，并排序
	def merge():ArrayBuffer[FreeFragment] = 
		var arr = new Array[FreeFragment](idle.length)
		idle.copyToArray(arr,0)
		for fc <- unleashing do
			arr++=fc.ids 
        arr.sortWith((f1:FreeFragment,f2:FreeFragment) => f1.start < f2.start)
		// TODO 合并
		var mg = new ArrayBuff[FreeFragment]()
		var i = 0
		while i<arr.length do
			var j = i+1
			while j<arr.length && arr(j-1).end == arr(j).start -1 do
				j++
			if j!=i+1 then
                mg+=FreeFragment(arr(i).start,arr(j).end,arr(j).end-arr(i).start+1)
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
	def read(bk:Block):Option[Freelist] = None
	def marshalHeader(hd:FreelistHeader):Array[Byte] = 
		var buf:ByteBuffer = ByteBuffer.allocate(freelistHeaderSize)
        buf.putInt(hd.count)
        buf.putInt(hd.ftype)
        buf.array()
	def marshalElement(ff:FreelistFragment):Array[Byte] = 
		var buf:ByteBuffer = ByteBuffer.allocate(freelistElementSize)
        buf.putInt(ff.start)
        buf.putInt(ff.end)
		buf.putInt(ff.length)
        buf.array()

