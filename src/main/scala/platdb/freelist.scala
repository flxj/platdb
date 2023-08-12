package platdb

import scala.collection.mutable.{ArrayBuffer}

private[platdb] class Meta extends Persistence:
	val id:Int 
    var pageSize:Int
    var flags:Int
    var root: bucketValue 
    var freelistId:Int // 记录freelist block的pgid
    var blockId:Int
    var txid:Int

	def size:Int
	def block():Block 
	def writeTo(bk:Block):Int

val freelistHeaderSize = 100
val freelistElementSize = 50 

/*

FreelistHeader: 记录freelist block的基本信息， 包括元素个数，freelist类型（hash/list），元素size, overflow值等

FreelistElement: 每个元素记录一个或一段(连续的一段)处于free状态的pageid值，并且按照pgid升序排列 ---> 在存block时候将转化为date字段

freelist block的结构就是： BlockHeader+Data(FreelistHeader+FreelistElement)， 它没有BlockIndex

*/

private[platdb] case class FreeFragment(start:Int,end:Int,length:Int)
private[platdb] case class FreeClaim(txid:Int, ids:Array[Int])

private[platdb] class Freelist extends Persistence:
	var header:BlockHeader
    var idle:ArrayBuffer[FreeFragment] = _
	var unleashing:ArrayBuff[FreeClaim] = _ 

	def size():Int = 
		blockHeaderSize + freelistHeaderSize + idle.length*freelistElementSize
	
	def unleash(txid:Int):Unit // 将unleashing集合中所有<=txid的freeClaim的ids移动到idle队列
	def free(txid:Int,startid:Int,tail:Int):Unit = None //  释放以startid为起始pageid的(tail+1)个连续的page空间
	def rollbackFree(txid:Int):Uint // 回滚对txid相关的释放
	def allocate(txid:Int,n:Int):Int = 0 // 分配大小为n*pageSize的连续空间，并返回第一个page空间的pgid, 如果当前idle列表内没有满足条件的空间，那么应该尝试增长文件，并将新增长的

	def block():Block = None
	def writeTo(bk:Block):Option[Int]


object Freelist:
	def read(bk:Block):Freelist = None

