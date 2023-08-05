package platdb

import scala.collection.mutable.{ArrayBuffer}

class Meta extends Persistence:
    var pageSize:Int
    var flags:Int
    var root: bucketPage 
    var freelistId:Int // 记录freelist block的pgid
    var blockId:Int
    var txid:Int

	def block():Block 

/*
// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
type freelist struct {
	freelistType   FreelistType                // freelist type
	ids            []pgid                      // all free and available free page ids.
	allocs         map[pgid]txid               // mapping of txid that allocated a pgid.
	pending        map[txid]*txPending         // mapping of soon-to-be free page ids by tx.
	cache          map[pgid]bool               // fast lookup of all free and pending page ids.
	freemaps       map[uint64]pidSet           // key is the size of continuous pages(span), value is a set which contains the starting pgids of same size
	forwardMap     map[pgid]uint64             // key is start pgid, value is its span size
	backwardMap    map[pgid]uint64             // key is end pgid, value is its span size
	allocate       func(txid txid, n int) pgid // the freelist allocate func
	free_count     func() int                  // the function which gives you free page number
	mergeSpans     func(ids pgids)             // the mergeSpan func
	getFreePageIDs func() []pgid               // get free pgids func
	readIDs        func(pgids []pgid)          // readIDs func reads list of pages and init the freelist
}
*/

val freelistHeaderSize = 100
val freelistElementSize = 50 



/*

FreelistHeader: 记录freelist block的基本信息， 包括元素个数，freelist类型（hash/list），元素size, overflow值等

FreelistElement: 每个元素记录一个或一段(连续的一段)处于free状态的pageid值，并且按照pgid升序排列 ---> 在存block时候将转化为date字段

freelist block的结构就是： BlockHeader+Data(FreelistHeader+FreelistElement)， 它没有BlockIndex

*/

case class FreeFragment(start:Int,end:Int,length:Int)
case class FreeClaim(txid:Int, ids:Array[Int])

class Freelist extends Persistence:
    var idle:ArrayBuffer[FreeFragment] = _
	var unleashing:ArrayBuff[FreeClaim] = _ 

	def size():Int = 
		blockHeaderSize + freelistHeaderSize + idle.length*freelistElementSize
	
	def unleash(txid:Int):Unit // 将unleashing集合中所有<=txid的freeClaim的ids移动到idle队列
	def free(txid:Int,startid:Int,tail:Int):Unit = None //  释放以startid为起始pageid的(tail+1)个连续的page空间
	def allocate(txid:Int,n:Int):Int = 0 // 分配大小为n*pageSize的连续空间，并返回第一个page空间的pgid, 如果当前idle列表内没有满足条件的空间，那么应该尝试增长文件，并将新增长的

	def block():Block = None
	def writeTo(bk:Block):Option[Int]

object Freelist:
	def load(bk:Block):Freelist = None

