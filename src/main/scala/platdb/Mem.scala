package platdb

import scala.util.{Try}

class MemDB(val name:String,val path:String):
    var store:DB = null

private[platdb] class MemTx( val id:Int) extends Transaction:
    def closed: Boolean = ???
    def commit(): Try[Boolean] = ???
    def createBucket(name: String): Try[Bucket] = ???
    def createBucketIfNotExists(name: String): Try[Bucket] = ???
    def deleteBucket(name: String): Try[Boolean] = ???
    def openBucket(name: String): Try[Bucket] = ???
    def rollback(): Try[Boolean] = ???
    def writable: Boolean = ???

// TODO 实现一种内存桶,使用自适应基数树，内存桶的内容能持久化到普通桶中
private[platdb] class MemBucket(val path:String) extends Bucket:
    var db:MemDB = null
    var tx:MemTx = null

    def +(key: String, value: String): Unit = ???
    def +(elems: Seq[(String, String)]): Unit = ???
    def -(key: String): Unit = ???
    def -(keys: Seq[String]): Unit = ???
    def apply(key: String): String = ???
    def closed: Boolean = ???
    def get(key: String): Try[String] = ???
    def put(key: String, value: String): Try[Boolean] = ???
    def contains(key: String): Try[Boolean] = ???
    def createBucket(name: String): Try[Bucket] = ???
    def createBucketIfNotExists(name: String): Try[Bucket] = ???
    def delete(key: String): Try[Boolean] = ???
    def deleteBucket(name: String): Try[Boolean] = ???
    def getBucket(name: String): Try[Bucket] = ???
    def iterator: BucketIterator = ???
    def length: Int = ???
    def name: String = ???

    // 定义一个新trait
    def writeTo(path:String):Unit = None // 将该内存map持久化到某个文件中 --> 新建一个磁盘DB对象，然后向其写入当前桶的所有内容 （注意版本）
    def appendTo(path:String):Unit = None // 追加到某个文件

class Set[K]:
    var bk:MemBucket = null 

class PriorityQueue[K,O]:
    var tx:MemTx = null 

class FIFO[K,O]:
    var pq:PriorityQueue[K,O] = null