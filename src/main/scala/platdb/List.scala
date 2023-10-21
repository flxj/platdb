package platdb

import scala.collection.mutable.ArrayBuffer
import scala.util.{Try,Success,Failure}

// 基于btree bucket 实现Blist trait
// 使用数字做key 
// 用一个列表作为索引维护key 的基本信息，比如最大key,最小key等，已经删除的key等，，该列表将存储为btree里的一个特殊元素

// 支持列表的切片，插入，删除，前后添加元素，前后移除元素等List/Array常规操作

private[platdb] object BTreeList:
    val indexElementValue = 20
    def apply(bk:BTreeBucket):Option[BTreeList] =
        bk.get("index") match
            case Failure(e) => None
            case Success(value) =>
                // TODO parse index info from value
                None


private[platdb] class BTreeList(var bk:BTreeBucket) extends BList:
    private var index:ArrayBuffer[(Long,Long,Int)] = new ArrayBuffer[(Long,Long,Int)]()
    
    def name: String = ???
    def length: Long = ???
    def iterator: CollectionIterator = ???
    def isEmpty: Boolean = ???
    def head:Try[(String,String)] = ???
    def last:Try[(String,String)] = ???
    def slice(from:Int,until:Int):BList = ???
    def reverse:BList = ???
    def init:BList = ???
    def tail:BList = ???
    def filter(p:(String,String) => Boolean): BList = ???
    def take(n: Int): BList = ???
    def takeRight(n: Int): BList = ???
    def drop(n: Int): Try[Unit] = ???
    def dropRight(n: Int): Try[Unit] = ???
    def insert(index: Int, elem:(String,String)): Try[Unit] = ???
    def insert(index: Int, elems: Seq[(String,String)]): Try[Unit] = ???
    def prepend(elem: (String,String)):Try[Unit] = ???
    def append(elem: (String,String)):Try[Unit] = ???
    def remove(index: Int): Try[Unit] = ???
    def remove(index: Int, count: Int):Try[Unit] = ???
    def update(index: Int, elem:(String,String)): Try[Unit] = ???

    def exists(p:(String,String) => Boolean): Boolean = ???
    def apply(n: Int):String = ???
    def :+(elem:(String,String)):Unit = ???
    def +:(elem:(String,String)):Unit = ???
