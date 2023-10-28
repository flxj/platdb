package platdb

import scala.collection.immutable.{Set}
import scala.util.{Try,Success,Failure}


// collection data type.
private[platdb] val bucketDataType:Byte = 1
private[platdb] val bsetDataType:Byte = 2
private[platdb] val blistDataType:Byte = 3

private[platdb] def dataTypeName(t:Byte):String = 
    t match
        case 0 => "Bucket"
        case 1 => "BSet"
        case 2 => "BList"
        case _ => "Unknown"

/**
  * CollectionIterator is used to traverse platdb data structs.
  */
trait CollectionIterator extends Iterator[(Option[String],Option[String])]:
    /**
      * Retrieves the specified element.
      *
      * @param key
      * @return
      */
    def find(key:String):(Option[String],Option[String])
    /**
      * moves the iterator to the first item in the bucket and returns its key and value.
      *
      * @return
      */
    def first():(Option[String],Option[String]) 
    /**
      * moves the iterator to the last item in the bucket and returns its key and value.
      *
      * @return
      */
    def last():(Option[String],Option[String]) 
    /**
      * Determine if there is the next element of the current iterator.
      *
      * @return
      */
    def hasNext():Boolean
    /**
      * moves the iterator to the next item in the bucket and returns its key and value.
      *
      * @return
      */
    def next():(Option[String],Option[String])
    /**
      * Determine whether the current iterator has the previous element.
      *
      * @return
      */
    def hasPrev():Boolean 
    /**
      * moves the iterator to the previous item in the bucket and returns its key and value.
      *
      * @return
      */
    def prev():(Option[String],Option[String]) 

trait Iterable:
    /**
      * the elements number of current collection.
      *
      * @return
      */
    def length:Long
    /**
      * return a collection iterator object.
      *
      * @return
      */
    def iterator:CollectionIterator


/**
  * Bucket represents an ordered (lexicographic) set of key-value pairs.
  */
trait Bucket extends Iterable:
    def name:String
    /**
      * 
      *
      * @param key
      * @return
      */
    def contains(key:String):Try[Boolean]
    /**
      * 
      *
      * @param key
      * @return
      */
    def get(key:String):Try[String]
    /**
      * 
      *
      * @param key
      * @param defalutValue
      * @return
      */
    def getOrElse(key:String,defalutValue:String):String
    /**
      * 
      *
      * @param key
      * @param value
      * @return
      */
    def put(key:String,value:String):Try[Unit]
    /**
      * 
      *
      * @param key
      * @return
      */
    def delete(key:String):Try[Unit]
    /**
      * 
      *
      * @param name
      * @return
      */
    def getBucket(name:String):Try[Bucket]
    /**
      * 
      *
      * @param name
      * @return
      */
    def createBucket(name:String):Try[Bucket]
    /**
      * 
      *
      * @param name
      * @return
      */
    def createBucketIfNotExists(name:String):Try[Bucket] 
    /**
      * 
      *
      * @param name
      * @return
      */
    def deleteBucket(name:String):Try[Unit]
    /**
      * 
      *
      * @param key
      * @return
      */
    def apply(key:String):String
    /**
      * 
      *
      * @param key
      * @param value
      */
    def +(key:String,value:String):Unit
    /**
      * 
      *
      * @param elems
      */
    def +(elems:Seq[(String,String)]):Unit
    /**
      * 
      *
      * @param key
      */
    def -(key:String):Unit
    /**
      * 
      *
      * @param keys
      */
    def -(keys:Seq[String]):Unit


/**
  * 
  */
trait BSet extends Iterable:
    def name:String
    def contains(key:String):Try[Boolean]
    def add(keys:String*):Try[Unit]
    def remove(keys:String*):Try[Unit]
    def +(key:String):Unit
    def -(key:String):Unit
    def intersect(set:BSet):Try[BSet]
    def intersect(set:Set[String]):Try[BSet]
    def union(set:BSet):Try[BSet]
    def union(set:Set[String]):Try[BSet]
    def difference(set:BSet):Try[BSet]
    def difference(set:Set[String]):Try[BSet]


/**
  * 
  */
trait BList extends Iterable:
    def name:String 
    def isEmpty: Boolean
    def get(idx:Int):Try[String]
    def head:Try[String]
    def last:Try[String]
    def slice(from:Int,until:Int):Try[BList]
    def reverse:BList
    def init:BList
    def tail:BList
    def filter(p:(String) => Boolean): BList
    def find(p:(String) => Boolean):Int
    def take(n: Int): Try[BList]
    def takeRight(n: Int): Try[BList]
    def drop(n: Int): Try[Unit]
    def dropRight(n: Int): Try[Unit]
    def insert(index: Int, elem:String): Try[Unit]
    def insert(index: Int, elems:Seq[String]): Try[Unit]
    def append(elem:String):Try[Unit]
    def append(elems:Seq[String]):Try[Unit]
    def prepend(elem: String):Try[Unit]
    def prepend(elems: Seq[String]):Try[Unit]
    def remove(index: Int): Try[Unit]
    def remove(index: Int, count: Int):Try[Unit]
    def update(index: Int, elem:String): Try[Unit]
    def exists(p:(String) => Boolean): Boolean
    def apply(n: Int):String
    def :+(elem:String):Unit
    def +:(elem:String):Unit
    

/**
  * some collection methods with transaction parameter.
  */
object Collection:
    /**
      * This method has the same meaning as the openBucket method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      * @return
      */
    def openBucket(name:String)(using tx:Transaction):Bucket =
        tx.openBucket(name) match
            case Success(bk) => bk
            case Failure(e) => throw e 
    /**
      * This method has the same meaning as the createBucket method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      * @return
      */
    def createBucket(name:String)(using tx:Transaction):Bucket =
        tx.createBucket(name) match
            case Success(bk) => bk
            case Failure(e) => throw e
    /**
      * This method has the same meaning as the createBucketIfNotExists method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      * @return
      */
    def createBucketIfNotExists(name:String)(using tx:Transaction):Bucket =
        tx.createBucketIfNotExists(name) match
            case Success(bk) => bk
            case Failure(e) => throw e
    /**
      * This method has the same meaning as the deleteBucket method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      */
    def deleteBucket(name:String)(using tx:Transaction):Unit = 
        tx.deleteBucket(name) match
            case Success(_) => None
            case Failure(e) => throw e
    /**
      * This method has the same meaning as the openSet method of the Transaction trait,
      *  but it may throw an exception
      *
      * @param name
      * @param tx
      * @return
      */
    def openSet(name:String)(using tx:Transaction):BSet =
        tx.openBSet(name) match
            case Success(set) => set
            case Failure(e) => throw e 
    /**
      * This method has the same meaning as the createSet method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      * @return
      */
    def createSet(name:String)(using tx:Transaction):BSet =
        tx.createBSet(name) match
            case Success(set) => set
            case Failure(e) => throw e
    /**
      * This method has the same meaning as the createSetIfNotExists method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      * @return
      */
    def createSetIfNotExists(name:String)(using tx:Transaction):BSet =
        tx.createBSetIfNotExists(name) match
            case Success(set) => set
            case Failure(e) => throw e
    /**
      * This method has the same meaning as the deleteSet method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      */
    def deleteSet(name:String)(using tx:Transaction):Unit = 
        tx.deleteBSet(name) match
            case Success(_) => None
            case Failure(e) => throw e
    /**
      * This method has the same meaning as the openList method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      */
    def openList(name:String)(using tx:Transaction):BList =
        tx.openList(name) match
            case Success(bk) => bk
            case Failure(e) => throw e 
    /**
      * This method has the same meaning as the createList method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      */
    def createList(name:String)(using tx:Transaction):BList =
        tx.createList(name) match
            case Success(bk) => bk
            case Failure(e) => throw e
    /**
      * This method has the same meaning as the createListIfNotExists method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      */
    def createListIfNotExists(name:String)(using tx:Transaction):BList =
        tx.createListIfNotExists(name) match
            case Success(bk) => bk
            case Failure(e) => throw e
    /**
      * This method has the same meaning as the deleteList method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      */
    def deleteList(name:String)(using tx:Transaction):Unit = 
        tx.deleteList(name) match
            case Success(_) => None
            case Failure(e) => throw e
