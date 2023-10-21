package platdb

import scala.collection.immutable.{Set}
import scala.util.{Try,Success,Failure}

/**
  * 
  */
object DataType extends Enumeration:
    type DataType = Value 
    val Bucket = Value(1,"Bucket")
    val ZSet = Value(2,"ZSet")
    val BList = Value(3,"BList")
    val Unknown = Value(4,"Unknown")

    def parse(id:Int):DataType =
        id match
            case 1 => Bucket
            case 2 => ZSet
            case 3 => BList
            case _ => Unknown

/**
  * 
  */
trait ZSet extends Iterable:
    def name:String
    def contains(key:String):Try[Boolean]
    def add(keys:String*):Try[Unit]
    def remove(keys:String*):Try[Unit]
    def +(key:String):Unit
    def -(key:String):Unit

    def intersect(set:ZSet):Try[ZSet]
    def intersect(set:Set[String]):Try[ZSet]
    def union(set:ZSet):Try[ZSet]
    def union(set:Set[String]):Try[ZSet]
    def difference(set:ZSet):Try[ZSet]
    def difference(set:Set[String]):Try[ZSet]

/**
  * 
  */
trait Bucket extends Iterable:
    def name:String
    def contains(key:String):Try[Boolean]
    def get(key:String):Try[String]
    def put(key:String,value:String):Try[Boolean]
    def delete(key:String):Try[Boolean]
    def getBucket(name:String):Try[Bucket]
    def createBucket(name:String):Try[Bucket]
    def createBucketIfNotExists(name:String):Try[Bucket] 
    def deleteBucket(name:String):Try[Boolean]

    def getOrElse(key:String,defalutValue:String):String
    def apply(key:String):String
    def +(key:String,value:String):Unit
    def +(elems:Seq[(String,String)]):Unit
    def -(key:String):Unit
    def -(keys:Seq[String]):Unit

/**
  * 
  */
trait BList extends Iterable:
    def name:String 
    def isEmpty: Boolean
    def head:Try[(String,String)]
    def last:Try[(String,String)]
    def slice(from:Int,until:Int):BList
    def reverse:BList
    def init:BList
    def tail:BList
    def filter(p:(String,String) => Boolean): BList
    def take(n: Int): BList
    def takeRight(n: Int): BList
    def drop(n: Int): Try[Unit]
    def dropRight(n: Int): Try[Unit]
    def insert(index: Int, elem:(String,String)): Try[Unit]
    def insert(index: Int, elems: Seq[(String,String)]): Try[Unit]
    def prepend(elem: (String,String)):Try[Unit]
    def append(elem: (String,String)):Try[Unit]
    def remove(index: Int): Try[Unit]
    def remove(index: Int, count: Int):Try[Unit]
    def update(index: Int, elem:(String,String)): Try[Unit]

    def exists(p:(String,String) => Boolean): Boolean
    def apply(n: Int):String
    def :+(elem:(String,String)):Unit
    def +:(elem:(String,String)):Unit
    

/**
  * some collection methods with transaction parameter.
  */
object Collection:
    /**
      * 
      *
      * @param name
      * @param tx
      * @return
      */
    def openBucket(name:String)(using tx:Transaction):Bucket =
        tx.openBucket(name) match
            case Success(bk) => bk
            case Failure(e) => throw e 
    def createBucket(name:String)(using tx:Transaction):Bucket =
        tx.createBucket(name) match
            case Success(bk) => bk
            case Failure(e) => throw e
    def createBucketIfNotExists(name:String)(using tx:Transaction):Bucket =
        tx.createBucketIfNotExists(name) match
            case Success(bk) => bk
            case Failure(e) => throw e
    def deleteBucket(name:String)(using tx:Transaction):Boolean = 
        tx.deleteBucket(name) match
            case Success(_) => true
            case Failure(e) => throw e
    
    /**
      * 
      *
      * @param name
      * @param tx
      * @return
      */
    def openZSet(name:String)(using tx:Transaction):ZSet =
        tx.openZSet(name) match
            case Success(set) => set
            case Failure(e) => throw e 
    def createZSet(name:String)(using tx:Transaction):ZSet =
        tx.createZSet(name) match
            case Success(set) => set
            case Failure(e) => throw e
    def createZSetIfNotExists(name:String)(using tx:Transaction):ZSet =
        tx.createZSetIfNotExists(name) match
            case Success(set) => set
            case Failure(e) => throw e
    def deleteZSet(name:String)(using tx:Transaction):Boolean = 
        tx.deleteZSet(name) match
            case Success(_) => true
            case Failure(e) => throw e
    /**
      * 
      *
      * @param name
      * @param tx
      * @return
      */
    def openList(name:String)(using tx:Transaction):BList =
        tx.openList(name) match
            case Success(bk) => bk
            case Failure(e) => throw e 
    def createList(name:String)(using tx:Transaction):BList =
        tx.createList(name) match
            case Success(bk) => bk
            case Failure(e) => throw e
    def createListIfNotExists(name:String)(using tx:Transaction):BList =
        tx.createListIfNotExists(name) match
            case Success(bk) => bk
            case Failure(e) => throw e
    def deleteList(name:String)(using tx:Transaction):Boolean = 
        tx.deleteList(name) match
            case Success(_) => true
            case Failure(e) => throw e
