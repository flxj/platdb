package platdb

import scala.util.{Success,Failure,Try}

trait Transaction2:
    // return current transactions identity.
    def id:Long
    // readonly or read-write transaction.
    def writable:Boolean
    // if the transaction is closed.
    def closed:Boolean
    // commit transaction,if its already closed then throw an exception.
    def commit():Try[Boolean]
    // rollback transaction,if its already closed then throw an exception.
    def rollback():Try[Boolean]
    // open a bucket,if not exists will throw an exception.
    def openBucket(name:String):Try[Bucket]
    // create a bucket,if already exists will throw an exception.
    def createBucket(name:String):Try[Bucket]
    // create a bucket,if exists then return the bucket.
    def createBucketIfNotExists(name:String):Try[Bucket]
    // delete a bucket,if bucket not exists will throw an exception.
    def deleteBucket(name:String):Try[Boolean]

    // ZSet methods
    def openZSet(name:String):Try[ZSet]
    def createZSet(name:String):Try[ZSet]
    def createZSetIfNotExists(name:String):Try[ZSet]
    def deleteZSet(name:String):Try[Unit]

    // list methods.
    def openList(name:String):Try[BList]
    def createList(name:String):Try[BList]
    def createListIfNotExists(name:String):Try[BList]
    def deleteBList(name:String):Try[Unit]


private[platdb] class BTreeSet(var bk:BTreeBucket) extends ZSet:
    def name:String = bk.name
    def length:Long = bk.length
    def closed:Boolean = bk.closed
    def contains(key:String):Try[Boolean] = bk.contains(key)
    def iterator:CollectionIterator = null // TODO 
    def -(key:String):Unit = bk-(key)
    def +(key:String):Unit = bk+(key,"")
    def add(keys:String*):Try[Unit] = 
        try 
            bk+(for k<- keys yield (k,""))
            Success(None)
        catch
            case e:Exception => Failure(e)
    def remove(keys:String*):Try[Unit] = 
        try 
            bk-(keys)
            Success(None)
        catch
            case e:Exception => Failure(e)
    def intersect(set:ZSet):Try[ZSet] = ???
    def intersect(set:Set[String]):Try[ZSet] = ???
    def union(set:ZSet):Try[ZSet] = ???
    def union(set:Set[String]):Try[ZSet] = ???
    def difference(set:ZSet):Try[ZSet] = ???
    def difference(set:Set[String]):Try[ZSet] = ???

private[platdb] class BTreeSetIter extends CollectionIterator:
    def find(key:String):(Option[String],Option[String]) = ???
    def first():(Option[String],Option[String])  = ???
    def last():(Option[String],Option[String]) = ???
    def hasNext():Boolean= ???
    def next():(Option[String],Option[String])= ???
    def hasPrev():Boolean = ???
    def prev():(Option[String],Option[String]) = ???
