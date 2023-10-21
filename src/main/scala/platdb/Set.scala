package platdb

import scala.util.{Success,Failure,Try}
import scala.collection.mutable.TreeMap

/**
  * 
  *
  * @param bk
  */
private[platdb] class BTreeSet(var bk:BTreeBucket) extends ZSet:
    def name:String = bk.name
    def length:Long = bk.length
    def closed:Boolean = bk.closed
    def contains(key:String):Try[Boolean] = bk.contains(key)
    def iterator:CollectionIterator = new BTreeSetIter(new BTreeBucketIter(bk)) 
    def -(key:String):Unit = bk-(key)
    def +(key:String):Unit = bk+(key,"")
    /**
      * 
      *
      * @param keys
      * @return
      */
    def add(keys:String*):Try[Unit] = 
        try 
            bk+(for k<- keys yield (k,""))
            Success(None)
        catch
            case e:Exception => Failure(e)
    /**
      * 
      *
      * @param keys
      * @return
      */
    def remove(keys:String*):Try[Unit] = 
        try 
            bk-(keys)
            Success(None)
        catch
            case e:Exception => Failure(e)
    /**
      * 
      *
      * @param set
      * @return
      */
    def intersect(set:ZSet):Try[ZSet] = 
        var tempSet = new TempZSet("intersect")
        try
            for (k,_) <- set.iterator do
                k match
                    case None => None
                    case Some(key) =>
                        contains(key) match
                            case Failure(e) => throw e
                            case Success(in) if in => tempSet+(key)
                            case _ => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def intersect(set:Set[String]):Try[ZSet] = 
        var tempSet = new TempZSet("intersect")
        try
            for (k,_) <- iterator do
                k match
                    case Some(key) if set.contains(key)  => tempSet+(key)
                    case _ => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    /**
      * 
      *
      * @param set
      * @return
      */
    def union(set:ZSet):Try[ZSet] = 
        var tempSet = new TempZSet("union")
        try
            for (k,_) <- iterator do
                k match
                    case Some(key) => tempSet+(key)
                    case None => None
            for (k,_) <- set.iterator do
                k match
                    case Some(key) => tempSet+(key)
                    case None => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def union(set:Set[String]):Try[ZSet] = 
        var tempSet = new TempZSet("union")
        try
            for (k,_) <- iterator do
                k match
                    case Some(key) => tempSet+(key)
                    case None => None
            for k <- set.iterator do
                tempSet+(k)
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    /**
      * 
      *
      * @param set
      * @return
      */
    def difference(set:ZSet):Try[ZSet] = 
        var tempSet = new TempZSet("difference")
        try
            for (k,_) <- set.iterator do
                k match
                    case None => None
                    case Some(key) =>
                        contains(key) match
                            case Failure(e) => throw e
                            case Success(in) if !in => tempSet+(key)
                            case _ => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)

    def difference(set:Set[String]):Try[ZSet] = 
        var tempSet = new TempZSet("difference")
        try
            for k <- set.iterator do
                contains(k) match
                    case Failure(e) => throw e
                    case Success(in) if !in => tempSet+(k)
                    case _ => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)

/**
  * 
  *
  * @param iter
  */
private[platdb] class BTreeSetIter(val iter:BTreeBucketIter) extends CollectionIterator:
    def find(key:String):(Option[String],Option[String]) = 
        val (k,_) = iter.find(key) 
        (k,None)
    def first():(Option[String],Option[String])  = 
        val (k,_) = iter.first()
        (k,None)
    def last():(Option[String],Option[String]) = 
        val (k,_) = iter.last()
        (k,None)
    def hasNext():Boolean= iter.hasNext()
    def next():(Option[String],Option[String])= 
        val (k,_) = iter.next()
        (k,None)
    def hasPrev():Boolean = iter.hasNext()
    def prev():(Option[String],Option[String]) = 
        val (k,_) = iter.prev()
        (k,None)

/**
  * 
  *
  * @param name
  */
private[platdb] class TempZSet(val name:String) extends ZSet:
    var zset = new TreeMap[String,Boolean]()
    def length:Long = zset.size
    def contains(key:String):Try[Boolean] = Success(zset.contains(key))
    def add(keys:String*):Try[Unit] = Success(zset.addAll(for k <- keys yield (k,true)))
    def remove(keys:String*):Try[Unit] = Success(zset--=(keys))
    def +(key:String):Unit = zset+=(key,true)
    def -(key:String):Unit = zset-=key
    def iterator: CollectionIterator = new TempZSetIter(this)
    def intersect(set:ZSet):Try[ZSet] = 
        var tempSet = new TempZSet("intersect") // TODO generate a random name
        try
            for (k,_) <- set.iterator do
                k match
                    case Some(key) if zset.contains(key) => tempSet+(key)
                    case _ => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def intersect(set:Set[String]):Try[ZSet] = 
        var tempSet = new TempZSet("intersect")
        try
            for k <- set.iterator do
                if zset.contains(k) then tempSet+(k)
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def union(set:ZSet):Try[ZSet] = 
        var tempSet = new TempZSet("union")
        try
            for (k,_) <- set.iterator do
                k match
                    case Some(key) => tempSet+(key)
                    case None => None
            for (k,_) <- zset.iterator do
                tempSet+(k)
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def union(set:Set[String]):Try[ZSet] = 
        var tempSet = new TempZSet("union")
        try
            for k <- set.iterator do
                tempSet+(k)  
            for (k,_) <- zset.iterator do
                tempSet+(k)
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def difference(set:ZSet):Try[ZSet] = 
        var tempSet = new TempZSet("difference")
        try
            for (k,_) <- set.iterator do
                k match
                    case None => None
                    case Some(key) =>
                        contains(key) match
                            case Failure(e) => throw e
                            case Success(in) if !in => tempSet+(key)
                            case _ => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def difference(set:Set[String]):Try[ZSet] = 
        var tempSet = new TempZSet("difference")
        try
            for k <- set.iterator do
                if !zset.contains(k) then
                    tempSet+(k)
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
/**
  * 
  *
  * @param tempSet
  */
private[platdb] class TempZSetIter(val tempSet:TempZSet) extends CollectionIterator:
    private var iter = tempSet.zset.keysIterator
    def find(key:String):(Option[String],Option[String]) = 
        if tempSet.zset.contains(key) then (Some(key),None) else (None,None)
    def first():(Option[String],Option[String])  = (Some(tempSet.zset.firstKey),None)
    def last():(Option[String],Option[String]) = (Some(tempSet.zset.lastKey),None)
    def hasNext():Boolean= iter.hasNext
    def next():(Option[String],Option[String])= (Some(iter.next()),None)
    def hasPrev():Boolean = false
    def prev():(Option[String],Option[String]) = (None,None)
