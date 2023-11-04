/*
 * Copyright (C) 2023 flxj(https://github.com/flxj)
 *
 * All Rights Reserved.
 *
 * Use of this source code is governed by an Apache-style
 * license that can be found in the LICENSE file.
 */
package platdb

import scala.util.{Success,Failure,Try}
import scala.collection.mutable.TreeMap

/**
  * 
  *
  * @param bk
  */
private[platdb] class BTreeSet(var bk:BTreeBucket) extends BSet:
    def name:String = bk.name
    def length:Long = bk.length
    def closed:Boolean = bk.closed
    def contains(key:String):Try[Boolean] = bk.contains(key)
    def iterator:CollectionIterator = new BTreeSetIter(new BTreeBucketIter(bk)) 
    def -=(key:String):Unit = bk-=(key)
    def +=(key:String):Unit = bk+=(key,"")
    /**
      * 
      *
      * @param keys
      * @return
      */
    def add(keys:String*):Try[Unit] = 
        try 
            bk+=(for k<- keys yield (k,""))
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
            bk-=(keys)
            Success(None)
        catch
            case e:Exception => Failure(e)
    /**
      * 
      *
      * @param set
      * @return
      */
    def and(set:BSet):Try[BSet] = 
        /*
        var tempSet = new TempBSet("intersect")
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
        */
        var tempSet = new TempBSet("intersect")
        try
            val it1 = iterator
            val it2 = set.iterator
            while it1.hasNext() && it2.hasNext() do
                val (k1,_) = it1.next()
                val (k2,_) = it2.next()
                (k1,k2) match
                    case (Some(key1),Some(key2)) =>
                        if key1 < key2 then
                            var continue = true
                            while continue && it1.hasNext() do
                                val (k,_) = it1.next()
                                k match 
                                    case Some(key) => 
                                        if key == key2 then
                                            tempSet+=(key)
                                        if key >= key2 then
                                            continue = false
                                    case None => None
                        else if key1 == key2 then
                            tempSet+=(key1)
                        else
                            var continue = true
                            while continue && it2.hasNext() do
                                val (k,_) = it2.next()
                                k match 
                                    case Some(key) => 
                                        if key == key1 then
                                            tempSet+=(key)
                                        if key >= key1 then
                                            continue = false
                                    case None => None
                    case _ => None
            
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
        
    def and(set:Set[String]):Try[BSet] = 
        var tempSet = new TempBSet("intersect")
        try
            for (k,_) <- iterator do
                k match
                    case Some(key) if set.contains(key)  => tempSet+=(key)
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
    def union(set:BSet):Try[BSet] = 
        var tempSet = new TempBSet("union")
        try
            for (k,_) <- iterator do
                k match
                    case Some(key) => tempSet+=(key)
                    case None => None
            for (k,_) <- set.iterator do
                k match
                    case Some(key) => tempSet+=(key)
                    case None => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
        
    def union(set:Set[String]):Try[BSet] = 
        var tempSet = new TempBSet("union")
        try
            for (k,_) <- iterator do
                k match
                    case Some(key) => tempSet+=(key)
                    case None => None
            for k <- set.iterator do
                tempSet+=(k)
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    /**
      * 
      *
      * @param set
      * @return
      */
    def diff(set:BSet):Try[BSet] = 
        var tempSet = new TempBSet("difference")
        try
            for (k,_) <- iterator do
                k match
                    case None => None
                    case Some(key) =>
                        set.contains(key) match
                            case Failure(e) => throw e
                            case Success(in) if !in => tempSet+=(key)
                            case _ => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)

    def diff(set:Set[String]):Try[BSet] = 
        var tempSet = new TempBSet("difference")
        try
            for (k,_) <- iterator do
                k match 
                    case Some(key) if !set.contains(key) => tempSet+=(key)
                    case _ => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    /*
    def &(set:BSet):BSet =
        this.and(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    
    def &(set:Set[String]):BSet =
        this.and(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset

    def |(set:BSet):BSet =
        this.union(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset

    def |(set:Set[String]):BSet =
        this.union(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    
    def -(set:BSet):BSet =
        this.diff(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    
    def -(set:Set[String]):BSet =
        this.diff(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    */

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
private[platdb] class TempBSet(val name:String) extends BSet:
    var map = new TreeMap[String,Boolean]()
    def length:Long = map.size
    def contains(key:String):Try[Boolean] = Success(map.contains(key))
    def add(keys:String*):Try[Unit] = Success(map.addAll(for k <- keys yield (k,true)))
    def remove(keys:String*):Try[Unit] = Success(map--=(keys))
    def +=(key:String):Unit = map+=(key,true)
    def -=(key:String):Unit = map-=key
    def iterator: CollectionIterator = new TempBSetIter(this)
    def and(set:BSet):Try[BSet] = 
        var tempSet = new TempBSet("intersect") // TODO generate a random name
        try
            for (k,_) <- set.iterator do
                k match
                    case Some(key) if map.contains(key) => tempSet+=(key)
                    case _ => None
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def and(set:Set[String]):Try[BSet] = 
        var tempSet = new TempBSet("intersect")
        try
            for k <- set.iterator do
                if map.contains(k) then tempSet+=(k)
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def union(set:BSet):Try[BSet] = 
        var tempSet = new TempBSet("union")
        try
            for (k,_) <- set.iterator do
                k match
                    case Some(key) => tempSet+=(key)
                    case None => None
            for (k,_) <- map.iterator do
                tempSet+=(k)
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def union(set:Set[String]):Try[BSet] = 
        var tempSet = new TempBSet("union")
        try
            for k <- set.iterator do
                tempSet+=(k)  
            for (k,_) <- map.iterator do
                tempSet+=(k)
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def diff(set:BSet):Try[BSet] = 
        var tempSet = new TempBSet("difference")
        try
            for (k,_) <- map.iterator do
                set.contains(k) match
                    case Failure(e) => throw e
                    case Success(in) if !in => tempSet+=(k)
                    case _ => None       
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    def diff(set:Set[String]):Try[BSet] = 
        var tempSet = new TempBSet("difference")
        try
            for (k,_) <- map.iterator do
                if !set.contains(k) then
                    tempSet+=(k)
            Success(tempSet)
        catch
            case e:Exception => Failure(e)
    /*
    def &(set:BSet):BSet =
        this.and(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    
    def &(set:Set[String]):BSet =
        this.and(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset

    def |(set:BSet):BSet =
        this.union(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset

    def |(set:Set[String]):BSet =
        this.union(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    
    def -(set:BSet):BSet =
        this.diff(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    
    def -(set:Set[String]):BSet =
        this.diff(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    */
/**
  * 
  *
  * @param tempSet
  */
private[platdb] class TempBSetIter(val tempSet:TempBSet) extends CollectionIterator:
    private var iter = tempSet.map.keysIterator
    def find(key:String):(Option[String],Option[String]) = 
        if tempSet.map.contains(key) then (Some(key),None) else (None,None)
    def first():(Option[String],Option[String])  = (Some(tempSet.map.firstKey),None)
    def last():(Option[String],Option[String]) = (Some(tempSet.map.lastKey),None)
    def hasNext():Boolean= iter.hasNext
    def next():(Option[String],Option[String])= (Some(iter.next()),None)
    def hasPrev():Boolean = false
    def prev():(Option[String],Option[String]) = (None,None)
