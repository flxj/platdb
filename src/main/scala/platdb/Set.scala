/*
   Copyright (C) 2023 flxj(https://github.com/flxj)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package platdb

import scala.util.{Success,Failure,Try}
import scala.collection.mutable.TreeMap

/**
  * BSet represents a set of string elements on disk, where the elements are unique and can be traversed in dictionary order.
  */
trait BSet extends PlatDBIterable:
    /**
      * name
      *
      * @return
      */
    def name:String
    /**
      * Check if the current collection contains an element.
      *
      * @param key
      * @return
      */
    def contains(key:String):Boolean
    /**
      * Add one element to the current collection.
      *
      * @param key
      */
    def +=(key:String):Unit
    /**
      * Remove one element from the current collection.
      *
      * @param key
      */
    def -=(key:String):Unit
    /**
      * Add one or more elements to the current collection.
      *
      * @param keys
      * @return
      */
    def add(keys:Seq[String]):Unit
    /**
      * Remove one or more elements from the current collection.
      *
      * @param keys
      * @return
      */
    def remove(keys:Seq[String]):Unit
    /**
      * Calculate the intersection of the current set and the target set, and the result is still a BSet (the object is stored in memory).
      *
      * @param set
      * @return
      */
    def and(set:BSet):BSet
    /**
      * Calculate the intersection of the current set and the target set，and the result is still a BSet (the object is stored in memory).
      *
      * @param set
      * @return
      */
    def and(set:Set[String]):BSet
    /**
      * Calculate the union of the current set and the target set，and the result is still a BSet (the object is stored in memory).
      *
      * @param set
      * @return
      */
    def union(set:BSet):BSet
    /**
      * Calculate the union of the current set and the target set，and the result is still a BSet (the object is stored in memory).
      *
      * @param set
      * @return
      */
    def union(set:Set[String]):BSet
    /**
      * Calculate the difference between the current set and the target set，and the result is still a BSet (the object is stored in memory).
      *
      * @param set
      * @return
      */
    def diff(set:BSet):BSet
    /**
      * Calculate the difference between the current set and the target set，and the result is still a BSet (the object is stored in memory).
      *
      * @param set
      * @return
      */
    def diff(set:Set[String]):BSet
    /**
      * A convenient method for calculating intersections, equivalent to the add method.
      *
      * @param set
      * @return
      */
    def &(set:BSet):BSet = and(set)
    /**
      * A convenient method for calculating intersections, equivalent to the add method.
      *
      * @param set
      * @return
      * @throws
      */
    def &(set:Set[String]):BSet = and(set) 
    /**
      * A convenient method for calculating unions, equivalent to the union method.
      *
      * @param set
      * @return
      * @throws
      */
    def |(set:BSet):BSet = union(set) 
    /**
      * A convenient method for calculating unions, equivalent to the union method.
      *
      * @param set
      * @return
      * @throws
      */
    def |(set:Set[String]):BSet = union(set) 
    /**
      * A convenient method for calculating difference sets, equivalent to the diff method.
      *
      * @param set
      * @return
      * @throws
      */
    def -(set:BSet):BSet = diff(set) 
    /**
      * A convenient method for calculating difference sets, equivalent to the diff method.
      *
      * @param set
      * @return
      * @throws
      */
    def -(set:Set[String]):BSet = diff(set) 

/**
  * BSet implementation based on B+tree.
  *
  * @param bk
  */
private[platdb] class BTreeSet(var bk:BTreeBucket) extends BSet:
    def name:String = bk.name
    def length:Long = bk.length
    def closed:Boolean = bk.closed
    def contains(key:String):Boolean = bk.contains(key)
    def iterator:CollectionIterator = new BTreeSetIter(new BTreeBucketIter(bk)) 
    def -=(key:String):Unit = bk-=(key)
    def +=(key:String):Unit = bk+=(key,"")
    /**
      * 
      *
      * @param keys
      * @return
      */
    def add(keys:Seq[String]):Unit = bk+=(for k<- keys yield (k,""))
    /**
      * 
      *
      * @param keys
      * @return
      */
    def remove(keys:Seq[String]):Unit = bk-=(keys)
    /**
      * 
      *
      * @param set
      * @return
      */
    def and(set:BSet):BSet = 
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
        val it1 = iterator
        val it2 = set.iterator
        while it1.hasNext() && it2.hasNext() do
            val k1 = it1.next()
            val k2 = it2.next()
            (k1,k2) match
                case (Some(key1,_),Some(key2,_)) =>
                    if key1 < key2 then
                        var continue = true
                        while continue && it1.hasNext() do
                            val k = it1.next()
                            k match 
                                case Some(key,_) => 
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
                            val kv = it2.next()
                            kv match 
                                case Some(key,_) => 
                                    if key == key1 then
                                        tempSet+=(key)
                                    if key >= key1 then
                                        continue = false
                                case None => None
                case _ => None
        tempSet
        
    def and(set:Set[String]):BSet = 
        var tempSet = new TempBSet("intersect")
        for kv <- iterator do
            kv match
                case Some(key,_) if set.contains(key)  => tempSet+=(key)
                case _ => None
        tempSet
        
    /**
      * 
      *
      * @param set
      * @return
      */
    def union(set:BSet):BSet = 
        var tempSet = new TempBSet("union")
        for kv <- iterator do
            kv match
                case Some(key,_) => tempSet+=(key)
                case None => None
        for kv <- set.iterator do
            kv match
                case Some(key,_) => tempSet+=(key)
                case None => None
        tempSet
        
    def union(set:Set[String]):BSet = 
        var tempSet = new TempBSet("union")
        for kv <- iterator do
            kv match
                case Some(key,_) => tempSet+=(key)
                case None => None
        for k <- set.iterator do
            tempSet+=(k)
        tempSet
    /**
      * 
      *
      * @param set
      * @return
      */
    def diff(set:BSet):BSet = 
        var tempSet = new TempBSet("difference")
        for kv <- iterator do
            kv match
                case None => None
                case Some(key,_) => if !set.contains(key) then tempSet+=(key)
        tempSet

    def diff(set:Set[String]):BSet = 
        var tempSet = new TempBSet("difference")
        for kv <- iterator do
            kv match 
                case Some(key,_) if !set.contains(key) => tempSet+=(key)
                case _ => None
        tempSet
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
private class BTreeSetIter(val iter:BTreeBucketIter) extends CollectionIterator:
    def find(key:String):Option[(String,String)] = iter.find(key)
    def first():Option[(String,String)]  = iter.first()
    def last():Option[(String,String)] = iter.last()
    def hasNext():Boolean= iter.hasNext()
    def next():Option[(String,String)]= iter.next()
    def hasPrev():Boolean = iter.hasNext()
    def prev():Option[(String,String)] = iter.prev()
/**
  * 
  *
  * @param name
  */
private class TempBSet(val name:String) extends BSet:
    var map = new TreeMap[String,Boolean]()
    def length:Long = map.size
    def contains(key:String):Boolean = map.contains(key)
    def add(keys:Seq[String]):Unit = map.addAll(for k <- keys yield (k,true))
    def remove(keys:Seq[String]):Unit =map--=(keys)
    def +=(key:String):Unit = map+=(key,true)
    def -=(key:String):Unit = map-=key
    def iterator: CollectionIterator = new TempBSetIter(this)
    def and(set:BSet):BSet = 
        var tempSet = new TempBSet("intersect") // TODO generate a random name
        for kv <- set.iterator do
            kv match
                case Some(key,_) if map.contains(key) => tempSet+=(key)
                case _ => None
        tempSet
    def and(set:Set[String]):BSet = 
        var tempSet = new TempBSet("intersect")
        for k <- set.iterator do
            if map.contains(k) then tempSet+=(k)
        tempSet

    def union(set:BSet):BSet = 
        var tempSet = new TempBSet("union")
        for kv <- set.iterator do
            kv match
                case Some(key,_) => tempSet+=(key)
                case None => None
        for (k,_) <- map.iterator do
            tempSet+=(k)
        tempSet
    def union(set:Set[String]):BSet = 
        var tempSet = new TempBSet("union")
        for k <- set.iterator do
            tempSet+=(k)  
        for (k,_) <- map.iterator do
            tempSet+=(k)
        tempSet
    def diff(set:BSet):BSet = 
        var tempSet = new TempBSet("difference")
        for (k,_) <- map.iterator do
            if !set.contains(k) then tempSet+=(k)     
        tempSet
    def diff(set:Set[String]):BSet = 
        var tempSet = new TempBSet("difference")
        for (k,_) <- map.iterator do
            if !set.contains(k) then
                tempSet+=(k)
        tempSet

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
private class TempBSetIter(val tempSet:TempBSet) extends CollectionIterator:
    private var iter = tempSet.map.keysIterator
    def find(key:String):Option[(String,String)] = 
        if tempSet.map.contains(key) then Some((key,"")) else None
    def first():Option[(String,String)]  = Some((tempSet.map.firstKey,""))
    def last():Option[(String,String)] = Some((tempSet.map.lastKey,""))
    def hasNext():Boolean= iter.hasNext
    def next():Option[(String,String)]= Some((iter.next(),""))
    def hasPrev():Boolean = false
    def prev():Option[(String,String)] = None
