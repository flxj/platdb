/*
 * Copyright (C) 2023 flxj(https://github.com/flxj)
 *
 * All Rights Reserved.
 *
 * Use of this source code is governed by an Apache-style
 * license that can be found in the LICENSE file.
 */
package platdb

import scala.collection.immutable.{Set}
import scala.util.{Try,Success,Failure}


// collection data type.
private[platdb] val bucketDataType:Byte = 1
private[platdb] val bsetDataType:Byte = 2
private[platdb] val blistDataType:Byte = 3
private[platdb] val regionDataType:Byte = 3

private[platdb] def dataTypeName(t:Byte):String = 
    t match
        case 0 => "Bucket"
        case 1 => "BSet"
        case 2 => "BList"
        case 3 => "Region"
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
      * name
      *
      * @param key
      * @return
      */
    def contains(key:String):Try[Boolean]
    /**
      * Retrieve the element
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
      * Add or update element.
      *
      * @param key
      * @param value
      * @return
      */
    def put(key:String,value:String):Try[Unit]
    /**
      * delete element.
      *
      * @param key
      * @return
      */
    def delete(key:String):Try[Unit]
    /**
      * Open nested subbuckets.
      *
      * @param name
      * @return
      */
    def getBucket(name:String):Try[Bucket]
    /**
      * Create a subbucket.
      *
      * @param name
      * @return
      */
    def createBucket(name:String):Try[Bucket]
    /**
      * Create a subbucket.
      *
      * @param name
      * @return
      */
    def createBucketIfNotExists(name:String):Try[Bucket] 
    /**
      * Delete subbucekt
      *
      * @param name
      * @return
      */
    def deleteBucket(name:String):Try[Unit]
    /**
      * A convenient way to retrieve element.
      *
      * @param key
      * @return
      */
    def apply(key:String):String
    /**
      * A convenient way to add or update element.
      *
      * @param key
      * @param value
      */
    def +=(key:String,value:String):Unit
    /**
      * A convenient way to add or update elements
      *
      * @param elems
      */
    def +=(elems:Seq[(String,String)]):Unit
    /**
      * A convenient way to delete an element.
      *
      * @param key
      */
    def -=(key:String):Unit
    /**
      * A convenient way to delete elements.
      *
      * @param keys
      */
    def -=(keys:Seq[String]):Unit
    /**
      * A convenient way to add or update element.
      *
      * @param key
      * @param value
      */
    def update(key:String,value:String):Unit 


/**
  * An ordered collection of strings.
  */
trait BSet extends Iterable:
    /**
      * name
      *
      * @return
      */
    def name:String
    /**
      * 是否包含某个元素
      *
      * @param key
      * @return
      */
    def contains(key:String):Try[Boolean]
    /**
      * 添加元素
      *
      * @param keys
      * @return
      */
    def add(keys:String*):Try[Unit]
    /**
      * 删除元素
      *
      * @param keys
      * @return
      */
    def remove(keys:String*):Try[Unit]
    /**
      * 计算交集
      *
      * @param set
      * @return
      */
    def and(set:BSet):Try[BSet]
    /**
      * 计算交集
      *
      * @param set
      * @return
      */
    def and(set:Set[String]):Try[BSet]
    /**
      * 计算并集
      *
      * @param set
      * @return
      */
    def union(set:BSet):Try[BSet]
    /**
      * 计算并集
      *
      * @param set
      * @return
      */
    def union(set:Set[String]):Try[BSet]
    /**
      * 计算差集
      *
      * @param set
      * @return
      */
    def diff(set:BSet):Try[BSet]
    /**
      * 计算差集
      *
      * @param set
      * @return
      */
    def diff(set:Set[String]):Try[BSet]
    /**
      * 便捷方法，等同于add
      *
      * @param set
      * @return
      */
    def &(set:BSet):BSet =
        and(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    /**
      * 
      *
      * @param set
      * @return
      */
    def &(set:Set[String]):BSet =
        and(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    /**
      * 便捷方法，等同于union
      *
      * @param set
      * @return
      */
    def |(set:BSet):BSet =
        union(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    /**
      * 
      *
      * @param set
      * @return
      */
    def |(set:Set[String]):BSet =
        union(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    /**
      * 便捷方法，等同于diff
      *
      * @param set
      * @return
      */
    def -(set:BSet):BSet =
        diff(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset
    /**
      * 
      *
      * @param set
      * @return
      */
    def -(set:Set[String]):BSet =
        diff(set) match
            case Failure(exception) => throw exception
            case Success(bset) => bset

/**
  * A list of strings
  */
trait BList extends Iterable:
    /**
      * name
      *
      * @return
      */
    def name:String 
    /**
      * 列表是否为空
      *
      * @return
      */
    def isEmpty: Boolean
    /**
      * 根据下标检索元素
      *
      * @param idx
      * @return
      */
    def get(idx:Int):Try[String]
    /**
      * 列表头部元素
      *
      * @return
      */
    def head:Try[String]
    /**
      * 列表尾部元素
      *
      * @return
      */
    def last:Try[String]
    /**
      * 列表切片操作，获得的子列表为只读模式
      *
      * @param from
      * @param until
      * @return
      */
    def slice(from:Int,until:Int):Try[BList]
    /**
      * 列表逆置，获得的逆列表为只读模式
      *
      * @return
      */
    def reverse:BList
    /**
      * 去除末尾元素后的列表，获得的子列表为只读模式
      *
      * @return
      */
    def init:BList
    /**
      * 去除头部元素后的列表，获得的子列表为只读模式
      *
      * @return
      */
    def tail:BList
    /**
      * 过滤元素，获得的子列表为只读模式
      *
      * @param p
      * @return
      */
    def filter(p:(String) => Boolean): BList
    /**
      * 查找第一个满足条件的元素下标，如果不存在则返回-1
      *
      * @param p
      * @return
      */
    def find(p:(String) => Boolean):Int
    /**
      * 获取前n个元素，获得的子列表是只读的
      *
      * @param n
      * @return
      */
    def take(n: Int): Try[BList]
    /**
      * 获取后n个元素，获得的子列表是只读的
      *
      * @param n
      * @return
      */
    def takeRight(n: Int): Try[BList]
    /**
      * 删除前n个元素
      *
      * @param n
      * @return
      */
    def drop(n: Int): Try[Unit]
    /**
      * 删除后n个元素
      *
      * @param n
      * @return
      */
    def dropRight(n: Int): Try[Unit]
    /**
      * 在index位置插入一个元素
      *
      * @param index
      * @param elem
      * @return
      */
    def insert(index: Int, elem:String): Try[Unit]
    /**
      * 在index位置插入多个元素
      *
      * @param index
      * @param elems
      * @return
      */
    def insert(index: Int, elems:Seq[String]): Try[Unit]
    /**
      * 尾部追加一个元素
      *
      * @param elem
      * @return
      */
    def append(elem:String):Try[Unit]
    /**
      * 尾部追加若干元素
      *
      * @param elems
      * @return
      */
    def append(elems:Seq[String]):Try[Unit]
    /**
      * 头部插入一个元素
      *
      * @param elem
      * @return
      */
    def prepend(elem: String):Try[Unit]
    /**
      * 头部插入多个元素
      *
      * @param elems
      * @return
      */
    def prepend(elems: Seq[String]):Try[Unit]
    /**
      * 删除一个元素
      *
      * @param index
      * @return
      */
    def remove(index: Int): Try[Unit]
    /**
      * 删除多个元素
      *
      * @param index
      * @param count
      * @return
      */
    def remove(index: Int, count: Int):Try[Unit]
    /**
      * 更新元素
      *
      * @param index
      * @param elem
      * @return
      */
    def set(index: Int, elem:String): Try[Unit]
    /**
      * 满足条件的元素是否存在
      *
      * @param p
      * @return
      */
    def exists(p:(String) => Boolean): Boolean
    /**
      * 检索元素的便捷方法
      *
      * @param n
      * @return
      */
    def apply(n: Int):String
    /**
      * 便捷方法，等同于append
      *
      * @param elem
      */
    def :+(elem:String):Unit
    /**
      * 便捷方法，等同于prepend
      *
      * @param elem
      */
    def +:(elem:String):Unit
    /**
      * 更新元素的便捷方法，等同于set
      *
      * @param index
      * @param elem
      */
    def update(index:Int,elem:String):Unit
    
/**
  * 空间索引
  */
trait Region:
    /**
      * name
      *
      * @return
      */
    def name:String
    /**
      * 对象个数
      *
      * @return
      */
    def length:Long
    /**
      * 维度
      *
      * @return
      */
    def dimension:Int
    // 迭代所有对象
    def iterator:Iterator[SpatialObject]
    // 插入一个点对象
    def mark(coordinate:Array[Double],key:String,Value:String):Try[Unit]
    // 插入一个对象
    def put(obj:SpatialObject):Try[Unit]
    // 查询对象
    def get(key:String):Try[SpatialObject]
    // 查询并过滤所有与给定窗口相交的对象
    def search(range:Rectangle,filter:(SpatialObject)=>Boolean):Try[Seq[SpatialObject]]
    // 过滤整个区域中的所有对象
    def scan(filter:(SpatialObject)=>Boolean):Try[Seq[SpatialObject]]
    // 删除一个对象
    def delete(key:String):Try[Unit]
    // 在指定范围内过滤并删除对象(相交)
    def delete(range:Rectangle,filter:(SpatialObject)=>Boolean):Try[Unit]
    // 删除所有包含range的对象(覆盖)
    def delete(range:Rectangle):Try[Unit] 
    // 整个region的范围
    def boundary():Try[Rectangle]
    // 查询距离obj对象最近的k个对象
    def nearby(obj:SpatialObject,k:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]]
    // 查询距离key对应对象最近的k个对象
    def nearby(key:String,k:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]]
    // 查询与obj对象的距离小于等于d的范围内的所有其它对象
    def nearby(obj:SpatialObject,d:Double,limit:Int)(using distFunc:(SpatialObject,SpatialObject)=>Double):Try[Seq[(SpatialObject,Double)]]
    /**
      * 便捷方法，等同于put
      *
      * @param obj
      */
    def +=(obj:SpatialObject):Unit
    /**
      * 便捷方法，等同于delete
      *
      * @param key
      */
    def -=(key:String):Unit
    /**
      * 更新对象的便捷方法
      *
      * @param key
      * @param obj
      */
    def update(key:String,obj:SpatialObject):Unit

/**
  * some collection methods with transaction parameter.
  */
object Collection:
    /**
      * This method has the same meaning as the openBucket method of the Transaction trait, 
      * but it may throw an exception.
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
      * but it may throw an exception.
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
    /**
      * 
      *
      * @param name
      * @param tx
      * @return
      */
    def openRegion(name:String)(using tx:Transaction):Region =
        tx.openRegion(name) match
            case Success(r) => r
            case Failure(e) => throw e 
    /**
      * 
      *
      * @param name
      * @param tx
      * @return
      */
    def createRegion(name:String,dimension:Int)(using tx:Transaction):Region =
        tx.createRegion(name,dimension) match
            case Success(r) => r
            case Failure(e) => throw e
    /**
      * 
      *
      * @param name
      * @param tx
      * @return
      */
    def createRegionIfNotExists(name:String,dimension:Int)(using tx:Transaction):Region =
        tx.createRegionIfNotExists(name,dimension) match
            case Success(r) => r
            case Failure(e) => throw e
    /**
      * delete region
      *
      * @param name
      * @param tx
      */
    def deleteRegion(name:String)(using tx:Transaction):Unit = 
        tx.deleteRegion(name) match
            case Success(_) => None
            case Failure(e) => throw e
