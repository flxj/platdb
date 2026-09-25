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

import scala.collection.immutable.{Set}
import scala.util.{Try,Success,Failure}

enum CollectionType:
    case Bucket,RawBucket
    case BSet,BList
    case Region
    case Unknown
    override def toString(): String = this match
        case Bucket => "bucket"
        case Region => "region"
        case RawBucket => "rawBucket"
        case BList => "blist"
        case BSet => "bset"
        case Unknown => "unknown"
    private[platdb] def toByte:Byte = ???

/**
  * some collection methods with transaction parameter.
  */
object Collection:
    // collection data type.
    private[platdb] val typeBucket:Byte = 1
    private[platdb] val typeBSet:Byte = 2
    private[platdb] val typeBList:Byte = 3
    private[platdb] val typeRegion:Byte = 4
    private[platdb] val typeRawBucket:Byte = 5
    //
    private[platdb] def typeName(t:CollectionType):String = t match
        case CollectionType.Bucket => "bucket"
        case CollectionType.Region => "region"
        case CollectionType.RawBucket => "rawBucket"
        case CollectionType.BList => "blist"
        case CollectionType.BSet => "bset"
        case CollectionType.Unknown => "unknown"
    //
    private[platdb] def typeName(t:Byte):String = t match
        case 1 => "bucket"
        case 2 => "bset"
        case 3 => "blist"
        case 4 => "region"
        case 5 => "rawBucket"
        case _ => "unknown"

    private[platdb] def getType(tp:String):CollectionType = 
        tp.toLowerCase() match
            case "bucket" => CollectionType.Bucket
            case "blist" => CollectionType.BList
            case "bset" => CollectionType.BSet
            case "region" | "rtree" => CollectionType.Region
            case "rawBucket"| "raw-bucket" => CollectionType.RawBucket
            case _ => CollectionType.Unknown
    /**
      * This method has the same meaning as the openBucket method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def openBucket(name:String)(using tx:Transaction):Bucket =
        tx.openBucket(name) match
            case Some(bk) => bk
            case None => throw new Exception(s"bucket ${name} not exists")
    /**
      * This method has the same meaning as the createBucket method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def createBucket(name:String)(using tx:Transaction):Bucket =
        tx.createBucket(name) match
            case Some(bk) => bk
            case None => throw new Exception(s"create bucket ${name} failed")
    /**
      * This method has the same meaning as the createBucketIfNotExists method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def createBucketIfNotExists(name:String)(using tx:Transaction):Bucket =
        tx.createBucketIfNotExists(name) match
            case Some(bk) => bk
            case None => throw new Exception(s"create bucket ${name} failed")
    /**
      * This method has the same meaning as the deleteBucket method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      * @throws
      */
    def deleteBucket(name:String)(using tx:Transaction):Unit = tx.deleteBucket(name) 
    /**
      * This method has the same meaning as the openSet method of the Transaction trait,
      *  but it may throw an exception
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def openSet(name:String)(using tx:Transaction):BSet =
        tx.openBSet(name) match
            case Some(set) => set
            case None => throw new Exception(s"set ${name} not exists")
    /**
      * This method has the same meaning as the createSet method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def createSet(name:String)(using tx:Transaction):BSet =
        tx.createBSet(name) match
            case Some(set) => set
            case None => throw new Exception(s"create set ${name} failed")
    /**
      * This method has the same meaning as the createSetIfNotExists method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def createSetIfNotExists(name:String)(using tx:Transaction):BSet =
        tx.createBSetIfNotExists(name) match
            case Some(set) => set
            case None => throw new Exception(s"create set ${name} failed")
    /**
      * This method has the same meaning as the deleteSet method of the Transaction trait, 
      * but it may throw an exception
      *
      * @param name
      * @param tx
      * @throws
      */
    def deleteSet(name:String)(using tx:Transaction):Unit = tx.deleteBSet(name) 
    /**
      * This method has the same meaning as the openList method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def openList(name:String)(using tx:Transaction):BList =
        tx.openList(name) match
            case Some(list) => list
            case None => throw new Exception(s"list ${name} not exists")
    /**
      * This method has the same meaning as the createList method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def createList(name:String)(using tx:Transaction):BList =
        tx.createList(name) match
            case Some(list) => list
            case None => throw new Exception(s"create list ${name} failed")
    /**
      * This method has the same meaning as the createListIfNotExists method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def createListIfNotExists(name:String)(using tx:Transaction):BList =
        tx.createListIfNotExists(name) match
            case Some(list) => list
            case None => throw new Exception(s"create list ${name} failed")
    /**
      * This method has the same meaning as the deleteList method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @throws
      */
    def deleteList(name:String)(using tx:Transaction):Unit = tx.deleteList(name)
    /**
      * This method has the same meaning as the openRegion method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def openRegion(name:String)(using tx:Transaction):Region =
        tx.openRegion(name) match
            case Some(r) => r
            case None => throw new Exception(s"region ${name} not exists")
    /**
      * This method has the same meaning as the createRegion method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def createRegion(name:String,dimension:Int)(using tx:Transaction):Region =
        tx.createRegion(name,dimension) match
            case Some(r) => r
            case None => throw new Exception(s"create region ${name} failed")
    /**
      * This method has the same meaning as the createRegionIfNotExists method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @return
      * @throws
      */
    def createRegionIfNotExists(name:String,dimension:Int)(using tx:Transaction):Region =
        tx.createRegionIfNotExists(name,dimension) match
            case Some(r) => r
            case None => throw new Exception(s"create region ${name} failed")
    /**
      * This method has the same meaning as the deleteRegion method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @throws
      */
    def deleteRegion(name:String)(using tx:Transaction):Unit = tx.deleteRegion(name) 
    
    def openRawBucket(name:String)(using tx:Transaction):RawBucket =
        tx.openRawBucket(name) match
            case Some(r) => r
            case None => throw new Exception(s"raw bucket ${name} not exists")
    
    def createRawBucket(name:String)(using tx:Transaction):RawBucket =
        tx.createRawBucket(name) match
            case Some(r) => r
            case None => throw new Exception(s"create raw bucket ${name} failed")
    
    def createRawBucketIfNotExists(name:String)(using tx:Transaction):RawBucket =
        tx.createRawBucketIfNotExists(name) match
            case Some(r) => r
            case None => throw new Exception(s"create raw bucket ${name} failed")

    def deleteRawBucket(name:String)(using tx:Transaction):Unit = tx.deleteRawBucket(name) 
