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


// collection data type.
private[platdb] val bucketDataType:Byte = 1
private[platdb] val bsetDataType:Byte = 2
private[platdb] val blistDataType:Byte = 3
private[platdb] val regionDataType:Byte = 4

private[platdb] def dataTypeName(t:Byte):String = 
    t match
        case 1 => "Bucket"
        case 2 => "BSet"
        case 3 => "BList"
        case 4 => "Region"
        case _ => "Unknown"

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
      * @throws
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
      * @throws
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
      * @throws
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
      * @throws
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
      * @throws
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
      * @throws
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
      * @throws
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
      * @throws
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
      * @throws
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
      * @throws
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
      * @throws
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
      * @throws
      */
    def deleteList(name:String)(using tx:Transaction):Unit = 
        tx.deleteList(name) match
            case Success(_) => None
            case Failure(e) => throw e
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
            case Success(r) => r
            case Failure(e) => throw e 
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
            case Success(r) => r
            case Failure(e) => throw e
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
            case Success(r) => r
            case Failure(e) => throw e
    /**
      * This method has the same meaning as the deleteRegion method of the Transaction trait, 
      * but it may throw an exception.
      *
      * @param name
      * @param tx
      * @throws
      */
    def deleteRegion(name:String)(using tx:Transaction):Unit = 
        tx.deleteRegion(name) match
            case Success(_) => None
            case Failure(e) => throw e
