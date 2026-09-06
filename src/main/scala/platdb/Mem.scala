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

import scala.util.{Try,Success}

private class MemDB(val name:String,val path:String):
    var store:DB = null

private class memTx(txid:Long) extends Transaction:
    override def id: Long = txid
    override def size:Long = ???
    override def writable: Boolean = ???
    override def closed: Boolean = ???
    override def commit(): Unit = ???
    override def rollback(): Unit = ???
    override def openBucket(name: String): Option[Bucket] = ???
    override def createBucket(name: String): Option[Bucket] = ???
    override def createBucketIfNotExists(name: String): Option[Bucket] = ???
    override def deleteBucket(name: String): Unit = ???
    def allCollection(): Seq[(String, String)] = ???
    def openBSet(name:String):Option[BSet] = ???
    def createBSet(name:String):Option[BSet] = ???
    def createBSetIfNotExists(name:String):Option[BSet] = ???
    def deleteBSet(name:String):Unit = ???
    def openList(name:String):Option[BList] = ???
    def createList(name:String):Option[BList] = ???
    def createListIfNotExists(name:String):Option[BList] = ???
    def deleteList(name:String):Unit = ???
    def openRegion(name:String):Option[Region] = ???
    def createRegion(name:String,dimension:Int):Option[Region] = ???
    def createRegionIfNotExists(name:String,dimension:Int):Option[Region] = ???
    def deleteRegion(name:String):Unit = ???
    def copyToFile(path:String):Long = ???
    def openRawBucket(name:String):Option[RawBucket] = ???
    def createRawBucket(name:String):Option[RawBucket] = ???
    def createRawBucketIfNotExists(name:String):Option[RawBucket] = ???
    def deleteRawBucket(name:String):Unit = ???
/**
  * 
  *
  * @param path
  */
private class MemBucket() extends Bucket:
    var db:MemDB = null
    var tx:memTx = null
    def +=(key: String, value: String): Unit = ???
    def +=(elems: Seq[(String, String)]): Unit = ???
    def -=(key: String): Unit = ???
    def -=(keys: Seq[String]): Unit = ???
    def apply(key: String): String = ???
    def closed: Boolean = ???
    def get(key: String): Option[String] = ???
    def put(key: String, value: String): Unit = ???
    def contains(key: String): Boolean = ???
    def createBucket(name: String): Option[Bucket] = ???
    def createBucketIfNotExists(name: String): Option[Bucket] = ???
    def delete(key: String): Unit = ???
    def deleteBucket(name: String): Unit = ???
    def getBucket(name: String): Option[Bucket] = ???
    def iterator: CollectionIterator = ???
    def length: Long = ???
    def name: String = ???
    def getOrElse(key:String,defalutValue:String):String = ???
    def update(key: String, value: String): Unit = ???
    def clean():Unit = ???
    def writeTo(path:String):Unit = None 
    def appendTo(path:String):Unit = None 