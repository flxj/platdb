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

private[platdb] class MemDB(val name:String,val path:String):
    var store:DB = null

private[platdb] class MemTx(val id:Long):
    def size:Long = ???
    def closed: Boolean = ???
    def commit(): Try[Unit] = ???
    def createBucket(name: String): Try[Bucket] = ???
    def createBucketIfNotExists(name: String): Try[Bucket] = ???
    def deleteBucket(name: String): Try[Unit] = ???
    def openBucket(name: String): Try[Bucket] = ???
    def rollback(): Try[Unit] = ???
    def writable: Boolean = ???
    // BSet methods
    def openBSet(name:String):Try[BSet] = ???
    def createBSet(name:String):Try[BSet] = ???
    def createBSetIfNotExists(name:String):Try[BSet] = ???
    def deleteBSet(name:String):Try[Unit] = ???

    // list methods.
    def openList(name:String):Try[BList] = ???
    def createList(name:String):Try[BList] = ???
    def createListIfNotExists(name:String):Try[BList] = ???
    def deleteList(name:String):Try[Unit] = ???
    def allCollection():Try[Seq[(String,String)]] = ???

/**
  * 
  *
  * @param path
  */
private[platdb] class MemBucket(val path:String) extends Bucket:
    var db:MemDB = null
    var tx:MemTx = null
    def +=(key: String, value: String): Unit = ???
    def +=(elems: Seq[(String, String)]): Unit = ???
    def -=(key: String): Unit = ???
    def -=(keys: Seq[String]): Unit = ???
    def apply(key: String): String = ???
    def closed: Boolean = ???
    def get(key: String): Try[String] = ???
    def put(key: String, value: String): Try[Unit] = ???
    def contains(key: String): Try[Boolean] = ???
    def createBucket(name: String): Try[Bucket] = ???
    def createBucketIfNotExists(name: String): Try[Bucket] = ???
    def delete(key: String): Try[Unit] = ???
    def deleteBucket(name: String): Try[Unit] = ???
    def getBucket(name: String): Try[Bucket] = ???
    def iterator: CollectionIterator = ???
    def length: Long = ???
    def name: String = ???
    def getOrElse(key:String,defalutValue:String):String = ???
    def update(key: String, value: String): Unit = ???


    def writeTo(path:String):Unit = None 
    def appendTo(path:String):Unit = None 

private[platdb] class MemSet[K]:
    var bk:MemBucket = null 

private[platdb] class PriorityQueue[K,O]:
    var tx:MemTx = null 

private[platdb] class FIFO[K,O]:
    var pq:PriorityQueue[K,O] = null