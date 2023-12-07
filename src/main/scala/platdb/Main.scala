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

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure,Success,Try}
import java.nio.ByteBuffer
import scala.concurrent.{Future,Await}

/*
def createBk(db:DB,name:String):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.update((tx:Transaction) => 
            tx.createBucket(name) match
                case Failure(e) => throw e
                case Success(bk) => println(s"create bucket ${bk.name} success")
        ) match
            case Success(_) => println("op success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")

def openBk(db:DB,name:String):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.view((tx:Transaction) => 
            tx.openBucket(name) match
                case Failure(e) => throw e
                case Success(bk) => println(s"open bucket ${bk.name} success")
        ) match
            case Success(_) => println("op success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")

def deleteBk(db:DB,name:String):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.update((tx:Transaction) => 
            tx.deleteBucket(name) match
                case Failure(e) => throw e
                case Success(_) => println(s"delete bucket ${name} success")
        ) match
            case Success(_) => println("op success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")

def openBkAndWrite(db:DB,name:String,count:Int):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.update(
            (tx:Transaction) =>
                tx.openBucket(name) match
                    case Failure(e) => throw e
                    case Success(bk) => 
                        for i <- 0 to count do
                            val k = s"key$i"
                            val v = BigInt(500, scala.util.Random).toString(36)
                            bk.put(k,v) match
                                case Failure(e) => throw e
                                case Success(_) => println(s"WRITE key:$k")
        ) match
            case Success(_) => println(s"WRITE bucket $name success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")
def openBkAndRead(db:DB,name:String,keys:Seq[String]):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.view(
            (tx:Transaction) =>
                tx.openBucket(name) match
                    case Failure(e) => throw e
                    case Success(bk) => 
                        for k <- keys do
                            bk.get(k) match
                                case Failure(e) => throw e
                                case Success(v) => println(s"GET key:$k value ${v}")
        ) match
            case Success(_) => println(s"READ bucket $name success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")

def openBkAndTravel(db:DB,name:String):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.view((tx:Transaction) => 
            tx.openBucket(name) match
                case Failure(e) => throw e
                case Success(bk) => 
                    println(s"open bucket ${bk.name} success")
                    var count:Int = 0
                    var it = bk.iterator
                    while it.hasPrev() do
                        it.prev() match
                            case (None,_) => println(s"ITER None elements")
                            case (Some(key),None) => 
                                count+=1
                                println(s"ITER key $key is a subbucket")
                            case (Some(key),Some(value)) => 
                                count+=1
                                println(s"ITER key: $key value:$value")
                    println(s"ITER count $count")
        ) match
            case Success(_) => println("delete success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")

def openBkAndCheck(db:DB,name:String,keys:Seq[String]):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.view((tx:Transaction) => 
            tx.openBucket(name) match
                case Failure(e) => throw e
                case Success(bk) => 
                    println(s"bucket length=${bk.length}")
                    for k <- keys do
                        bk.contains(k) match
                            case Success(_) => println(s"key:$k Exists")
                            case Failure(exception) => println(s"key:$k,${exception.getMessage()}")
        ) match
            case Success(_) => println("check success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")

def testIntersect(arr1:Array[Int],arr2:Array[Int]):ArrayBuffer[Int] =
    var i = 0
    var j = 0
    var res = new ArrayBuffer[Int]()
    while i<arr1.length && j<arr2.length do
        while arr1(i) < arr2(j) do
            i+=1
        if i>=arr1.length then
            return res
        if arr1(i) == arr2(j) then
            res+=arr1(i)
            i+=1
        j+=1
    res

def testUnion(arr1:Array[Int],arr2:Array[Int]):ArrayBuffer[Int] =
    var i = 0
    var j = 0
    var res = new ArrayBuffer[Int]()
    while i<arr1.length && j<arr2.length do
        while arr1(i) < arr2(j) do
            res+=arr1(i)
            i+=1
        if i<arr1.length then
            if arr1(i) == arr2(j) then
                res+=arr1(i)
                i+=1
            else 
                res+=arr2(j)
            j+=1
    while i<arr1.length do
        res+=arr1(i)
        i+=1
    while j<arr2.length do
        res+=arr2(j)
        j+=1
    res

def testDifference(arr1:Array[Int],arr2:Array[Int]):ArrayBuffer[Int] =
    var i = 0
    var j = 0
    var res = new ArrayBuffer[Int]()
    while i<arr1.length && j<arr2.length do
        while arr1(i) < arr2(j) do
            res+=arr1(i)
            i+=1
        if i<arr1.length then
            if arr1(i) == arr2(j) then
                i+=1
            j+=1
    while i<arr1.length do
        res+=arr1(i)
        i+=1
    res

def createList(db:DB,name:String):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.update((tx:Transaction) => 
            tx.createListIfNotExists(name) match
                case Failure(e) => throw e
                case Success(list) => 
                    println(s"create list ${list.name} success")
                    println(s"list length is ${list.length}")
        ) match
            case Success(_) => println("op success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")  

def openListAndRead(db:DB,name:String,nums:Seq[Int]):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")
        
        println(s"[debug] ===========================> read tx A start")
        db.view(
            (tx:Transaction) =>
                tx.openList(name) match
                    case Failure(e) => throw e
                    case Success(list) => 
                        println(s"list length is:${list.length}")
                        for n <- nums do
                            list.get(n) match
                                case Failure(e) => throw e
                                case Success(v) => println(s"GET idx:$n value ${v}")
        ) match
            case Success(_) => println(s"READ A list $name success")
            case Failure(e) => throw e
        println(s"[debug] ===========================> read tx B start")
        db.view(
            (tx:Transaction) =>
                tx.openList(name) match
                    case Failure(e) => throw e
                    case Success(list) => 
                        println(s"list length is:${list.length}")
                        for n <- nums do
                            list.get(n) match
                                case Failure(e) => throw e
                                case Success(v) => println(s"GET idx:$n value ${v}")
        ) match
            case Success(_) => println(s"READ B list $name success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")

def openListAndTravel(db:DB,name:String):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.view((tx:Transaction) => 
            tx.openList(name) match
                case Failure(e) => throw e
                case Success(list) => 
                    println(s"open list ${list.name} success")
                    var count:Int = 0
                    var it = list.iterator
                    while it.hasNext() do
                        it.next() match
                            case (None,_) => println(s"ITER None elements")
                            case (Some(key),None) => 
                                count+=1
                                println(s"ITER key $key is a None")
                            case (Some(key),Some(value)) => 
                                count+=1
                                println(s"ITER key: $key value:$value")
                    println(s"ITER count $count, list length is:${list.length}")
        ) match
            case Success(_) => println("delete success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")

def openListAndPend(db:DB,name:String,prepend:Boolean,count:Int):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")
        println(s"=========================> update start")
        var len = 0L 
        db.update((tx:Transaction) => 
            tx.openList(name) match
                case Failure(e) => throw e
                case Success(list) => 
                    len = list.length
                    println(s"open list ${list.name} success,list len is ${len}")
                    for i <- 0 until count do
                        if prepend then
                            list+:=(i*1000).toString
                        else 
                            list:+=(i+1000).toString 
                    println(s"Pend count $count, list length is:${list.length}")
        ) match
            case Success(_) => println("pend success")
            case Failure(e) => throw e
        println(s"=======================> view start")
        db.view((tx:Transaction) => 
            tx.openList(name) match
                case Failure(e) => throw e
                case Success(list) => 
                    println(s"open list ${list.name} success")
                    for i <- 0 until count do
                        val v = list((len+i).toInt)
                        println(s"${len+i} --> $v")
        ) match
            case Success(_) => println("delete success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")

def openListAndWriteUpdate(db:DB,name:String,count:Int):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")
        println(s"============================================> tx A start")
        var len = 0L
        var lenA = 0L 
        db.update(
            (tx:Transaction) =>
                tx.openList(name) match
                    case Failure(e) => throw e
                    case Success(list) => 
                        len = list.length
                        println(s"init length is $len")
                        for i <- 0 until count do
                            //val v = BigInt(500, scala.util.Random).toString(36)
                            val v = (i*100).toString
                            list.append(v) match
                                case Failure(e) => throw e
                                case Success(_) => None
                        lenA = list.length 
        ) match
            case Success(_) => println(s"Write list $name success,after write list length is $lenA")
            case Failure(e) => throw e
        println(s"============================================> tx B start")
        var lenB = 0L 
        db.update(
            (tx:Transaction) =>
                tx.openList(name) match
                    case Failure(e) => throw e
                    case Success(list) => 
                        println(s"current length is ${list.length}")
                        for i <- 0 until count do
                            val v = (i*1000).toString
                            list((len+i).toInt) = v 
                        lenB = list.length          
        ) match
            case Success(_) => println(s"Update list $name success,after update list length is $lenB")
            case Failure(e) => throw e
        println(s"===============================================> tx C start")
        db.view(
            (tx:Transaction) =>
                tx.openList(name) match
                    case Failure(e) => throw e
                    case Success(list) => 
                        println(s"tx id is ${tx.id}")
                        println(s"current length is ${list.length}")
                        for i <- 0 until count do
                            val v = list((len+i).toInt)
                            if v != (i*1000).toString then
                                throw new Exception("check failed")
        ) match
            case Success(_) => println(s"Chcek list $name success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")
def copyArray(a:ArrayBuffer[Int]):ArrayBuffer[Int] = 
    var dest = new Array[Int](a.length)
    val _ = a.copyToArray(dest,0,a.length)
    dest.toBuffer.asInstanceOf[ArrayBuffer[Int]]
*/

def openDBAndGetCollections(db:DB):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.view((tx:Transaction) => 
            tx.allCollection() match
                case Failure(e) => throw e
                case Success(list) => 
                    for (k,v) <- list do
                        println(s"[$k, ${v}]")    
        ) match
            case Success(_) => None
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")

import java.util.Date 
import scala.concurrent.ExecutionContext
implicit val ec:ExecutionContext = ExecutionContext.global

def testTimeout(timeout:Int,success:Boolean):Future[Try[Int]] = Future {
        val start = new Date()
        var stop = false
        var n:Try[Int] = Failure(new Exception("get int timeout"))
        while !stop do
            println(s"do something...")
            Thread.sleep(1000)
            if success then
                n = Success(100000)
                stop = true
            else
                val now = new Date()
                if now.getTime() - start.getTime() > timeout then
                    stop = true
        n
    }

import scala.concurrent.duration._
object PlatDB:
    @main def main(args: String*) =
        val path:String =s"C:${File.separator}Users${File.separator}flxj_${File.separator}test${File.separator}platdb${File.separator}db.test"
        //val path = "/tmp/db.test"
        //var db = new DB(path)

        // test bucket
        //val name:String ="bk1"
        //
        //createBk(db,name)
        //openBk(db,name)
        //deleteBk(db,name)
        //openBk(db,name)
        //openBkAndWrite(db,name,500)
        //openBkAndTravel(db,name)
        //openBkAndRead(db,name,List[String]("key3"))
        //openBkAndCheck(db,name,List[String]("key0","key100","key123","key300","key500","key450","key600","key789"))

        // test list
        //val name = "list1"
        //createList(db,name)
        //openListAndRead(db,name,List[Int](0,1,3,5,7,9,11,13,15,17,19))
        //openListAndTravel(db,name)
        //openListAndWriteUpdate(db,name,10)
        //openListAndPend(db,name,false,10)

        //openDBAndGetCollections(db)

        var server = new Server(ServerOptions(path,defaultOptions,"",0,""))
        server.run()

        /*
        val res = testTimeout(2000,false)
        val n = Await.result(res, 10.seconds)
        n match
            case Success(m) => println(s"get number $m")
            case Failure(e) => println(s"get number failed ${e.getMessage()}")
        */
      
