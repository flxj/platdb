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
import scala.util.{Failure,Success}
import java.nio.ByteBuffer

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
            tx.createList(name) match
                case Failure(e) => throw e
                case Success(list) => println(s"create list ${list.name} success")
        ) match
            case Success(_) => println("op success")
            case Failure(e) => throw e
    catch
        case e:Exception => throw e
    finally
        db.close() match
            case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
            case Success(value) => println("close success")  

def openListAndWrite(db:DB,name:String,count:Int):Unit =
    try
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")

        db.update(
            (tx:Transaction) =>
                tx.openList(name) match
                    case Failure(e) => throw e
                    case Success(list) => 
                        val len = list.length
                        println(s"init length is $len")
                        for i <- 1 to count do
                            //val v = BigInt(500, scala.util.Random).toString(36)
                            val v = (i*100).toString
                            list.append(v) match
                                case Failure(e) => throw e
                                case Success(_) => println(s"WRITE:$i")
                        println(s"after write length is ${list.length} expect ${len+count}")   
        ) match
            case Success(_) => println(s"WRITE list $name success")
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
            case Success(_) => println(s"READ list $name success")
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

def copyArray(a:ArrayBuffer[Int]):ArrayBuffer[Int] = 
    var dest = new Array[Int](a.length)
    val _ = a.copyToArray(dest,0,a.length)
    dest.toBuffer.asInstanceOf[ArrayBuffer[Int]]

import java.util.Base64
object PlatDB:
    @main def main(args: String*) =
        /*
        var freelist = new Freelist(new BlockHeader(2,freelistType,0,0,0))
        
        for i <- Range(9,0,-2) do
            freelist.free(1,i,0)
        freelist.free(2,8,0)

        freelist.unleash(1,2)
        println(s"after release freelist is ${freelist.toString()}")

        for i <- 1 to 5 do
            val id = freelist.allocate(i,1)
            println(s"txid ${i} allocated pgid $id")
        println(s"freelist is ${freelist.toString()}")
        */
        val path:String =s"C:${File.separator}Users${File.separator}flxj_${File.separator}test${File.separator}platdb${File.separator}db.test"
        var db = new DB(path)

        // test bucket
        val name:String ="bk1"
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
        //openListAndWrite(db,name,500)
        //openListAndRead(db,name,List[Int](0,100,200,3,45,300,123,456,499))
        //openListAndTravel(db,name)

        /*
        var arr = new ArrayBuffer[(Long,Long,Int)]()
        arr+=((0L,499L,500))
        arr+=((1200L,1299L,100))
        val v = KList.indexValue(arr)

        KList.indexElements(v.getBytes("ascii")) match
            case None => println("failed")
            case Some(a) => println(a)
        */
        
        ///*
        val bytesToLong:(Array[Byte]) => Long =
            (data:Array[Byte]) =>
                if data.length < 8 then
                    0L 
                else 
                    val a = (data(0) & 0xff) << 24 | (data(1) & 0xff) << 16 | (data(2) & 0xff) << 8 | (data(3) & 0xff) 
                    val b =  (data(4) & 0xff) << 24 | (data(5) & 0xff) << 16 | (data(6) & 0xff) << 8 | (data(7) & 0xff)
                    (a & 0x00000000ffffffffL) << 32 | (b & 0x00000000ffffffffL)
        
        var buf:ByteBuffer = ByteBuffer.allocate(12)
        buf.putLong(500L)
        buf.putInt(-123)
        val data = buf.array()
        val n0 = bytesToLong(data.take(8))
        println(s"n0=$n0")

        val ds = Base64.getEncoder().encodeToString(data)
        println(s"data str len=${ds.length()}")

        val s = new String(data,"ascii")
        val data1 = s.getBytes("ascii")
        val n = bytesToLong(data1.take(8))
        println(s"n=$n")

        val n1 = ByteBuffer.wrap(data1.take(8)).getLong()
        println(s"n1=$n1")

        val data2 = Base64.getDecoder().decode(ds)
        println(s"data2 len=${data2.length}")
        val n2 = bytesToLong(data2.take(8))
        println(s"n2=$n2")
        //*/









        

        
        
        

