/*
 * Copyright (C) 2023 flxj(https://github.com/flxj)
 *
 * All Rights Reserved.
 *
 * Use of this source code is governed by an Apache-style
 * license that can be found in the LICENSE file.
 */
package platdb

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure,Success}

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
        val name:String ="bk1"
        var db = new DB(path)
        //createBk(db,name)
        //openBk(db,name)
        //deleteBk(db,name)
        //openBk(db,name)
        //openBkAndWrite(db,name,500)
        openBkAndTravel(db,name)
        //openBkAndRead(db,name,List[String]("key3"))
        //openBkAndCheck(db,name,List[String]("key0","key100","key123","key300","key500","key450","key600","key789"))

        /*
        val arr1 = Array[Int](1,2,3,4,5,6,7,8,9,10,11,12,13)
        val arr2 = Array[Int](-1,-5,5,7,9)

        val intsect = testIntersect(arr1,arr2)
        println(s"1 ${intsect}")

        val union = testUnion(arr1,arr2)
        println(s"2 ${union}")

        val diff = testDifference(arr1,arr2)
        println(s"3 ${diff}")
        */





        

        
        
        

