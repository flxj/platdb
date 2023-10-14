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
        //openBkAndWrite(db,name,50)
        //openBkAndTravel(db,name)

        
        
        

