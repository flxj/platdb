
import scala.util.{Try,Success,Failure}
import platdb._
import platdb.defaultOptions
import java.io.File
import org.junit.internal.runners.statements.Fail

class BucketSuit1 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"

    test("create bucket"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            val res = db.update((tx:Transaction) => 
                tx.createBucket(bk1) match
                    case None => throw new Exception("create bucket error")
                    case Some(bk) =>
                        println(s"create bucket $bk1 success")
                None
            ) 
            res match
                case Success(_) => println("op bucket success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close db success")
    }
}

class BucketSuit2 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"

    test("create a bucket,but already exists"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            val res = db.update((tx:Transaction) => 
                tx.createBucket(bk1) match
                    case None => println("create bucket error")
                    case Some(bk) =>
                        throw new Exception(s"create bucket $bk1 success,but we except exists error")
                None
            )
            res match
                case Success(_) => println("op success")
                case Failure(e) => if DB.isAlreadyExists(e) then println("bucket already exists") else throw e 
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close db success")
    }
}

class BucketSuit3 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"

    test("open a bucket"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            val res = db.view((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case None => throw new Exception("open bucket error")
                    case Some(bk) =>
                        println(s"open bucket ${bk.name} success")
                None
            )
            res match
                case Success(_) => println("op bucket success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close db success")
    }
}

class BucketSuit4 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test"  
    val bk1:String = "bk1"

    test("delete a bucket"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            db.update((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case None => throw new Exception("open bucket error")
                    case Some(bk) => println(s"open bucket ${bk.name} success")
                tx.deleteBucket(bk1) 
                println(s"delete bucket ${bk1} success")
            ) match
                case Success(_) => println("delete success")
                case Failure(e) => throw e
            
            db.view((tx:Transaction) =>
                tx.openBucket(bk1) match
                    case None => None
                    case Some(bk) => throw new Exception(s"open bucket ${bk.name} success,but we except not exists error")
            ) match
                case Success(_) => println("test success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close db success")
    }
}

class BucketSuit5 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test"  
    val bk1:String = "bk1"
    val elems = List[(String,String)](("key1","value1111111111"),("svc","qazxswedcvfrtgbn"),("(kd7","!@#$%^&*()_+"),("009s","[]';/.,"),("0dsa","1234567890"),("fred","d"),("aaa","bbb"),("bbb","gsdhggsdhgfsh"))

    test("create a bucket and write elements"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            db.update((tx:Transaction) => 
                tx.createBucketIfNotExists(bk1) match
                    case None => throw new Exception("open bucket error")
                    case Some(bk) => 
                        println(s"open bucket ${bk.name} success")
                        for (k,v) <- elems do
                            bk.put(k,v)
                            println(s"write element (${k},${v}) success")
            ) match
                case Success(_) => println("write success")
                case Failure(e) => throw e

            db.view((tx:Transaction) =>
                tx.openBucket(bk1) match
                    case None => throw new Exception("open bucket error")
                    case Some(bk) => 
                        if bk.length != elems.length then 
                            throw new Exception(s"bucket length error,(${bk.length},${elems.length})")
            ) match
                case Success(_) => println("check success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close db success")
    }
}

class BucketSuit6 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test"  
    val bk1:String = "bk1"
    val keys = List[String]("key1","svc","(kd7","009s","0dsa","fred","aaa","bbb")
    val keys2 = List[String]("key30","key35","key40","key45","key50")

    test("open a bucket and read elements"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            db.view((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case None => throw new Exception("open bucket error")
                    case Some(bk) => 
                        println(s"open bucket ${bk.name} success")
                        for k <- keys do
                            bk.get(k) match
                                case Some(v) => println(s"read key:$k success, value is:$v")
                                case None => None
                        for k <- keys2 do
                            bk.get(k) match
                                case Some(v) => println(s"read key:$k success, value is:$v")
                                case None => None
            ) match
                case Success(_) => println("delete success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close success")
    }
}

class BucketSuit7 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"

    test("open a bucket and travel it"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            db.view((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case None => throw new Exception("open bucket error")
                    case Some(bk) => 
                        var count:Int = 0
                        for e <- bk.iterator do
                            e match
                                case None => println(s"ITER None elements")
                                case Some(key,value) => 
                                    count+=1
                                    println(s"ITER key: $key value:$value")
                        println(s"count = $count, bucket length = ${bk.length}")
            ) match
                case Success(_) => println("ITER success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close success")
    }
    test("open a bucket and travel it 2"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            db.view((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case None => throw new Exception("open bucket error")
                    case Some(bk) => 
                        var count:Int = 0
                        bk.iterator.foreach( kv => 
                            kv match
                                case Some((k,v)) => 
                                    count += 1
                                    println(s"visit key: $k value:$v")
                                case None => None
                        )
                        println(s"count = $count, length=${bk.length}")
                        assertEquals(count == bk.length,true)
                        
            ) match
                case Success(_) => println("ITER success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close success")
    }
}

class BucketSuit8 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"
    
    test("open a bucket and reverse travel it"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            db.view((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case None => throw new Exception("open bucket error")
                    case Some(bk) => 
                        var it = bk.iterator
                        while it.hasPrev() do
                            it.prev() match
                                case None => println(s"ITER None elements")
                                case Some(key,value) => println(s"ITER key: $key value:$value")
            ) match
                case Success(_) => println("ITER success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close success")
    }
}

class BucketSuit9 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"
    val count:Int = 50
    
    test("open a bucket insert and travel it"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)
            db.update(
                (tx:Transaction) =>
                    tx.openBucket(bk1) match
                        case None => throw new Exception("open bucket error")
                        case Some(bk) => 
                            for i <- 0 to count do
                                val k = s"key$i"
                                val v = BigInt(500, scala.util.Random).toString(36)
                                bk.put(k,v)
            ) match
                case Success(_) => println(s"WRITE bucket $bk1 success")
                case Failure(e) => throw e

            db.view((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case None => throw new Exception("open bucket error")
                    case Some(bk) => 
                        for e <- bk.iterator do
                            e match
                                case None => println(s"ITER None elements")
                                case Some(key,value) => println(s"ITER key: $key value:$value")
            ) match
                case Success(_) => println("ITER success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close success")
    }
}


class BucketSuit10 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "raw-bk1"
    val count:Int = 50
    
    test("create raw bucket"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)
            db.update(
                (tx:Transaction) =>
                    tx.createRawBucketIfNotExists(bk1) match
                        case None => throw new Exception("open bucket error")
                        case Some(bk) => 
                            for i <- 0 to count do
                                val k = s"key$i".getBytes()
                                val v = BigInt(500, scala.util.Random).toString(36).getBytes()
                                bk.put(k,v)
            ) match
                case Success(_) => println(s"write bucket $bk1 success")
                case Failure(e) => throw e
            var cnt:Int = 0
            db.view((tx:Transaction) => 
                tx.openRawBucket(bk1) match
                    case None => throw new Exception("open bucket error")
                    case Some(bk) => 
                        for e <- bk.iterator do
                            e match
                                case None => println(s"find None elements")
                                case Some(key,value) => cnt += 1
            ) match
                case Success(_) => println("view success")
                case Failure(e) => throw e
            assertEquals(count == cnt,false)
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close success")
    }
}

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.control.Breaks._


class BucketSuit11 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"
    val bk2:String = "bk2"
    val kv = List[(String,String)](
        ("k1","v1"),
        ("kk1","vv1"),
        ("kkkk1","vvvv1"),
        ("kkkkk1","v1"),
        ("kfdffd1","vffdf1"),
        ("ksdsdds1","vgfgg1"),
        ("kcfdgfd1","vhgfnh1"),
        ("kty61","vkkfgss1"),
        ("kvbnhh1","vtrtrrd1"),
        ("kawqer1","vfbhju1"),
        ("klop[1","vxcswq1"),
        ("kjugfdre1","vbnmhy1"),
        ("kcxvfretghjuy1","vbbbbbbbbbb1")
    )
    
    test("concurrent"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)
            //

            val v1:Future[Int] = Future {
                Thread.sleep(1000)
                var cnt:Int = 0 
                db.view( (tx:Transaction) =>
                    tx.openBucket(bk1) match
                        case None => throw new Exception("open bucket error")
                        case Some(bk) => 
                            for e <- bk.iterator do
                                e match
                                    case None => println(s"find None elements")
                                    case Some(key,value) => cnt += 1
                ) match
                    case Failure(e) => throw e 
                    case Success(_) => println(s"future1 success")
                cnt
            }

            val v2:Future[Int] = Future{
                // read
                var cnt:Int = 0 
                var ok:Boolean = false
                var lp:Int = 0 
                while !ok && lp < 100 do 
                    db.view( (tx:Transaction) =>
                        tx.openBucket(bk2) match
                            case None => None 
                            case Some(bk) => 
                                for e <- bk.iterator do
                                    e match
                                        case None => println("find None elements")
                                        case Some(key,value) => 
                                            println(s"find key=${key}, value=${value}")
                                            cnt += 1
                                ok = true
                    ) match
                        case Failure(e) => throw e 
                        case Success(_) => None
                    lp+=1
                    Thread.sleep(50)
                println(s"future2 success")
                cnt
            }

            val v3:Future[Int] = Future{
                // write
                var cnt:Int = 0 
                db.update((tx:Transaction) =>
                    tx.createBucketIfNotExists(bk2) match
                        case None => throw new Exception(s"create ${bk2} failed")
                        case Some(bk) => 
                            for (k,v) <- kv do 
                                bk.put(k,v)
                                cnt += 1
                ) match
                    case Failure(e) => throw e 
                    case Success(_) => println("future3 success")
                cnt
            }

            val r1 = Await.result(v1, 5.seconds)
            val r2 = Await.result(v2, 5.seconds)
            val r3 = Await.result(v3, 10.seconds)
            //
            println(s"r1=${r1}")
            println(s"r2=${r2},r3=${r3}")
            assert(r2 == r3 && r2 == kv.length)
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => 
                    assertEquals(db.closed,true)
                    println("close success")
    }
}
