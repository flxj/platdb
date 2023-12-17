
import scala.util.{Try,Success,Failure}
import platdb._
import platdb.defaultOptions
import java.io.File

class BucketSuit1 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"

    test("create bucket"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            val res = db.update((tx:Transaction) => 
                tx.createBucket(bk1) match
                    case Failure(e) => throw e
                    case Success(bk) =>
                        println(s"create bucket $bk1 success")
                None
            ) 
            res match
                case Success(_) => println("op success")
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

class BucketSuit2 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"

    test("create a bucket,but already exists"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            val res = db.update((tx:Transaction) => 
                tx.createBucket(bk1) match
                    case Failure(e) => println(s"${e.getMessage()}")
                    case Success(bk) =>
                        throw new Exception(s"create bucket $bk1 success,but we except exists error")
                None
            )
            res match
                case Success(_) => println("op success")
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

class BucketSuit3 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"

    test("open a bucket"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            val res = db.view((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case Failure(e) => throw e
                    case Success(bk) =>
                        println(s"open bucket ${bk.name} success")
                None
            )
            res match
                case Success(_) => println("op success")
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

class BucketSuit4 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test"  
    val bk1:String = "bk1"

    test("delete a bucket"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            db.update((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case Failure(e) => throw e
                    case Success(bk) => println(s"open bucket ${bk.name} success")
                tx.deleteBucket(bk1) match
                    case Failure(e) => throw e
                    case Success(_) => println(s"delete bucket ${bk1} success")
            ) match
                case Success(_) => println("delete success")
                case Failure(e) => throw e
            
            db.view((tx:Transaction) =>
                tx.openBucket(bk1) match
                    case Failure(e) => println(e.getMessage())
                    case Success(bk) => throw new Exception(s"open bucket ${bk.name} success,but we except not exists error")
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
                    println("close success")
    }
}

class BucketSuit5 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test"  
    val bk1:String = "bk1"
    val keys = List[String]("key1","svc","(kd7","009s","0dsa","fred","aaa","bbb")
    val elems = List[(String,String)](("key1","value1111111111"),("svc","qazxswedcvfrtgbn"),("(kd7","!@#$%^&*()_+"),("009s","[]';/.,"),("0dsa","1234567890"),("fred","d"),("aaa","bbb"),("bbb","gsdhggsdhgfsh"))
    val keys2 = List[String]("key30","key35","key40","key45","key50")

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
                    case Failure(e) => throw e
                    case Success(bk) => 
                        println(s"open bucket ${bk.name} success")
                        for (k,v) <- elems do
                            bk.put(k,v) match
                                case Failure(e) => throw e
                                case Success(_) => println(s"write (key:$k value:$v) success")
            ) match
                case Success(_) => println("write success")
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
    test("open a bucket and read elements"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
        
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            db.view((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case Failure(e) => throw e
                    case Success(bk) => 
                        println(s"open bucket ${bk.name} success")
                        for k <- keys do
                            bk.get(k) match
                                case Success(v) => println(s"read key:$k success, value is:$v")
                                case Failure(e) => throw e
                        for k <- keys2 do
                            bk.get(k) match
                                case Success(v) => println(s"read key:$k success, value is:$v")
                                case Failure(e) => throw e
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

class BucketSuit6 extends munit.FunSuite {
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
                    case Failure(e) => throw e
                    case Success(bk) => 
                        var count:Int = 0
                        for e <- bk.iterator do
                            e match
                                case (None,_) => println(s"ITER None elements")
                                case (Some(key),None) => 
                                    count+=1
                                    println(s"ITER key $key is a subbucket")
                                case (Some(key),Some(value)) => 
                                    count+=1
                                    println(s"ITER key: $key value:$value")
                        println(s"ITER count $count")
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
                    case Failure(e) => throw e
                    case Success(bk) => 
                        var count:Int = 0
                        var it = bk.iterator
                        while it.hasNext() do
                            it.next() match
                                case (None,_) => println(s"ITER None elements")
                                case (Some(key),None) => 
                                    count+=1
                                    println(s"ITER key $key is a subbucket")
                                case (Some(key),Some(value)) => 
                                    count+=1
                                    println(s"ITER key: $key value:$value")
                        println(s"ITER count $count")
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

class BucketSuit7 extends munit.FunSuite {
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
                    case Failure(e) => throw e
                    case Success(bk) => 
                        var it = bk.iterator
                        while it.hasPrev() do
                            it.prev() match
                                case (None,_) => println(s"ITER None elements")
                                case (Some(key),None) => println(s"ITER key $key is a subbucket")
                                case (Some(key),Some(value)) => println(s"ITER key: $key value:$value")
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
                        case Failure(e) => throw e
                        case Success(bk) => 
                            for i <- 0 to count do
                                val k = s"key$i"
                                val v = BigInt(500, scala.util.Random).toString(36)
                                bk.put(k,v) match
                                    case Failure(e) => throw e
                                    case Success(_) => println(s"WRITE key:$k")
            ) match
                case Success(_) => println(s"WRITE bucket $bk1 success")
                case Failure(e) => throw e

            db.view((tx:Transaction) => 
                tx.openBucket(bk1) match
                    case Failure(e) => throw e
                    case Success(bk) => 
                        for e <- bk.iterator do
                            e match
                                case (None,_) => println(s"ITER None elements")
                                case (Some(key),None) => println(s"ITER key $key is a subbucket")
                                case (Some(key),Some(value)) => println(s"ITER key: $key value:$value")
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
