import platdb._
import platdb.defaultOptions
import platdb.Collection._

import scala.util.Failure
import scala.util.Success
import java.io.File

class TxSuit1 extends munit.FunSuite {
    val path:String = s"C:${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"

    test("create bucket and add elements"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            db.update(
                (tx:Transaction) => 
                    given t:Transaction = tx
                    var bk = createBucketIfNotExists(bk1)
                    bk+=("k1","value1")
                    bk+=("k2","value2")
                    bk+=("k3","value3")
            ) match
                case Success(_) => println("op success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => println("close success")
    }
    test("update some element and rollback"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
            assertEquals(db.closed,false)
            assertEquals(db.readonly,false)

            db.update(
                (tx:Transaction) => 
                    given t:Transaction = tx
                    var bk = openBucket(bk1)
                    bk+=("k1","aaaaa")
                    bk+=("k2","bbbbb")
                    bk+=("k3","ccccc")
            ) match
                case Success(_) => println("update success")
                case Failure(e) => throw e
            //
            db.begin(true) match
                case Failure(e) => throw e 
                case Success(tx) => 
                    try 
                        tx.openBucket(bk1) match
                            case Some(bk) => 
                                bk.update("k1","xxxxx")
                                bk.update("k2","yyyyy")
                            case None => None
                    catch
                        case e:Exception => throw e 
                    finally
                        tx.rollback()
                    println("tx rollback")
            db.view (
                (tx:Transaction) => 
                    tx.openBucket(bk1) match
                        case None => None  
                        case Some(bk) => 
                            bk.get("k1") match
                                case Some(v) => 
                                    if v != "aaaaa" then 
                                        throw new Exception("k1 value error")
                                case None => throw new Exception(s"not found k1")
                            bk.get("k2") match
                                case Some(v) => 
                                    if v != "bbbbb" then 
                                        throw new Exception("k2 value error")
                                case None => throw new Exception(s"not found k2")
                            bk.get("k3") match
                                case Some(v) => 
                                    if v != "ccccc" then 
                                        throw new Exception("k3 value error")
                                case None => throw new Exception(s"not found k3")
                            None
            ) match
                case Failure(e) => throw e 
                case Success(_) => None
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => println("close db success")
    }
}
