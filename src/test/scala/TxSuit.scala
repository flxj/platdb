import platdb._
import platdb.defaultOptions
import platdb.Collection._

import scala.util.Failure
import scala.util.Success
import java.io.File

class TxSuit1 extends munit.FunSuite {
    val path:String= s"C:${File.separator}Users${File.separator}flxj_${File.separator}test${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "bk1"

    test("create bucket and add elements"){
        var db = new DB(path)
        db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
        assertEquals(db.closed,false)
        assertEquals(db.readonly,false)
        
        try
            db.update(
                (tx:Transaction) => 
                    given t:Transaction = tx
                    var bk = createBucket(bk1)
                    println(s"create bucket $bk1 success")
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
}
