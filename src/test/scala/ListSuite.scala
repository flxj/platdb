import scala.util.Failure
import scala.util.Success
import java.io.File
import platdb._
import platdb.defaultOptions
import platdb.Collection.createList

class ListSuit1 extends munit.FunSuite {
    val path:String= s"C:${File.separator}Users${File.separator}flxj_${File.separator}test${File.separator}platdb${File.separator}db.test" 
    val bk1:String = "list1"

    test("create list and add elements"){
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
                    var list = createList(bk1)
                    println(s"create list $bk1 success")
                    list:+= "value1"
                    list:+= "value2"
                    list:+= "value3"
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
