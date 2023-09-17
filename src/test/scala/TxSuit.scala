import platdb._
import platdb.defaultOptions
import scala.util.Failure
import scala.util.Success

class TxSuit extends munit.FunSuite {
    var db:DB = null
    val path:String= "C:\\Users\\flxj_\\test\\platdb\\db.test"

    test("tx: open tx"){
        db = new DB(path)
        db.open() match
            case Failure(exception) => throw exception
            case Success(value) => println("open success")
        
        assertEquals(db.isClosed,false)
        assertEquals(db.isReadonly,false)
    }
}
