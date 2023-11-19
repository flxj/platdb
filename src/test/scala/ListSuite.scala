import scala.util.Failure
import scala.util.Success
import java.io.File
import platdb._
import platdb.defaultOptions
import platdb.Collection._

class ListSuit1 extends munit.FunSuite {
    val path:String= s"C:${File.separator}Users${File.separator}flxj_${File.separator}test${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("create list and append elements"){
        var len = 0L
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
                    var list = createListIfNotExists(name)
                    println(s"create list $name success")
                    len = list.length

                    list:+= "value1"
                    list:+= "value2"
                    list:+= "value3"
                    list.append("value4") match
                        case Failure(exception) => throw exception
                        case Success(_) => None
                    list.append("value5") match
                        case Failure(exception) => throw exception
                        case Success(_) => None
                    list.append(List[String]("value6","value7","value8","value9")) match
                        case Failure(exception) => throw  exception
                        case Success(_) => None
                    list.append(List[String]("value10","value11","value12","value13")) match
                        case Failure(exception) => throw  exception
                        case Success(_) => None
            ) match
                case Success(_) => println("append success")
                case Failure(e) => throw e
            // check append.
            db.view(
                (tx:Transaction) => 
                    given t:Transaction = tx
                    var list = openList(name)
                    println(s"create list $name success")
                    if list.length!=(len+13) then
                        throw new Exception(s"after append,we expect list length is ${len+13},but actual get ${list.length}")
                    else
                        list.last match
                            case Failure(exception) => throw exception
                            case Success(value) => 
                                if value!="value13" then
                                    throw new Exception(s"after append,we expect last element is value13,but actual get ${value}")
            ) match
                case Success(_) => println("check legth success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => println("close success")
    }
}

class ListSuit2 extends munit.FunSuite {
    val path:String= s"C:${File.separator}Users${File.separator}flxj_${File.separator}test${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("create list and prepend elements"){
        var len = 0L
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
                    var list = createListIfNotExists(name)
                    println(s"create list $name success")
                    len = list.length

                    list+:= "value1"
                    list+:= "value2"
                    list+:= "value3"
                    list.prepend("value4") match
                        case Failure(exception) => throw exception
                        case Success(_) => None
                    list.prepend("value5") match
                        case Failure(exception) => throw exception
                        case Success(_) => None
                    list.prepend(List[String]("value6","value7","value8","value9")) match
                        case Failure(exception) => throw  exception
                        case Success(_) => None
                    list.prepend(List[String]("value10","value11","value12","value13")) match
                        case Failure(exception) => throw  exception
                        case Success(_) => None
            ) match
                case Success(_) => println("prepend success")
                case Failure(e) => throw e
            // check length
            db.view(
                (tx:Transaction) => 
                    given t:Transaction = tx
                    var list = openList(name)
                    println(s"create list $name success")
                    if list.length!=(len+13) then
                        throw new Exception(s"after prepend,we expect list length is ${len+13},but actual get ${list.length}")
                    else
                        list.head match
                            case Failure(exception) => throw exception
                            case Success(value) => 
                                if value!="value10" then
                                    throw new Exception(s"after prepend,we expect first element is value10,but actual get ${value}")
            ) match
                case Success(_) => println("check legth success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => println("close success")
    }
}

class ListSuit3 extends munit.FunSuite {
    val path:String= s"C:${File.separator}Users${File.separator}flxj_${File.separator}test${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("create list and update elements"){
        var len = 0L 
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
                    var list = createListIfNotExists(name)
                    len = list.length
                    println(s"create list $name success")

                    list:+= "value1"
                    list:+= "value2"
                    list:+= "value3"
                    list.append("value4") match
                        case Failure(exception) => throw exception
                        case Success(_) => None
                    list.append("value5") match
                        case Failure(exception) => throw exception
                        case Success(_) => None
                    list.append(List[String]("value6","value7","value8","value9")) match
                        case Failure(exception) => throw  exception
                        case Success(_) => None
                    list.append(List[String]("value10","value11","value12","value13")) match
                        case Failure(exception) => throw  exception
                        case Success(_) => None
            ) match
                case Success(_) => println("write success")
                case Failure(e) => throw e
            // update
            db.update(
                (tx:Transaction) => 
                    given t:Transaction = tx

                    var list = openList(name)
                    if list.length!=(len+13) then
                        throw new Exception(s"after write,we expect list length is ${len+13},but actual get ${list.length}")
                    else
                        for i <- 0 until 13 do
                            list((len+i).toInt) = s"valuevalue${i}"
            ) match
                case Success(_) => println("update success")
                case Failure(e) => throw e
            // check
            db.view(
                (tx:Transaction) => 
                    given t:Transaction = tx

                    var list = openList(name)
                    for i <- 0 until 13 do
                        val v = list((len+i).toInt)
                        if v!=s"valuevalue${i}" then
                            throw new Exception(s"after update,we expect list(${len+i})==valuevalue$i,but actual is $v")
            ) match
                case Success(_) => println("check success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => println("close success")
    }
}
