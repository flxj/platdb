import scala.util.Failure
import scala.util.Success
import java.io.File
import platdb._
import platdb.defaultOptions
import platdb.Collection._

class ListSuit1 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("open list and append elements"){
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
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("open list and prepend elements"){
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
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("open list and update 10 elements"){
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
                    println(s"create list $name success,len:${len}")

                    list:+= "value1"
                    list:+= "value2"
                    list:+= "value3"
                    list.append("value4") match
                        case Failure(exception) => throw exception
                        case Success(_) => None
                    list.append("value5") match
                        case Failure(exception) => throw exception
                        case Success(_) => None
                    list.append(List[String]("value6","value7","value8")) match
                        case Failure(exception) => throw exception
                        case Success(_) => None
                    list.append(List[String]("value10","value11")) match
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
                    if list.length!=(len+10) then
                        throw new Exception(s"after write,we expect list length is ${len+10},but actual get ${list.length}")
                    else
                        println(s"after write,len is:${list.length}")
                        for i <- 0 until 10 do
                            list((len+i).toInt) = s"valuevalue${i}"
            ) match
                case Success(_) => println("update success")
                case Failure(e) => throw e
            // check
            db.view(
                (tx:Transaction) => 
                    given t:Transaction = tx

                    var list = openList(name)
                    for i <- 0 until 10 do
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

class ListSuit4 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("open list and remove elements"){
        val count = 20
        var db = new DB(path)
        db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
        assertEquals(db.closed,false)
        assertEquals(db.readonly,false)
        
        try
            println(s"============================================> tx A Write")
            var len = 0L
            var lenA = 0L 
            db.update(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case Failure(e) => throw e
                        case Success(list) => 
                            len = list.length
                            println(s"init length is $len")
                            for i <- 0 until count do
                                //val v = BigInt(500, scala.util.Random).toString(36)
                                val v = (i*123).toString
                                list.append(v) match
                                    case Failure(e) => throw e
                                    case Success(_) => None
                            lenA = list.length 
            ) match
                case Success(_) => println(s"Append list $name success,after write list length is $lenA")
                case Failure(e) => throw e
            println(s"============================================> tx B Write")
            var lenB = 0L 
            db.update(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case Failure(e) => throw e
                        case Success(list) => 
                            if list.length != lenA then 
                                throw new Exception(s"current length is ${list.length},but expect is $lenA")
                            for i <- 0 until count do
                                val v = (i*1000).toString
                                list.append(v) match
                                    case Failure(e) => throw e
                                    case Success(_) => None
                            lenB = list.length      
            ) match
                case Success(_) => println(s"Append list $name success,after update list length is $lenB")
                case Failure(e) => throw e
            println(s"============================================> tx C Remove")
            var lenC = 0L 
            db.update(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case Failure(e) => throw e
                        case Success(list) => 
                            if list.length != lenB then 
                                throw new Exception(s"current length is ${list.length},but expect is $lenB")
                            list.remove(len.toInt,count) match
                                case Failure(e) => throw e
                                case Success(_) => None    
                            lenC = list.length   
            ) match
                case Success(_) => println(s"remove list $name success,after update list length is $lenC")
                case Failure(e) => throw e
            println(s"===============================================> tx D check")
            db.view(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case Failure(e) => throw e
                        case Success(list) => 
                            if list.length != lenC then
                                throw new Exception(s"current length is ${list.length},but expect is $lenC")
                            for i <- 0 until count do
                                val v = list((len+i).toInt)
                                if v != (i*1000).toString then
                                    throw new Exception("check failed")
            ) match
                case Success(_) => println(s"Chcek list $name success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => println("close success")
    }
}

class ListSuit5 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("open list and travel"){
        var len = 0L 
        var db = new DB(path)
        db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
        assertEquals(db.closed,false)
        assertEquals(db.readonly,false)
        
        try
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
    }
}

class ListSuit6 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"
    var count = 20

    test("open list and update"){
        var len = 0L 
        var db = new DB(path)
        db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
        assertEquals(db.closed,false)
        assertEquals(db.readonly,false)
        
        try
            println(s"============================================> tx A start")
            var len = 0L
            var lenA = 0L 
            db.update(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case Failure(e) => throw e
                        case Success(list) => 
                            len = list.length
                            println(s"init length is $len")
                            for i <- 0 until count do
                                //val v = BigInt(500, scala.util.Random).toString(36)
                                val v = (i*100).toString
                                list.append(v) match
                                    case Failure(e) => throw e
                                    case Success(_) => None
                            lenA = list.length 
            ) match
                case Success(_) => println(s"Write list $name success,after write list length is $lenA")
                case Failure(e) => throw e
            println(s"============================================> tx B start")
            var lenB = 0L 
            db.update(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case Failure(e) => throw e
                        case Success(list) => 
                            println(s"current length is ${list.length}")
                            for i <- 0 until count do
                                val v = (i*1000).toString
                                list((len+i).toInt) = v 
                            lenB = list.length          
            ) match
                case Success(_) => println(s"Update list $name success,after update list length is $lenB")
                case Failure(e) => throw e
            println(s"===============================================> tx C start")
            db.view(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case Failure(e) => throw e
                        case Success(list) => 
                            println(s"tx id is ${tx.id}")
                            println(s"current length is ${list.length}")
                            for i <- 0 until count do
                                val v = list((len+i).toInt)
                                if v != (i*1000).toString then
                                    throw new Exception("check failed")
            ) match
                case Success(_) => println(s"Chcek list $name success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => println("close success")
    }
}

