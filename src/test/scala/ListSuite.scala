import scala.util.Failure
import scala.util.Success
import java.io.File
import platdb._
import platdb.defaultOptions
import platdb.Collection._
import scala.compiletime.ops.double

class ListSuit1 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("create list and append elements"){
        val count = 15
        var oldLen = 0L
        var db = new DB(path)
        db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        assertEquals(db.closed,false)
        assertEquals(db.readonly,false)
        
        try
            db.update(
                (tx:Transaction) => 
                    given t:Transaction = tx
                    var list = createListIfNotExists(name)
                    println(s"create list $name success")
                    oldLen = list.length

                    list:+= "value1"
                    list:+= "value2"
                    list:+= "value3"
                    list.append("value4") 
                    list.append("value5") 
                    list.append(List[String]("value6","value7","value8","value9")) 
                    list.append(List[String]("value10","value11","value12","value13","value14","value15")) 
            ) match
                case Success(_) => println("append success")
                case Failure(e) => throw e
            // check append.
            db.view(
                (tx:Transaction) => 
                    given t:Transaction = tx
                    var list = openList(name)
                    println(s"open list $name success")
                    if list.length != (oldLen+count) then
                        throw new Exception(s"after append,we expect list length is ${oldLen+count},but actual get ${list.length}")
                    else
                        list.last match
                            case None => None
                            case Some(value) => 
                                if value != "value15" then
                                    throw new Exception(s"after append,we expect last element is value13,but actual get ${value}")
                        
                        for i <- 0 until count do
                            list.get((oldLen+i).toInt) match
                                case None => throw new Exception(s"reed idx ${oldLen+i} failed")
                                case Some(v) => 
                                    if v != s"value${i+1}" then 
                                        throw new Exception(s"read list(${oldLen+i}) get '${v}'")     
            ) match
                case Success(_) => println("check append success")
                case Failure(e) => 
                    println("check append failed")
                    throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close db failed: ${exception.getMessage()}")
                case Success(value) => println("close db success")
    }
}

class ListSuit2 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("open list and prepend elements"){
        val count = 15L
        var len = 0L
        var db = new DB(path)
        db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        assertEquals(db.closed,false)
        assertEquals(db.readonly,false)
        
        try
            db.update(
                (tx:Transaction) => 
                    given t:Transaction = tx
                    var list = openList(name)
                    println(s"open list $name success")
                    len = list.length

                    list+:= "v1"
                    list+:= "v2"
                    list+:= "v3"
                    list.prepend("v4") 
                    list.prepend("v5") 
                    list.prepend(List[String]("v6","v7","v8","v9")) 
                    list.prepend(List[String]("v10","v11","v12","v13","v14","v15")) 
            ) match
                case Success(_) => println("prepend success")
                case Failure(e) => throw e
            // check length
            db.view(
                (tx:Transaction) => 
                    given t:Transaction = tx
                    var list = openList(name)
                    if list.length != (len+count) then
                        throw new Exception(s"after prepend,we expect list length is ${len+count},but actual get ${list.length}")
                    else
                        list.first match
                            case None => None
                            case Some(value) => 
                                if value != "v15" then
                                    throw new Exception(s"after prepend,we expect first element is v15,but actual get ${value}")
            ) match
                case Success(_) => println("check prepend success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close db failed: ${exception.getMessage()}")
                case Success(value) => println("close db success")
    }
}

class ListSuit3 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("open list and update 10 elements"){
        val count = 10
        var len = 0L 
        var db = new DB(path)
        db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        assertEquals(db.closed,false)
        assertEquals(db.readonly,false)
        
        try
            db.update(
                (tx:Transaction) => 
                    given t:Transaction = tx
                    var list = openList(name)
                    len = list.length
                    println(s"opem list $name success,len:${len}")

                    list:+= "value1"
                    list:+= "value2"
                    list:+= "value3"
                    list.append("value4") 
                    list.append("value5") 
                    list.append(List[String]("value6","value7","value8")) 
                    list.append(List[String]("value9","value10")) 
            ) match
                case Success(_) => println("append success")
                case Failure(e) => throw e
            // update
            db.update(
                (tx:Transaction) => 
                    given t:Transaction = tx

                    var list = openList(name)
                    if list.length!=(len+count) then
                        throw new Exception(s"after write,we expect list length is ${len+count},but actual get ${list.length}")
                    else
                        println(s"after write,len is:${list.length}")
                        for i <- 0 until count do
                            list((len+i).toInt) = s"valuevalue${i}"
            ) match
                case Success(_) => println("update success")
                case Failure(e) => throw e
            // check
            db.view(
                (tx:Transaction) => 
                    given t:Transaction = tx

                    var list = openList(name)
                    for i <- 0 until count do
                        val v = list((len+i).toInt)
                        if v != s"valuevalue${i}" then
                            throw new Exception(s"after update,we expect list(${len+i})==valuevalue$i,but actual is $v")
            ) match
                case Success(_) => println("check success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close db failed: ${exception.getMessage()}")
                case Success(value) => println("close db success")
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
                case Success(value) => println("open db success")
        assertEquals(db.closed,false)
        assertEquals(db.readonly,false)
        
        try
            println(s"======> tx A Write")
            var len = 0L
            var lenA = 0L 
            db.update(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case None => throw new Exception("open list error")
                        case Some(list) => 
                            len = list.length
                            println(s"init length is $len")
                            for i <- 0 until count do
                                //val v = BigInt(500, scala.util.Random).toString(36)
                                val v = (i*123).toString
                                list.append(v)
                            lenA = list.length 
            ) match
                case Success(_) => println(s"append list $name success,after write list length is $lenA")
                case Failure(e) => throw e
            println(s"=======> tx B Write")
            var lenB = 0L 
            db.update(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case None => throw new Exception("open list error")
                        case Some(list) => 
                            if list.length != lenA then 
                                throw new Exception(s"current length is ${list.length},but expect is $lenA")
                            for i <- 0 until count do
                                val v = (i*1000).toString
                                list.append(v)
                            lenB = list.length      
            ) match
                case Success(_) => println(s"Append list $name success,after update list length is $lenB")
                case Failure(e) => throw e
            println(s"========> tx C Remove")
            var lenC = 0L 
            db.update(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case None => throw new Exception("open list error")
                        case Some(list) => 
                            if list.length != lenB then 
                                throw new Exception(s"current length is ${list.length},but expect is $lenB")
                            list.remove(len.toInt,count) 
                            lenC = list.length   
            ) match
                case Success(_) => println(s"remove list $name success,after update list length is $lenC")
                case Failure(e) => throw e
            println(s"========> tx D check")
            db.view(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case None => throw new Exception("open list error")
                        case Some(list) => 
                            if list.length != lenC then
                                throw new Exception(s"current length is ${list.length},but expect is $lenC")
                            for i <- 0 until count do
                                val v = list((len+i).toInt)
                                if v != (i*1000).toString then
                                    throw new Exception(s"check failed: get '${v}' but expect '${(i*1000).toString}'")
            ) match
                case Success(_) => println(s"Chcek list $name success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close db failed: ${exception.getMessage()}")
                case Success(value) => println("close db success")
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
                case Success(value) => println("open db success")
        assertEquals(db.closed,false)
        assertEquals(db.readonly,false)
        
        try
            /*
            db.view((tx:Transaction) => 
                tx.openList(name) match
                    case None => throw new Exception("open list error")
                    case Some(list) => 
                        var count = 0L
                        for i <- 0 until (list.length).toInt do 
                            list.get(i) match
                                case None => println(s"ITER None elements")
                                case Some(value) => 
                                    count += 1
                                    println(s"ITER value:$value") 
                        println(s"ITER count $count, list length is:${list.length}")
                        assert(count == list.length)
            ) match
                case Success(_) => println("read1 success")
                case Failure(e) => throw e
            */
            
            db.view((tx:Transaction) => 
                tx.openList(name) match
                    case None => throw new Exception("open list error")
                    case Some(list) => 
                        println(s"open list ${list.name} success")
                        var count:Int = 0
                        var it = list.iterator
                        while it.hasNext() do
                            it.next() match
                                case None => println(s"ITER None elements")
                                case Some(key,value) => 
                                    count+=1
                                    println(s"ITER key: $key value:$value")
                        println(s"ITER count $count, list length is:${list.length}")
            ) match
                case Success(_) => println("read2 success")
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close db failed: ${exception.getMessage()}")
                case Success(value) => println("close db success")
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
            println("====> tx A start")
            var len = 0L
            var lenA = 0L 
            db.update(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case None => throw new Exception("open list error")
                        case Some(list) => 
                            len = list.length
                            println(s"init length is $len")
                            for i <- 0 until count do
                                //val v = BigInt(500, scala.util.Random).toString(36)
                                val v = (i*100).toString
                                list.append(v) 
                            lenA = list.length 
            ) match
                case Success(_) => println(s"Write list $name success,after write list length is $lenA")
                case Failure(e) => throw e
            println("====> tx B start")
            var lenB = 0L 
            db.update(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case None => throw new Exception("open list error")
                        case Some(list) => 
                            println(s"current length is ${list.length}")
                            for i <- 0 until count do
                                val v = (i*1000).toString
                                list((len+i).toInt) = v 
                            lenB = list.length          
            ) match
                case Success(_) => println(s"Update list $name success,after update list length is $lenB")
                case Failure(e) => throw e
            println("====> tx C start")
            db.view(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case None => throw new Exception("open list error")
                        case Some(list) => 
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


class ListSuit7 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("delete list"){
        var db = new DB(path)
        db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open db success")
        assertEquals(db.closed,false)
        assertEquals(db.readonly,false)
        
        try
            db.update(
                (tx:Transaction) =>
                    tx.deleteList(name) 
            ) match
                case Success(_) => println(s"delete list $name success")
                case Failure(e) => throw e
        
            db.view(
                (tx:Transaction) =>
                    tx.openList(name) match
                        case None => println("check delete list success")
                        case Some(list) => throw new Exception("delete list failed")         
            ) match
                case Success(_) => None
                case Failure(e) => throw e
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close db failed: ${exception.getMessage()}")
                case Success(value) => println("close db success")
    }
}


/*
class ListSuit8 extends munit.FunSuite {
    val path:String= s"C:${File.separator}platdb${File.separator}db.test" 
    val name:String = "list1"

    test("test list"){
        try

        catch
            case e:Exception => throw e
    }
}
*/
