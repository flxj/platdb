// For more information on writing tests, see
// https://scalameta.org/munit/docs/getting-started.html

import platdb._
import scala.util.Failure
import scala.util.Success
import java.io.{File,PrintWriter}

import platdb.defaultOptions

val path:String= "C:\\Users\\flxj_\\test\\platdb\\db.test"

class DBSuit1 extends munit.FunSuite {
    test("create a new platdb instance"){
        var db = new DB(path)
        try 
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
            assertEquals(db.isClosed,false)
            assertEquals(db.isReadonly,false)
        catch
            case e:Exception => throw e
        finally
            if !db.isClosed then
                db.close() match
                    case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                    case Success(value) => println("close success")
    }
}

class DBSuit2 extends munit.FunSuite {
    test("open a exists db"){
        var db = new DB(path)
        try
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("open success")
        
            assertEquals(db.isClosed,false)
            assertEquals(db.isReadonly,false)
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                case Success(value) => println("close success")
    }
}

class DBSuit3 extends munit.FunSuite {
    test("open db: file format is not platdb"){
        val p:String= "C:\\Users\\flxj_\\test\\platdb\\notdb.test"
        var writer = new PrintWriter(new File(p))
        for i <- Range(1,100) do
            writer.println(i)
        writer.close()
        var db = new DB(p)
        try 
            db.open() match
                case Failure(exception) => println(exception.getMessage())
                case Success(value) => throw new Exception("open success")
        catch
            case e:Exception => throw e
        finally
            db.close()
    }
}

class DBSuit4 extends munit.FunSuite {
    test("use a not open db"){
        var db = new DB(path)
        try 
            assertEquals(db.isClosed,true)
            assertEquals(db.isReadonly,false)
            
            db.begin(true) match
                case Failure(exception) => println(s"begin failed: ${exception.getMessage()}")
                case Success(tx) =>
                    assertEquals(tx.closed,false)
                    assertEquals(tx.writable,true)
                    throw new Exception("db not open")
        catch
            case e:Exception => throw e
        finally
            if !db.isClosed then
                db.close() match
                    case Failure(exception) => println(s"close failed: ${exception.getMessage()}")
                    case Success(value) => println("close success")

    }
}

class DBSuit5 extends munit.FunSuite {
    test("open db timeout") {
        var db = new DB(path)
        try 
            db.open() match
                case Failure(exception) => throw exception
                case Success(value) => println("db1 open success")
        catch
            case e:Exception =>
                db.close() match
                    case Failure(ee) => println(s"db1 close failed:${ee.getMessage()}")
                    case Success(_) => println("db1 open failed,so just close it now.")
                throw e
        
        assertEquals(db.isClosed,false)
        assertEquals(db.isReadonly,false)
        
        println("try to open db2")
        var db2 = new DB(path)
        try 
            db2.open() match
                case Failure(exception) => println(exception.getMessage())
                case Success(value) => throw new Exception("db2 open success")
        catch
            case e:Exception => throw e
        finally
            db.close() match
                case Failure(exception) => throw exception
                case Success(value) => println("db1 close success")
    }
}

