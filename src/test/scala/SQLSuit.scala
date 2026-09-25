import platdb._
import platdb.defaultOptions
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks._
import scala.collection.mutable.{Map,ArrayBuffer}
import java.io.File
import java.lang.Exception
import scala.util.Failure
import scala.util.Success
import scala.compiletime.ops.double

val dbPath:String= s"C:${File.separator}platdb${File.separator}db.test" 


object Tabulator {
    def format(head:Array[String],data: Array[Array[Any]]):String = 
        if head.length == 0 then ""
        else
            val maxSize = for h <- head yield h.length()
            for row <- data do 
                for i <- 0 until row.length do 
                    maxSize(i) = if row(i) != null then Math.max(maxSize(i),row(i).toString.length) else Math.max(maxSize(i),"null".length)
            val hd = formatRow(head, maxSize)
            val rows = for (row <- data) yield formatRow(row, maxSize)
            formatRows(rowSeparator(maxSize),hd,rows)

    def formatRows(rowSeparator: String, head:String,rows: Seq[String]): String = (
        rowSeparator :: 
        head :: 
        rowSeparator :: 
        rows.toList ::: 
        rowSeparator :: 
        List()).mkString("\n")

    def formatRow(row: Seq[Any], colSizes: Seq[Int]) = {
        val cells = (for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else ("%" + size + "s").format(item))
        cells.mkString("|", "|", "|")
    }

    def rowSeparator(colSizes: Seq[Int]) = colSizes map { "-" * _ } mkString("+", "+", "+")
}

class SQLSuit1 extends munit.FunSuite {
    val sql1:String = """CREATE TABLE IF NOT EXISTS T1 (
                        PersonID INT PRIMARY KEY,
                        Name CHAR(255),
                        Number BigInt,
                        Grade1 Float,
                        Grade2 Double,
                        Content VARCHAR(1024));"""
    val sql2:String = """create table IF NOT EXISTS t2 (
    c1 int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    c2 varchar(100),
    c3 varchar(100));"""
    test("create table") {
        var db = SQLEngine(dbPath)
        try 
            db.open() match
                case Failure(e) => throw e 
                case Success(_) => println("open db success")

            db.exec(sql1) match
                case Failure(e) => throw e 
                case Success(_) => println("create t1 success")
            db.exec(sql2) match
                case Failure(e) => throw e 
                case Success(_) => println("create t2 success")

            db.query("show tables") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p)
                    if res.data.length != 2 then 
                        throw new Exception(s"show tables error, count=${res.data.length}")
        catch
            case e:Exception => throw e 
        finally
            if db != null then 
                db.close() match
                    case Failure(e) => println(e.getMessage())
                    case Success(_) => println("close db success")
    }
}

class SQLSuit2 extends munit.FunSuite {
    test("show table") {
        var db = SQLEngine(dbPath)
        try 
            db.open() match
                case Failure(e) => throw e 
                case Success(_) => println("open db success")

            db.query("show tables") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p)   
        catch
            case e:Exception => throw e 
        finally
            if db != null then 
                db.close() match
                    case Failure(e) => println(e.getMessage())
                    case Success(_) => println("close db success")
    }
    test("show table info") {
        var db = SQLEngine(dbPath)
        try 
            db.open() match
                case Failure(e) => throw e 
                case Success(_) => println("open db success")

            db.query("SHOW COLUMNS FROM t3") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p)   
        catch
            case e:Exception => throw e 
        finally
            if db != null then 
                db.close() match
                    case Failure(e) => println(e.getMessage())
                    case Success(_) => println("close db success")
    }
}

class SQLSuit3 extends munit.FunSuite {
    val sql1 = "drop table t1"
    val sql2 = "drop table t2"
    test("drop") {
        var db = SQLEngine(dbPath)
        try 
            db.open() match
                case Failure(e) => throw e 
                case Success(_) => println("open db success")
            
            db.exec(sql1) match
                case Failure(e) => throw e 
                case Success(_) => println("drop t1 success")
            db.exec(sql2) match
                case Failure(e) => throw e 
                case Success(_) => println("drop t2 success")
            db.query("show tables") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p) 
                    if res.data.length != 0 then 
                        throw new Exception("canot drop table")
        catch
            case e:Exception => throw e 
        finally
            if db != null then 
                db.close() match
                    case Failure(e) => println(e.getMessage())
                    case Success(_) => println("close db success")
    }
}

class SQLSuit4 extends munit.FunSuite {
    val sql1 = """insert into t1 values (1,'aaa',100,12.3,123.456,'1qazxsw2')"""
    val sql2 = """insert into t1 values (2,'bbb',200,23.45,765.234,'3edcvfr4'),(3,'ccc',300,345.5,3456.789,'5tgbnhy6')"""
    val sql3 = """insert into t2 (c2,c3) values ('aaaa','abcdedfr'),('ccccc','好好'),('eeeee','{})(+_)'),('ggggg','hh*7&^ll')"""
    test("insert") {
        var db = SQLEngine(dbPath)
        try 
            db.open() match
                case Failure(e) => throw e 
                case Success(_) => println("open db success")
            
            for sql <- List[String](sql1,sql2,sql3) do
                db.exec(sql) match
                    case Failure(e) => throw e 
                    case Success(_) => None 

            println("insert success")
            db.query("select count(*) from t1") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p) 
        catch
            case e:Exception => throw e 
        finally
            if db != null then 
                db.close() match
                    case Failure(e) => println(e.getMessage())
                    case Success(_) => println("close db success")
    }
}

class SQLSuit5 extends munit.FunSuite {
    test("select") {
        var db = SQLEngine(dbPath)
        try 
            db.open() match
                case Failure(e) => throw e 
                case Success(_) => println("open db success")
            
            db.query("select * from t1") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p) 
            db.query("select c1 as COL1,c2 as COL2,c3 as COL3 from t2") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p) 
        catch
            case e:Exception => throw e 
        finally
            if db != null then 
                db.close() match
                    case Failure(e) => println(e.getMessage())
                    case Success(_) => println("close db success")
    }
}

class SQLSuit6 extends munit.FunSuite {
    val sql1 = """update t1 set number=111 where content='1qazxsw2'"""
    val sql2 = """update t2 set c2='vvvvvv' where c1>=2"""
    test("update") {
        var db = SQLEngine(dbPath)
        try 
            db.open() match
                case Failure(e) => throw e 
                case Success(_) => println("open db success")
            
            db.exec(sql2) match
                case Failure(e) => throw e 
                case Success(res) => println("update success")
                    
            db.query("select * from t2") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p) 
        catch
            case e:Exception => throw e 
        finally
            if db != null then 
                db.close() match
                    case Failure(e) => println(e.getMessage())
                    case Success(_) => println("close db success")
    }
}

class SQLSuit7 extends munit.FunSuite {
    val sql = """delete from t2 where c1=3"""
    test("delete") {
        var db = SQLEngine(dbPath)
        try 
            db.open() match
                case Failure(e) => throw e 
                case Success(_) => println("open db success")
            
            db.exec(sql) match
                case Failure(e) => throw e 
                case Success(res) => println("delete success")
                    
            db.query("select * from t2") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p) 
        catch
            case e:Exception => throw e 
        finally
            if db != null then 
                db.close() match
                    case Failure(e) => println(e.getMessage())
                    case Success(_) => println("close db success")
    }
}

class SQLSuit8 extends munit.FunSuite {
    val sql1 = """insert into t1 values (4,'abc',12345,45.666,777.888,'hhhhhhhh'),(5,'jjjj',876543,888.666,9999.888,'lllllll')"""
    val sql2 = """update t2 set c2='uuuuuuu' where c1>=2"""
    test("tx") {
        var db = SQLEngine(dbPath)
        try 
            db.open() match
                case Failure(e) => throw e 
                case Success(_) => println("open db success")

            db.beginTx(false) match
                case Failure(e) => throw e 
                case Success(tx) =>
                    try
                        tx.exec(sql1)
                        tx.exec(sql2)
                        tx.commit()
                    catch
                        case e:Exception => throw e 
                    finally
                        tx.rollback()
            println("tx success")
            db.query("select * from t1") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p) 

            db.query("select * from t2") match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p) 
        catch
            case e:Exception => throw e 
        finally
            if db != null then 
                db.close() match
                    case Failure(e) => println(e.getMessage())
                    case Success(_) => println("close db success")
    }
}


class SQLSuit9 extends munit.FunSuite {
    val sql0:String = """create table IF NOT EXISTS t3 (
    c1 int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    c2 bigint,
    c3 double,
    c4 char(100),
    c5 varchar(100)
    );"""
    val sql1 = """insert into t3 values 
    (1,123,1.2,'aaa','qqqqq'),
    (2,234,3.4,'bbb','aaaaa'),
    (3,345,4.5,'ccc','zzzzz'),
    (4,456,6.7,'ddd','wwwww'),
    (5,567,8.9,'eee','sssss'),
    (6,678,10.0,'fff','xxxxx');"""
    val sql2 = """select c1 as C1,c3 as C3,c5 as C5 from t3"""
    val sql3 = """select * from t3 where c1>=3"""
    test("rows") {
        var db = SQLEngine(dbPath)
        try 
            db.open() match
                case Failure(e) => throw e 
                case Success(_) => println("open db success")
            /*
            db.exec("drop table t3") match
                case Failure(e) => throw e 
                case Success(_) => println("drop t3 success")
            */
            db.beginTx(false) match
                case Failure(e) => throw e 
                case Success(tx) =>
                    try
                        tx.exec(sql0)
                        tx.exec(sql1)
                        tx.commit()
                    catch
                        case e:Exception => throw e 
                    finally
                        tx.rollback()
            println("create table success")

            db.query(sql2) match
                case Failure(e) => throw e 
                case Success(res) => 
                    val p = Tabulator.format(res.columns,res.data)
                    println(p) 
            
            db.queryIter(sql3) match
                case Failure(e) => throw e 
                case Success(rows) => 
                    val cols = rows.getColumn.mkString(" ,")
                    println(cols)
                    breakable(
                        while true do
                            rows.next() match
                                case None => break()
                                case Some(row) => 
                                    val r = row.map( v => if v != null then v.toString() else "null").mkString(" ,")
                                    println(r)
                    )
                    rows.close()
        catch
            case e:Exception => throw e 
        finally
            if db != null then 
                db.close() match
                    case Failure(e) => println(e.getMessage())
                    case Success(_) => println("close db success")
    }
}
