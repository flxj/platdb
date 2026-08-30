import platdb._
import spray.json._
import spray.json.{DefaultJsonProtocol,RootJsonFormat}
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.{PlainSelect, Select}
import net.sf.jsqlparser.statement.drop.Drop
import net.sf.jsqlparser.statement.create.table.{CreateTable, ColumnDefinition,Index}
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.expression.{Expression, LongValue, StringValue, DoubleValue, NullValue, Function, JdbcParameter}
import net.sf.jsqlparser.statement.insert.Insert
import scala.jdk.CollectionConverters._
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList
import scala.collection.mutable.{Map}

class B(val b1:Int):
    var b2:String = ""

class A(val a1:String):
    var a2:Int = 0 
    var a3:String = ""
    var a4:Array[B] = new Array[B](2)

/*
object clsBProto extends DefaultJsonProtocol {
    implicit object clsBJsonFormat extends JsonFormat[clsB] {
        override def write(obj: clsB): JsValue = JsObject(
            "b1" -> JsNumber(obj.b1),
            "b2" -> JsString(obj.b2),
        )
        override def read(json: JsValue): clsB = {
            val fields = json.asJsObject.fields // 将 JSON 解析为字段 Map
            val b1 = fields("b1").convertTo[Int]
            val b2 = fields("b2").convertTo[String]
            var b:clsB = new clsB(b1)
            b.b2 = b2 
            b
        }
    }
}
*/

/*
object clsAProto extends DefaultJsonProtocol {
    implicit object clsBJsonFormat extends JsonFormat[clsB] {
        override def write(obj: clsB): JsValue = JsObject(
            "b1" -> JsNumber(obj.b1),
            "b2" -> JsString(obj.b2),
        )
        override def read(json: JsValue): clsB = {
            val fields = json.asJsObject.fields // 将 JSON 解析为字段 Map
            val b1 = fields("b1").convertTo[Int]
            val b2 = fields("b2").convertTo[String]
            var b:clsB = new clsB(b1)
            b.b2 = b2 
            b
        }
    }
    implicit object clsAJsonFormat extends JsonFormat[clsA] {
        override def write(obj: clsA): JsValue = JsObject(
            "a1" -> JsString(obj.a1),
            "a2" -> JsNumber(obj.a2),
            "a3" -> JsString(obj.a3),
            "a4" -> JsArray((for b <- obj.a4 yield clsBJsonFormat.write(b)).toList)
        )

        override def read(json: JsValue): clsA = {
            val fields = json.asJsObject.fields // 将 JSON 解析为字段 Map
            val a1 = fields("a1").convertTo[String]
            val a2 = fields("a2").convertTo[Int]
            val a3 = fields("a3").convertTo[String]
            val a4 = fields("a4").convertTo[Array[clsB]]
            var a:clsA = new clsA(a1)
            a.a2 = a2 
            a.a3 = a3
            a.a4 = a4
            a
        }
    }
}
*/

class SQLSuit1 extends munit.FunSuite {
    //import clsBProto._
    //import clsAProto._
    /*
    test("insert"){
        //val statement:String = "INSERT INTO mytable (col1, col2) VALUES (a, b), (d, e)"
        val statement:String = "INSERT INTO mytable (col1, col2) VALUES (a, b)"
        val stmt = CCJSqlParserUtil.parse(statement)
        stmt match
            case ins:Insert => 
                val cols = ins.getColumns().asScala.toArray
                cols.zipWithIndex.foreach( (col,idx) => 
                    println(s"col ${idx} name is ${col.getColumnName()}")
                )
                val vals = ins.getValues()
                val exps = vals.getExpressions().asScala.toList
                exps.zipWithIndex.foreach( (exp,idx) => 
                    exp match
                        case row:ParenthesedExpressionList[Expression] => 
                            val r = row.getExpressions().asScala.toList
                            r.foreach( v => 
                                println(s"value is ${v.toString()}")
                            )
                        case v:Expression => println(s"${v.toString()}")
                        case _ => fail("not row or value")
                )
            case _ => fail("not insert sql")
    }
    */
    test("test") {
        val ca = new A("a")
        ca.a2 = 111
        ca.a4 = new Array[B](2)
        ca.a4(0) = new B(1)
        ca.a4(1) = new B(2)

        for i <- 0 until ca.a4.length do 
            val c = ca.a4(i)
            c.b2 = "xxx"
            //ca.a4(i).b2 = "yyy"
        for b <- ca.a4 do 
            println(b.b2) // xxx
        
        val mp = Map[String,A]()
        mp.put("a",ca)

        mp.get("a") match
            case None => println("none")
            case Some(a) => a.a2 = 222 
        //
        mp.get("a") match
            case None => println("none")
            case Some(a) => println(s"a.a2=${a.a2}") // 222
    }
    
}
