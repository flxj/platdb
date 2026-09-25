/*
   Copyright (C) 2023 flxj(https://github.com/flxj)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package platdb

import scala.collection.mutable.{ArrayBuffer}
import scala.util.control.Breaks._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import spray.json._
import spray.json.{DefaultJsonProtocol,RootJsonFormat}

import net.sf.jsqlparser.schema.{Table,Column}
import net.sf.jsqlparser.statement.{Statement,ShowStatement,ShowColumnsStatement}
import net.sf.jsqlparser.statement.select.{PlainSelect, Select,SelectVisitorAdapter,SelectItemVisitorAdapter,SelectItem}
import net.sf.jsqlparser.statement.select.{FromItemVisitorAdapter,AllColumns,Values}
import net.sf.jsqlparser.statement.create.table.{CreateTable, ColumnDefinition,Index}
import net.sf.jsqlparser.expression.{Expression,BooleanValue, LongValue, StringValue, DoubleValue, NullValue, Function, JdbcParameter}
import net.sf.jsqlparser.expression.operators.relational.{ExpressionList,ParenthesedExpressionList}
import net.sf.jsqlparser.expression.{ExpressionVisitorAdapter,AllValue,BinaryExpression}
import net.sf.jsqlparser.expression.operators.conditional.{OrExpression,AndExpression}
import net.sf.jsqlparser.expression.operators.relational.{EqualsTo,NotEqualsTo, GreaterThan, GreaterThanEquals, MinorThan, MinorThanEquals}
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression
import net.sf.jsqlparser.expression.operators.relational.Plus
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction
import net.sf.jsqlparser.expression.operators.arithmetic.Division

/**
  * The currently supported column data types.
  */
enum ColumnValueType:
    case CHAR 
    case VRCHAR
    case DOUBLE
    case FLOAT 
    case INT 
    case BIGINT
    case UNKNOWN
    override def toString(): String = this match
        case CHAR => "char"
        case VRCHAR => "varchar"
        case DOUBLE => "double"
        case FLOAT => "float"
        case INT => "int"
        case BIGINT => "bigint"
        case UNKNOWN => ""

/**
  * Record the column information of the table.
  * 
  */
private[platdb] class columnInfo(val name:String,val ctype:String):
    var autoNext:Long = 0 
    var auto:Boolean = false 
    var notNull:Boolean = false 
    var hide:Boolean = false 
    var defVal:String = ""

    def getValueType():ColumnValueType = ctype match
        case "char" => ColumnValueType.CHAR
        case "varchar" => ColumnValueType.VRCHAR
        case "double" => ColumnValueType.DOUBLE
        case "float" => ColumnValueType.FLOAT 
        case "int" => ColumnValueType.INT
        case "bigint" => ColumnValueType.BIGINT
        case _ => ColumnValueType.UNKNOWN
/**
  * Record table meta info.
  * 
  */
private[platdb] class tableInfo(val name:String):
    var id:Int = 0
    // if the table not setting pk,then we use rowId as its pk. 
    var rowId:Long = 0 
    // primary key column name.
    var pk:String = ""
    var cols:Array[columnInfo] = new Array[columnInfo](0)

private[platdb] object tableProto extends DefaultJsonProtocol {
    implicit object colJsonFormat extends JsonFormat[columnInfo] {
        override def write(obj: columnInfo): JsValue = JsObject(
            "name" -> JsString(obj.name),
            "ctype" -> JsString(obj.ctype),
            "auto" -> JsBoolean(obj.auto),
            "notNull" -> JsBoolean(obj.notNull),
            "hide" -> JsBoolean(obj.hide),
            "autoNext" -> JsNumber(obj.autoNext),
            "defVal" -> JsString(obj.defVal)
        )
        override def read(json: JsValue): columnInfo = {
            val fields = json.asJsObject.fields
            val name = fields("name").convertTo[String]
            val ctype = fields("ctype").convertTo[String]
            val col = new columnInfo(name,ctype)
            col.auto = fields("auto").convertTo[Boolean]
            col.autoNext = fields("autoNext").convertTo[Long]
            col.hide = fields("hide").convertTo[Boolean]
            col.defVal = fields("defVal").convertTo[String]
            col
        }
    }
    implicit object tableJsonFormat extends JsonFormat[tableInfo] {
        override def write(obj: tableInfo): JsValue = JsObject(
            "name" -> JsString(obj.name),
            "id" -> JsNumber(obj.id),
            "rowId" -> JsNumber(obj.rowId),
            "pk" -> JsString(obj.pk),
            "cols" -> JsArray((for c <- obj.cols yield colJsonFormat.write(c)).toList)
        )

        override def read(json: JsValue): tableInfo = {
            val fields = json.asJsObject.fields
            val name = fields("name").convertTo[String]
            val tb = new tableInfo(name)
            tb.pk = fields("pk").convertTo[String]
            tb.id = fields("id").convertTo[Int]
            tb.rowId = fields("rowId").convertTo[Long]
            tb.cols = fields("cols").convertTo[Array[columnInfo]]
            tb
        }
    }
}

/**
  * A SQL expression evaluator.
  * 
  */
private[platdb] class SQLEvaluator(val table:tableInfo,var row:Array[Array[Byte]]) extends ExpressionVisitorAdapter[(Array[Byte],Byte)] {
    override def visit[S](col: Column,ctx: S): (Array[Byte],Byte) = 
        var idx:Int = -1 
        breakable(
            for (c,i) <- table.cols.zipWithIndex do 
                if col.getColumnName() == c.name then 
                    idx = i
                    break()
        )
        if idx < 0 then throw new IllegalArgumentException(s"Column '${col.getColumnName}' not found in context")
        var t:Byte = 0
        if row(idx).length > 0 then 
            t = table.cols(idx).ctype match
                case "char"|"varchar" => 1.toByte
                case "float"|"double" => 2.toByte
                case "int"|"bigint" => 3.toByte
                case _ => -1.toByte
        (row(idx),t)
    override def visit[S](b:BooleanValue,ctx:S): (Array[Byte],Byte) = (Array[Byte](if b.getValue then 1.toByte else 0.toByte),4.toByte) // Boolean:4
    override def visit[S](l:LongValue,ctx:S): (Array[Byte],Byte) = (Util.longToBytes(l.getValue),3)
    override def visit[S](d:DoubleValue,ctx:S): (Array[Byte],Byte) = (Util.doubleToBytes(d.getValue),2)
    override def visit[S](s:StringValue,ctx:S): (Array[Byte],Byte) = (s.getValue.getBytes(SQLEngine.defaultCharset),1)
    override def visit[S](n:NullValue,ctx:S): (Array[Byte],Byte) = (Array[Byte](),0)
    override def visit[S](equ: EqualsTo,ctx:S):(Array[Byte],Byte) = {
        val (l,_) = equ.getLeftExpression.accept(this, ctx)
        val (r,_) = equ.getRightExpression.accept(this, ctx)
        if l != null && l.sameElements(r) then (Array[Byte](1),4) else (Array[Byte](0),4)
    }
    override def visit[S](g: GreaterThan, ctx: S): (Array[Byte],Byte) = {
        val (l,tl) = g.getLeftExpression.accept(this, ctx)
        val (r,tr) = g.getRightExpression.accept(this, ctx)
        if tl != tr then
            (Array[Byte](0),4)
        else 
            tl match
                case 4 => if l(0) > r(0) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 3 => if Util.bytesToLong(l) > Util.bytesToLong(r) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 2 => if Util.bytesToDouble(l) > Util.bytesToDouble(r) then (Array[Byte](1),0) else (Array[Byte](0),0)
                case 1 => 
                    var ok:Boolean = true
                    var ok2:Boolean = false 
                    breakable(
                        for i <- 0 until Util.min(l.length,r.length) do 
                            if l(i) < r(i) then 
                                ok = false
                                break()
                            else if l(i) > r(i) then 
                                ok2 = true 
                    )
                    if !ok then 
                        (Array[Byte](0),4) // <
                    else if ok2 then 
                        (Array[Byte](1),4) // >
                    else
                        (Array[Byte](0),4) // =
                case _ => (Array[Byte](0),0)
    }
    override def visit[S](g: GreaterThanEquals, ctx: S): (Array[Byte],Byte) = {
        val (l,tl) = g.getLeftExpression.accept(this, ctx)
        val (r,tr) = g.getRightExpression.accept(this, ctx)
        if tl != tr then
            (Array[Byte](0),4)
        else 
            tl match
                case 4 => if l(0) >= r(0) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 3 => if Util.bytesToLong(l) >= Util.bytesToLong(r) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 2 => if Util.bytesToDouble(l) >= Util.bytesToDouble(r) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 1 => 
                    var ok:Boolean = true
                    breakable(
                        for i <- 0 until Util.min(l.length,r.length) do 
                            if l(i) < r(i) then 
                                ok = false 
                                break()
                    )
                    if ok then (Array[Byte](1),4) else (Array[Byte](0),4)
                case _ => (Array[Byte](0),0)
    }
    override def visit[S](g: MinorThan, ctx: S): (Array[Byte],Byte) = {
        val (l,tl) = g.getLeftExpression.accept(this, ctx)
        val (r,tr) = g.getRightExpression.accept(this, ctx)
        if tl != tr then
            (Array[Byte](0),4)
        else 
            tl match
                case 4 => if l(0) > r(0) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 3 => if Util.bytesToLong(l) > Util.bytesToLong(r) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 2 => if Util.bytesToDouble(l) > Util.bytesToDouble(r) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 1 => 
                    var ok:Boolean = true
                    var ok2:Boolean = false 
                    breakable(
                        for i <- 0 until Util.min(l.length,r.length) do 
                            if l(i) > r(i) then 
                                ok = false
                                break()
                            else if l(i) < r(i) then 
                                ok2 = true 
                    )
                    if !ok then 
                        (Array[Byte](0),4) // > 
                    else if ok2 then 
                        (Array[Byte](1),4) // <
                    else
                        (Array[Byte](0),4) // =
                case _ => (Array[Byte](0),0)
    }
    override def visit[S](g: MinorThanEquals, ctx: S): (Array[Byte],Byte) = {
        val (l,tl) = g.getLeftExpression.accept(this, ctx)
        val (r,tr) = g.getRightExpression.accept(this, ctx)
        if tl != tr then
            (Array[Byte](0),4)
        else 
            tl match
                case 4 => if l(0) >= r(0) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 3 => if Util.bytesToLong(l) >= Util.bytesToLong(r) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 2 => if Util.bytesToDouble(l) >= Util.bytesToDouble(r) then (Array[Byte](1),4) else (Array[Byte](0),4)
                case 1 => 
                    var ok:Boolean = true
                    breakable(
                        for i <- 0 until Util.min(l.length,r.length) do 
                            if l(i) > r(i) then 
                                ok = false 
                                break()
                    )
                    if ok then (Array[Byte](1),4) else (Array[Byte](0),4)
                case _ => (Array[Byte](0),0)
    }
    override def visit[S](and: AndExpression, ctx: S): (Array[Byte],Byte) = {
        val (l,tl) = and.getLeftExpression.accept(this, ctx)
        val (r,tr) = and.getRightExpression.accept(this, ctx)
        if tl != tr || tl != 4 then 
            (Array[Byte](0),4)
        else 
            (Array[Byte]((l(0)&r(0)).toByte),4)                 
    }
    override def visit[S](or: OrExpression, ctx: S): (Array[Byte],Byte) = {
        val (l,tl) = or.getLeftExpression.accept(this, ctx)
        val (r,tr) = or.getRightExpression.accept(this, ctx)
        if tl != tr || tl != 4 then 
            (Array[Byte](0),4)
        else 
            (Array[Byte]((l(0)|r(0)).toByte),4)
    }
    override def visit[S](isNull: IsNullExpression, ctx: S): (Array[Byte], Byte) = 
        val (l,tl) = isNull.getLeftExpression.accept(this, ctx)
        if tl != 0 then (Array[Byte](1),4) else (Array[Byte](0),4)

    override def visit[S](plus: Plus, ctx: S): (Array[Byte], Byte) = 
        val (l,tl) = plus.getLeftExpression.accept(this, ctx)
        val (r,tr) = plus.getRightExpression.accept(this, ctx)
        numericalCompute(l,r,tl,tr,'+')
    
    override def visit[S](sub: Subtraction, ctx: S): (Array[Byte], Byte) = 
        val (l,tl) = sub.getLeftExpression.accept(this, ctx)
        val (r,tr) = sub.getRightExpression.accept(this, ctx)
        numericalCompute(l,r,tl,tr,'-')

    override def visit[S](multi: Multiplication, ctx: S): (Array[Byte], Byte) = 
        val (l,tl) = multi.getLeftExpression.accept(this, ctx)
        val (r,tr) = multi.getRightExpression.accept(this, ctx)
        numericalCompute(l,r,tl,tr,'*')
        
    override def visit[S](div: Division, ctx: S): (Array[Byte], Byte) = 
        val (l,tl) = div.getLeftExpression.accept(this, ctx)
        val (r,tr) = div.getRightExpression.accept(this, ctx)
        numericalCompute(l,r,tl,tr,'/')

    // TODO: IN, LIKE, CASE,Function...

    private def numericalCompute(l:Array[Byte],r:Array[Byte],tl:Byte,tr:Byte,tp:Byte):(Array[Byte],Byte) = 
        if tl != tr || (tl != 2 && tl != 3 ) then
            throw new Exception(s"data type ${typeName(tl)} and ${typeName(tr)} not support '${tp}'")
        else
            tp match
                case '+' => 
                    if tl == 2 then
                        (Util.doubleToBytes(Util.bytesToDouble(l) + Util.bytesToDouble(r)),tl)
                    else
                        (Util.longToBytes(Util.bytesToLong(l) + Util.bytesToLong(r)),tl)
                case '-' => 
                    if tl == 2 then
                        (Util.doubleToBytes(Util.bytesToDouble(l) - Util.bytesToDouble(r)),tl)
                    else
                        (Util.longToBytes(Util.bytesToLong(l) - Util.bytesToLong(r)),tl)
                case '*' => 
                    if tl == 2 then
                        (Util.doubleToBytes(Util.bytesToDouble(l) * Util.bytesToDouble(r)),tl)
                    else
                        (Util.longToBytes(Util.bytesToLong(l) * Util.bytesToLong(r)),tl)
                case '/' => 
                    if tl == 2 then
                        (Util.doubleToBytes(Util.bytesToDouble(l) / Util.bytesToDouble(r)),tl)
                    else
                        (Util.longToBytes(Util.bytesToLong(l) / Util.bytesToLong(r)),tl)
                case _ => throw new Exception(s"not support operation '' now")

    def typeName(tp:Byte):String = tp match
        case 1 => "string"
        case 2 => "double"
        case 3 => "long"
        case 4 => "bool"
        case _ => "null"

    def checkType(tp:Byte,tn:String):Boolean = tp match
        case 1 => tn == "char" || tn == "varchar"
        case 2 => tn == "float" || tn == "double"
        case 3 => tn == "int" || tn == "bigint"
        case _ => false
}

/**
  * Rows represents query results, where 'columns' represent column names,' 
  * dataTypes' represent the data types of each column, 
  * 'data' represents data rows, and each row of data is represented as an Array[Any]. 
  * Users can use type assertions to obtain query values.
  * Note that elements with null query results also correspond to the values of scala/java null in this array.
  * 
  */
case class Rows(columns:Array[String],dataTypes:Array[String],data:Array[Array[Any]])

/**
  * Result represents the execution result of SQL statements, 
  * such as create/insert/update/delete...
  * 
  */
case class Result(lastInsertId:Long,rowsAffected:Long)

/**
  * The query results represented by RowIter can be obtained iteratively 
  * by calling the next method in a loop, obtaining one line of results 
  * at a time until the next method returns None.
  * Note that RowIter should be closed after use, 
  * otherwise it may cause database transaction leakage.
  */
class RowsIter():
    private var openFlag:Boolean = false
    private var idx:Int = 0
    private[platdb] var data:Array[Array[Any]] = null
    private[platdb] var qExec:QueryExecutor = null
    private[platdb] def open():Unit = 
        if !openFlag && qExec != null then 
            qExec.open()
        openFlag = true
    /**
      * Close the current object and release underlying resources.
      */
    def close():Unit = 
        try
            data = null
            openFlag = false
        finally
            if qExec != null then qExec.close()
    /**
      * Get column name list
      *
      * @return
      */
    def getColumn:Array[String] = qExec.getColumns
    /**
      * Get the data type of the column
      *
      * @return
      */
    def getType:Array[ColumnValueType] = qExec.getTypes
    /**
      * Get all the data of the query results.
      *
      * @return
      */
    def getData:Array[Array[Any]] = 
        if openFlag then
            if data == null then 
                data = qExec.getData()
            data
        else
            null
    /**
      * Attempt to query the next row of data
      *
      * @return
      */
    def next():Option[Array[Any]] = 
        if openFlag && qExec != null && !qExec.dummy then 
            qExec.next()
        else 
            if data != null && idx < data.length then
                idx += 1
                Some(data(idx-1))
            else
                None
