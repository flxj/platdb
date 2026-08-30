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

import scala.collection.mutable.{Map,ArrayBuffer}
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import spray.json._
import spray.json.{DefaultJsonProtocol,RootJsonFormat}

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.{Statement,ShowStatement,ShowColumnsStatement}
import net.sf.jsqlparser.statement.select.{PlainSelect, Select,SelectVisitorAdapter,SelectItemVisitorAdapter,SelectItem}
import net.sf.jsqlparser.statement.select.{FromItemVisitorAdapter,AllColumns}
import net.sf.jsqlparser.statement.drop.Drop
import net.sf.jsqlparser.statement.create.table.{CreateTable, ColumnDefinition,Index}
import net.sf.jsqlparser.schema.{Table,Column}
import net.sf.jsqlparser.expression.{Expression,BooleanValue, LongValue, StringValue, DoubleValue, NullValue, Function, JdbcParameter}
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.select.Values
import net.sf.jsqlparser.statement.show.ShowTablesStatement
import net.sf.jsqlparser.expression.operators.relational.{ExpressionList,ParenthesedExpressionList}
import net.sf.jsqlparser.expression.{ExpressionVisitorAdapter,AllValue,BinaryExpression}
import net.sf.jsqlparser.expression.operators.conditional.{OrExpression,AndExpression}
import net.sf.jsqlparser.expression.operators.relational.{EqualsTo,NotEqualsTo, GreaterThan, GreaterThanEquals, MinorThan, MinorThanEquals}
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression

case class SQLRows(columns:Array[String],data:Array[Array[Any]])
case class SQLResult(lastInsertId:Long,rowsAffected:Long)

trait SQLTx:
    def commit:Try[Unit]
    def rollback:Try[Unit]
    def query(stmt:String):Try[SQLRows]
    def exec(stmt:String):Try[SQLResult]

case class SQLEngineOptions(path:String)

private[platdb] class sqlTx(val id:Long,val se:SQLEngine) extends SQLTx:
    def commit: util.Try[Unit] = 
        se.dbTx.remove(id) match
            case None => Success(None)
            case Some(tx) => tx.rollback()   
    def rollback: Try[Unit] = 
        se.dbTx.remove(id) match
            case None => Success(None)
            case Some(tx) => tx.rollback() //TODO: need clean tableInfo in db.tbs
    def query(stmt: String): util.Try[SQLRows] = 
        se.dbTx.get(id) match
            case None => Failure(new Exception("wrong tx"))
            case Some(tx) => se.txQuery(stmt,tx)
    def exec(stmt:String):Try[SQLResult] = 
        se.dbTx.get(id) match
            case None => Failure(new Exception("wrong tx"))
            case Some(tx) => se.txExec(stmt,tx)

private[platdb] class columnInfo(val name:String,val ctype:String): //TODO: use Byte as ctype type.
    var autoNext:Long = 0 
    var auto:Boolean = false 
    var notNull:Boolean = false 
    var hide:Boolean = false 
    var defVal:String = ""

private[platdb] class tableInfo(val name:String):
    var id:Int = 0
    // if the table not setting pk,then we use rowId as its pk. 
    var rowId:Long = 0 
    var pk:String = ""
    var charSet:String = ""
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
            tb.id = fields("id").convertTo[Int]
            tb.rowId = fields("rowId").convertTo[Long]
            tb.cols = fields("cols").convertTo[Array[columnInfo]]
            tb
        }
    }
}

private class SQLEvaluator(val table:tableInfo,var row:Array[Array[Byte]]) extends ExpressionVisitorAdapter[(Array[Byte],Byte)] {
    override def visit[S](col: Column,ctx: S): (Array[Byte],Byte) = 
        var idx:Int = -1 
        breakable(
            for i <- 0 until table.cols.length do 
                if col.getColumnName() == table.cols(i).name then 
                    idx = i 
                    break()
        )
        if idx < 0 then throw new IllegalArgumentException(s"Column '${col.getColumnName}' not found in context")
        var t:Byte = 0
        if row(idx).length > 0 then t = table.cols(idx).ctype match
            case "char"|"varchar" => 1.toByte
            case "float"|"double" => 2.toByte
            case "int"|"bigint" => 3.toByte
            case _ => -1.toByte
        (row(idx),t)
    override def visit[S](b:BooleanValue,ctx:S): (Array[Byte],Byte) = (Array[Byte](if b.getValue then 1.toByte else 0.toByte),4.toByte) // Boolean:4
    override def visit[S](l: LongValue,ctx:S): (Array[Byte],Byte) = (Util.longToBytes(l.getValue),3)
    override def visit[S](d: DoubleValue,ctx:S): (Array[Byte],Byte) = (Util.doubleToBytes(d.getValue),2)
    override def visit[S](s: StringValue,ctx:S): (Array[Byte],Byte) = (s.getValue.getBytes(StandardCharsets.UTF_8),1)
    override def visit[S](n: NullValue,ctx:S): (Array[Byte],Byte) = (Array[Byte](),0)
    override def visit[S](equ: EqualsTo,ctx:S):(Array[Byte],Byte) = {
        val (l,_) = equ.getLeftExpression.accept(this, ctx)
        val (r,_) = equ.getRightExpression.accept(this, ctx)
        if l != null && l.equals(r) then (Array[Byte](1),4) else (Array[Byte](0),4)
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

    // TODO:  (+,-,*,/), IN, LIKE, CASE,Function...

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

object SQLEngine:
    val errValueFmt = new Exception("The data format doesn't match the column field type")
    val errClosed = new Exception("db is closed state")
    val errNoTable = new Exception("table not exists")

class SQLEngine(val path:String,val ops:Options):
    import tableProto._
    private[platdb] val metaTb:String = "platdb_info_meta"
    private[platdb] val tableTb:String = "platdb_info_table"
    private[platdb] val indexTb:String = "platdb_info_index"
    private[platdb] var openFlag:Boolean = false
    private[platdb] var db:DB = null
    private[platdb] var tbs:Map[String,tableInfo] = Map[String,tableInfo]()
    private[platdb] var dbTx:Map[Long,Transaction] = Map[Long,Transaction]()
    //
    def characterSet:String = StandardCharsets.UTF_8.toString()
    //
    private def contains(name:String):Boolean =
        if tbs.contains(name) then
            true
        else
            db.get(tableTb,name) match
                case Success(_,s) =>
                    val tb = s.parseJson.convertTo[tableInfo]
                    tbs.put(name,tb)
                    true
                case Failure(e) => if !DB.isNotExists(e) then throw e else true
    //
    def open:Try[Unit] =
        try
            if openFlag then
                return Success(None)
            db = new DB(path)(using ops)
            db.open() match
                case Failure(e) => throw e
                case Success(_) => None
            // 1. create catalog table if not exists
            //  platdb_info_meta bucket --> table_id,indx_id
            //  platdb_info_tables bucket --> key:table_name, value:table_info
            //  platdb_info_index bucket  --> key:table_name, value:index_info
            
            // table(bucket) --> bucket_name:table_name, 
            //                   key: tablePrefix_{TableID}_recordPrefixSep_{RowID}
            //                   value: record_head + column_value
            db.update (
                (tx:Transaction) =>
                    tx.createBucketIfNotExists(metaTb) match
                        case Failure(e) => throw e
                        case Success(_) => None 
                    tx.createBucketIfNotExists(tableTb) match
                        case Failure(e) => throw e
                        case Success(_) => None 
                    tx.createBucketIfNotExists(indexTb) match
                        case Failure(e) => throw e
                        case Success(_) => None
            ) match
                case Failure(e) => throw e
                case Success(_) => None
            openFlag = true 
            Success(None)
        catch
            case ex:Exception => Failure(ex)
            case er:Error => Failure(er)
    // 
    def close:Try[Unit] = 
        try
            if !openFlag then return Success(None)
            for (id,tx) <- dbTx do 
                tx.rollback() match 
                    case Failure(e) => throw e 
                    case Success(_) => None 
            db.close() match
                case Failure(e) => throw e 
                case Success(_) => 
                    dbTx.clear()
                    tbs.clear()
                    openFlag = false
                    Success(None)
        catch
            case ex:Exception => Failure(ex)
            case er:Error => Failure(er)
    //
    def query(stmt:String):Try[SQLRows] =  
        if !openFlag then return Failure(SQLEngine.errClosed)
        var res:SQLRows = null
        val r = db.begin(false) match
            case Failure(e) => Failure(e)
            case Success(tx) => txQuery(stmt,tx) match
                case Failure(e) => tx.rollback()
                case Success(s) => 
                    res = s 
                    tx.commit() 
        r match
            case Failure(e) => Failure(e)
            case Success(_) => Success(res)
    // 
    private[platdb] def txQuery(stmt:String,tx:Transaction):Try[SQLRows] = 
        try 
            if !openFlag then return Failure(SQLEngine.errClosed)
            val sql = preprocess(stmt)
            val stat: Statement = CCJSqlParserUtil.parse(sql) // JSQLParserException
            stat match
                case sel:Select => sel.getSelectBody() match
                    case ps:PlainSelect =>
                        // 1.check table
                        val tables = ArrayBuffer[String]()
                        ps.getFromItem().accept( new FromItemVisitorAdapter{
                            override def visit(table: Table): Unit = tables.append(table.getName())
                        })
                        if tables.length == 0 then 
                            throw new Exception("no table")
                        else if tables.length != 1 then 
                            throw new Exception("not support muti-tables query now")
                        val name = tables(0).toLowerCase()
                        if !contains(name) then throw SQLEngine.errNoTable
                        val tbi = tbs.get(name) match
                            case Some(v) => v 
                            case None => null
                        
                        // 2.check column
                        var allFlag:Boolean = false 
                        val columns = ArrayBuffer[String]()
                        val items = ps.getSelectItems().asScala.toList
                        items.foreach( item =>
                            item match
                                case exp:Expression => 
                                    exp.accept(new ExpressionVisitorAdapter {
                                        override def visit(column: Column): Unit = 
                                            if item.getAliasName() != "" then
                                                columns.append(item.getAliasName())
                                            else
                                                columns.append(column.getColumnName())
                                        override def visit(all: AllColumns): Unit = allFlag = true 
                                    })
                                case _ => throw new Exception("not support other type column now") 
                        )
                        if columns.length == 0 && !allFlag then 
                            throw new Exception("query column is empty")
                        else if allFlag && columns.length != 0 then 
                            throw new Exception("not support such columns")
                        // get columns type info
                        val ftypes = new ArrayBuffer[String](columns.length)
                        if allFlag then 
                            for col <- tbi.cols do 
                                columns.append(col.name)
                                ftypes.append(col.ctype)
                        else
                            for c <- columns do 
                                var i:Int = -1
                                breakable(
                                    for j <- 0 until tbi.cols.length do 
                                        if tbi.cols(j).name == c then 
                                            i = j 
                                            break()
                                )
                                if i < 0 then 
                                    throw new Exception(s"not found column ${c}")
                                ftypes.append(tbi.cols(i).ctype)
                        // 3.filter row by where 
                        val rd = new ArrayBuffer[Array[Any]]()
                        ps.getWhere() match
                            case null => tx.openBucket(name) match
                                case Failure(e) => throw e 
                                case Success(bk) => 
                                    for (k,v) <- bk.iterator do 
                                        v match
                                            case Some(str) =>
                                                val row = splitRow(str,tbi)
                                                val r = project(columns,ftypes,row)
                                                rd.append(r)
                                            case None => None 
                            case exp:Expression => 
                                val eva = new SQLEvaluator(tbi,null)
                                tx.openBucket(name) match
                                    case Failure(e) => throw e 
                                    case Success(bk) =>
                                        for (k,v) <- bk.iterator do v match 
                                            case Some(str) => 
                                                val row = splitRow(str,tbi)
                                                eva.row = row 
                                                val (ok,_) = exp.accept(eva,null)
                                                if ok.length > 0 && (ok(0)&1) != 0 then 
                                                    val r = project(columns,ftypes,row)
                                                    rd.append(r)
                                            case None => None
                        Success(SQLRows(columns.toArray,rd.toArray))
                    case _ => throw new Exception("not support the sql now")   
                case st:ShowTablesStatement => 
                    // list all tables name
                    val ts = new ArrayBuffer[Array[Any]]
                    tx.openBucket(tableTb) match
                        case Failure(e) => Failure(e)
                        case Success(bk) => 
                            for (k,_) <- bk.iterator do 
                                k match
                                    case Some(v) => ts.append(Array[Any](v))
                                    case None => None
                    Success(SQLRows(Array[String]("tables"),ts.toArray))
                case sc:ShowColumnsStatement => 
                    val table = sc.getTableName().toLowerCase()
                    tx.openBucket(tableTb) match
                        case Failure(e) => Failure(e)
                        case Success(bk) => 
                            bk.get(table) match
                                case Failure(e) => Failure(e)
                                case Success(v) => 
                                    val info = v.toJson.convertTo[tableInfo]
                                    val rows = new Array[Array[Any]](info.cols.length)
                                    for i <- 0 until info.cols.length do 
                                        rows(i) = Array[Any](info.cols(i).name,info.cols(i).ctype,"")
                                        if info.cols(i).name == info.pk then 
                                            rows(i)(3) = "primary key"
                                    Success(SQLRows(Array[String]("name","type","info"),rows))
                case _ => throw new Exception("not support the query sql now")
        catch 
            case ex:Exception => Failure(ex)
            case er:Error => Failure(er)
    //
    private def project(cols:ArrayBuffer[String],ftype:ArrayBuffer[String],row:Array[Array[Byte]]):Array[Any] = 
        val res = new Array[Any](cols.length)
        for i <- 0 until cols.length do 
            res(i) = (if row(i).length == 0 then null 
                else ftype(i) match
                    case "char"|"varchar" => new String(row(i),StandardCharsets.UTF_8) 
                    case "int" => Util.bytesToInt(row(i))
                    case "bigint" => Util.bytesToLong(row(i)) 
                    case "float" =>  Util.bytesToFloat(row(i)) 
                    case "double" => Util.bytesToDouble(row(i)) 
                    case _ =>  null
            )
        res 
    //
    private def parseCreateTable(ct:CreateTable):tableInfo = 
        // 1.get table name, check if exists
        val name = ct.getTable.getName
        if contains(name) then 
            throw new Exception("table already exists")
        // init a table info obj
        val tb = new tableInfo(name)
        // 2.check field type
        val cols = new ArrayBuffer[columnInfo]()
        val cd = ct.getColumnDefinitions.asScala.toList
        cd.foreach( col => 
            val cname = col.getColumnName
            val ctype = col.getColDataType
            val tname = ctype.getDataType.toLowerCase
            val targs = ctype.getArgumentsStringList.asScala.toArray
            tname match
                case "varchar" => 
                    if targs.length != 1 then
                        throw new Exception(s"invalid field ${tname}")
                    val l = targs(0).toInt
                    if l <= 0 || l > 65535 then
                        throw new Exception(s"invalid field ${tname}(${l})")
                case "char" => 
                    if targs.length != 1 then
                        throw new Exception(s"invalid field ${tname}")
                    val l = targs(0).toInt
                    if l <= 0 || l > 255 then
                        throw new Exception(s"invalid field ${tname}(${l})")
                case "int"|"bigint"|"double"|"float" => None 
                case _ => throw new Exception(s"not support field type ${tname} now")
            val c = new columnInfo(cname,tname)
            val specs = col.getColumnSpecs.asScala.toList
            specs.foreach( sp => 
                if sp.toLowerCase() == "auto_increment" then
                    if tname != "int" || tname != "bigint" then 
                        throw new Exception(s"${cname} filed type is ${tname},not support AUTO_INCREMENT")
                    else
                        c.auto = true
                if sp.toLowerCase == "primary" then
                    if tb.pk != "" then 
                        throw new Exception("not support composite keys now")
                    else
                        tb.pk = cname
                // TODO: process other options,example, not null, default value
            )
            cols.append(c)
        )
        tb.cols = cols.toArray
        // 
        val index = ct.getIndexes.asScala.toList
        index.foreach( idx =>
            if (idx.getType == "PRIMARY KEY") then 
                val pks = idx.getColumns.asScala.map(_.getColumnName).toArray
                
                if pks.length != 1 then 
                    throw new Exception("not support composite keys now")

                if tb.pk != "" && tb.pk != pks(0) then
                    throw new Exception("primary key duplication")

                tb.pk = pks(0)
                var ok:Boolean = false 
                for c <- tb.cols do
                    if c.name == tb.pk then ok = true 
                if !ok then 
                    throw new Exception("primary key field not exists")
            else
                throw new Exception("not support secondary index now")
        )
        tb 
    // get table name.
    private def parseDrop(dt:Drop):String = 
        // 1.get table name, check if exists
        val name = dt.getName.getName()
        if !contains(name) then throw SQLEngine.errNoTable else name 
    //
    private def parseInsert(ins:Insert):(String,ArrayBuffer[(String,String)]) = 
        // sql = "INSERT INTO employees (id, name) VALUES (1, 'John')";
        val table = ins.getTable().getName()
        if !contains(table) then throw SQLEngine.errNoTable
        tbs.get(table) match
            case None => throw new Exception("table not exists")
            case Some(tbi) => 
                // check the field exists
                val cols = ins.getColumns().asScala.toArray
                if cols.length == 0 then throw new Exception("columns is empty")
                val cidx = new Array[Int](cols.length)
                for i <- 0 until cols.length do 
                    val c = cols(i)
                    var j = -1 
                    breakable(
                        for k <- 0 until tbi.cols.length do 
                            if c.getColumnName().toLowerCase() == tbi.cols(k).name then 
                                j = k
                                break()
                    )
                    if j<0 then 
                        throw new Exception(s"field ${c.getColumnName()} not exists")
                    else
                        cidx(i) = j 
                // process values. check data type.
                val data = ArrayBuffer[(String,String)]()
                Option(ins.getValues()) match
                    case None => throw new Exception("insert values is empty")
                    case Some(vals:Values) => 
                        val rows = vals.getExpressions.asScala.toArray
                        breakable(
                            for exp <- rows do exp match
                                case row:ParenthesedExpressionList[Expression] =>
                                    // muti-rows insert
                                    val r = row.getExpressions().asScala.toArray 
                                    val (k,v) = format(table,cidx,r) 
                                    data.append((k,v))
                                case _:Expression => break() // single row insert 
                                case null => throw new Exception("insert values is empty")
                        )
                        if data.length == 0 then 
                            data.append(format(table,cidx,rows))
                (table,data)
    // the row record format: head+data --> the head is a offset array.
    private def format(table:String,cidx:Array[Int],row:Array[Expression]):(String,String) =
        tbs.get(table) match
            case Some(tbi) => 
                // head: offset array
                var key:String = ""
                val data = new Array[Array[Byte]](tbi.cols.length)
                for i <- 0 until cidx.length do 
                    val j = cidx(i)
                    val exp = row(i)
                    val col = tbi.cols(j)
                    data(j) = exp match
                        case lv:LongValue => 
                            if col.ctype != "int" || col.ctype != "bigint" then throw SQLEngine.errValueFmt
                            Util.longToBytes(lv.getValue)
                        case sv: StringValue    => 
                            if col.ctype != "char" || col.ctype != "varchar" then throw SQLEngine.errValueFmt
                            sv.getValue.getBytes()
                        case dv: DoubleValue => 
                            if col.ctype != "float" || col.ctype != "double" then throw SQLEngine.errValueFmt
                            Util.doubleToBytes(dv.getValue)
                        case jp: JdbcParameter  => jp.toString.getBytes(StandardCharsets.UTF_8)
                        case _  => exp.toString.getBytes(StandardCharsets.UTF_8)
                    if col.name == tbi.pk then 
                        if col.auto then // TODO: if exp not null,return a error 
                            key = s"${col.autoNext}"
                            col.autoNext += 1
                        else
                            key = exp.toString
                for i <- 0 until tbi.cols.length do 
                    if tbi.cols(i).auto then 
                        data(i) = Util.longToBytes(tbi.cols(i).autoNext)
                        tbi.cols(i).autoNext += 1 
                    // TODO process default value
                // default primary key.
                if key == "" && tbi.pk == "" then 
                    key = s"${tbi.rowId}"
                    tbi.rowId += 1 

                var size:Int = tbi.cols.length*4
                for d <- data do {
                    size += d.length
                }
                var buf:ByteBuffer = ByteBuffer.allocate(size)
                var offset:Int = tbi.cols.length*4
                for d <- data do 
                    offset += d.length
                    buf.putInt(offset)
                for d <- data if d.length > 0 do buf.put(d)
                (key,new String(buf.array(), StandardCharsets.UTF_8))
            case None => throw new Exception("table wrong")
    //
    private def splitRow(row:String,tbi:tableInfo):Array[Array[Byte]] = 
        val arr = row.getBytes(StandardCharsets.UTF_8)
        val res = new Array[Array[Byte]](tbi.cols.length)
        var l:Int = tbi.cols.length*4
        for i <- 0 until tbi.cols.length do 
            val r = Util.bytesToInt(arr.slice(i,i+4))
            if r - l > 0 then 
                res(i) = arr.slice(l,r)
        res
    //
    private def parseDelete(del:Delete,tx:Transaction):(String,ArrayBuffer[String],Boolean) = 
        val table = del.getTable().getName()
        if !contains(table) then throw SQLEngine.errNoTable
        tbs.get(table) match
            case None => throw new Exception("table not exists")
            case Some(tbi) => del.getWhere() match
                case null => (table,null,true)
                case exp:Expression => 
                    val eva = new SQLEvaluator(tbi,null)
                    val arr = ArrayBuffer[String]()
                    tx.openBucket(table) match
                        case Failure(e) =>  throw e 
                        case Success(bk) =>
                            for (k,v) <- bk.iterator do 
                                v match
                                    case None => None
                                    case Some(value) => 
                                        eva.row = splitRow(value,tbi)
                                        val (res,_) = exp.accept(eva,null)
                                        if res.length > 0 && (res(0).toInt & 1) != 0 then 
                                            k match
                                                case Some(key) => arr.append(key)
                                                case None => None 
                    (table,arr,false)
    //
    private def execUpdate(up:Update,tx:Transaction):Try[SQLResult] = 
        val table = up.getTable().getName().toLowerCase()
        if !contains(table) then 
            throw SQLEngine.errNoTable
        tbs.get(table) match
            case Some(tbi) =>
                // check set columns and values
                val cidx = new ArrayBuffer[Int]()
                val vals = new ArrayBuffer[Array[Byte]]()
                val eva = new SQLEvaluator(tbi,null)
                val set = up.getUpdateSets().asScala.toList
                set.foreach( s =>
                    val cols = s.getColumns().asScala.toList
                    val exps = s.getValues().asScala.toList
                    cols.zip(exps).foreach( (col,exp) => 
                        val name = col.getColumnName().toLowerCase()
                        breakable(
                            for i <- 0 until tbi.cols.length do 
                                if tbi.cols(i).name == name then 
                                    cidx.append(i)
                                    if tbi.pk == name then 
                                        throw new Exception(s"'${name}' column is primary key")
                                    if tbi.cols(i).auto then 
                                        throw new Exception(s"'${name}' column is auto_increment")
                                    val (v,tp) =  exp.accept(eva,null)
                                    if !eva.checkType(tp,tbi.cols(i).ctype) then 
                                        throw new Exception(s"column ${name} type is ${tbi.cols(i).ctype}")
                                    vals.append(v)
                                    break()
                        )
                    )
                )
                // filter table rows,generate new row
                val w = up.getWhere()
                val keys = new ArrayBuffer[String]()
                val rows = new ArrayBuffer[Array[Array[Byte]]]()
                val bk = tx.openBucket(table) match
                    case Failure(e) => throw e 
                    case Success(bk) => bk 
                for (k,v) <- bk.iterator do 
                    v match
                        case Some(str) =>
                            eva.row = splitRow(str,tbi)
                            val (ok,_) = w.accept(eva,null)
                            if ok.length > 0 && (ok(0)&1) != 0 then 
                                k match
                                    case Some(key) => 
                                        keys.append(key)
                                        rows.append(eva.row)
                                    case None => None
                        case None => None 
                // update table
                val res = SQLResult(0,keys.length.toLong)
                var buf:ByteBuffer = ByteBuffer.allocate(tbi.cols.length*4)
                keys.zip(rows).foreach( (key,vs) => 
                    for i <- cidx do 
                        vs(i) = vals(i)
                    buf.clear()
                    var offset:Int = tbi.cols.length*4
                    for d <- vs do 
                        offset += d.length
                        buf.putInt(offset)
                    for d <- vs if d.length > 0 do buf.put(d)
                    bk.put(key,new String(buf.array(),StandardCharsets.UTF_8)) match 
                        case Failure(e) => throw e 
                        case Success(_) => None 
                )
                Success(res)
            case None => throw SQLEngine.errNoTable
    //
    private def execDelete(del:Delete,tx:Transaction):Try[SQLResult] = 
        val table = del.getTable().getName()
        if !contains(table) then throw SQLEngine.errNoTable
        val (keys,all) = tbs.get(table) match
            case None => throw new Exception("table not exists")
            case Some(tbi) => del.getWhere() match
                case null => (null,true)
                case exp:Expression => 
                    val eva = new SQLEvaluator(tbi,null)
                    val arr = ArrayBuffer[String]()
                    tx.openBucket(table) match
                        case Failure(e) =>  throw e 
                        case Success(bk) =>
                            for (k,v) <- bk.iterator do 
                                v match
                                    case None => None
                                    case Some(value) => 
                                        eva.row = splitRow(value,tbi)
                                        val (res,_) = exp.accept(eva,null)
                                        if res.length > 0 && (res(0).toInt & 1) != 0 then 
                                            k match
                                                case Some(key) => arr.append(key)
                                                case None => None 
                            (arr,false)
        tx.openBucket(table) match
            case Failure(e) => throw e 
            case Success(bk) => 
                if all then 
                    val r = bk.length
                    bk.clean()
                    Success(SQLResult(0,r))
                else 
                    for key <- keys do 
                        bk.delete(key) match
                            case Failure(e) => throw e 
                            case Success(_) => None
                    Success(SQLResult(0,keys.length.toLong))
    //
    def exec(stmt:String):Try[SQLResult] = 
        if !openFlag then return Failure(SQLEngine.errClosed)
        var res:SQLResult = SQLResult(0,0)
        val r = db.begin(true) match
            case Failure(e) => Failure(e)
            case Success(tx) => txExec(stmt,tx) match
                case Failure(e) => tx.rollback()
                case Success(s) => 
                    res = s 
                    tx.commit() 
        r match
            case Failure(e) => Failure(e)
            case Success(_) => Success(res)
    // create table,drop table
    // insert into...
    // update...
    // delete...
    private[platdb] def txExec(stmt:String,tx:Transaction):Try[SQLResult] = 
        try
            if !openFlag then return Failure(SQLEngine.errClosed)
            val sql = preprocess(stmt)
            val stat: Statement = CCJSqlParserUtil.parse(sql) // JSQLParserException
            stat match
                case ct:CreateTable => 
                    val tb = parseCreateTable(ct)
                    // 4.create a tables bucket with table_name. reate a table_info record, save it to tableTb
                    val tbInfo:String = tb.toJson.compactPrint
                    tx.createBucket(tb.name) match
                        case Failure(e) => throw e
                        case Success(_) => None 
                    tx.openBucket(tableTb) match 
                        case Failure(e) => throw e
                        case Success(bk) => bk.put(tb.name,tbInfo) match
                            case Failure(e) => throw e
                            case Success(_) => 
                                tbs.put(tb.name,tb)
                                Success(SQLResult(0,0))
                case dp:Drop => 
                    val name = parseDrop(dp)
                    // 2. delete table bucket, delete table_info
                        tx.deleteBucket(name) match
                            case Failure(e) => throw e 
                            case Success(_) => None 
                        tx.openBucket(tableTb) match 
                            case Failure(e) => throw e
                            case Success(bk) => bk.delete(name) match 
                                case Failure(e) => throw e 
                                case Success(_) => 
                                    tbs.remove(name)
                                    Success(SQLResult(0,0)) // TODO get table length.
                case ins:Insert => 
                    val (table,data) = parseInsert(ins)
                    // insert the data to db
                    tx.openBucket(table) match
                        case Failure(e) => throw e 
                        case Success(bk) =>
                            for (k,v) <- data do 
                                bk.contains(k) match
                                    case Failure(e) => throw e 
                                    case Success(flag) => 
                                        if flag then 
                                            throw new Exception("duplicate primary key")
                                        else
                                            bk.put(k,v) match 
                                                case Failure(e) => throw e 
                                                case Success(_) => None 
                    tx.openBucket(tableTb) match
                        case Failure(e) => throw e 
                        case Success(bk) => tbs.get(table) match
                            case None => throw new Exception("table wrong")
                            case Some(info) => bk.put(table,info.toJson.compactPrint) match
                                case Failure(e) => throw e 
                                case Success(_) => Success(SQLResult(0,data.length))
                case up:Update => execUpdate(up,tx)
                case del:Delete => execDelete(del,tx)
                case _ => Failure(new Exception("not support the sql now"))
        catch 
            case ex:Exception => Failure(ex)
            case er:Error => Failure(er)

    // create a new transaction.
    def beginTx(readOnly:Boolean):Try[SQLTx] = 
        if !openFlag then 
            Failure(SQLEngine.errClosed)
        else 
            db.begin(!readOnly) match
                case Failure(e) => Failure(e)
                case Success(tx) =>
                    dbTx.put(tx.id,tx)
                    Success(new sqlTx(tx.id,this))
    //
    private def preprocess(s:String):String =
        //(for ss <- s.split(";") if s.length > 0 yield ss.replaceAll("`","").replaceAll("\"","")).toArray
        s.replaceAll("`","").replaceAll("\"","")