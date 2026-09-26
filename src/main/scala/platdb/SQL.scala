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
import scala.util.matching.Regex
import java.nio.charset.Charset
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.sql.SQLException

import spray.json._
import spray.json.{DefaultJsonProtocol,RootJsonFormat}

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{Table,Column}
import net.sf.jsqlparser.statement.{Statement,ShowStatement,ShowColumnsStatement}
import net.sf.jsqlparser.statement.select.{PlainSelect, Select,SelectVisitorAdapter,SelectItemVisitorAdapter,SelectItem}
import net.sf.jsqlparser.statement.select.{FromItemVisitorAdapter,AllColumns,Values}
import net.sf.jsqlparser.statement.create.table.{CreateTable, ColumnDefinition,Index}
import net.sf.jsqlparser.statement.drop.Drop
import net.sf.jsqlparser.statement.update.{Update,UpdateSet}
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.show.ShowTablesStatement
import net.sf.jsqlparser.expression.{Expression,BooleanValue, LongValue, StringValue, DoubleValue, NullValue, Function, JdbcParameter}
import net.sf.jsqlparser.expression.operators.relational.{ExpressionList,ParenthesedExpressionList}
import net.sf.jsqlparser.expression.{ExpressionVisitorAdapter,AllValue,BinaryExpression}
import net.sf.jsqlparser.expression.operators.conditional.{OrExpression,AndExpression}
import net.sf.jsqlparser.expression.operators.relational.{EqualsTo,NotEqualsTo, GreaterThan, GreaterThanEquals, MinorThan, MinorThanEquals}
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression

/**
  * SQLOptions represents the parameters required to create an SQLEngine object.
  * The parameters of the current object are consistent with those required to create the platdb.DB object.
  */
case class SQLEngineOptions(path:String,dbOps:Options)

/**
  * DBTransaction represents an SQLEngine transaction interface, 
  * and errors encountered during transaction execution will be thrown as Exception.
  * 
  */
trait DBTransaction:
    // Submit transaction 
    def commit():Unit
    // Rollback transaction 
    def rollback():Unit
    // Execute query statements, such as select/show 
    def query(stmt:String):Rows
    //
    def queryIter(stmt:String):RowsIter
    // Execute DD or DML statements, such as create/drop/insert/update/delete
    def exec(stmt:String):Result

private class sqlTx(val id:Long,val se:SQLEngine) extends DBTransaction:
    def commit(): Unit = 
        se.dbTx.remove(id) match
            case None => None
            case Some(tx) => tx.commit()
    def rollback(): Unit = 
        se.dbTx.remove(id) match
            case None => None
            case Some(tx) => tx.rollback() //TODO: clean tableInfo in db.tbs if need.
    def query(stmt: String): Rows = 
        se.dbTx.get(id) match
            case None => throw new Exception("wrong tx")
            case Some(tx) => se.txQuery(stmt,tx)
    
    def queryIter(stmt: String): RowsIter = 
        se.dbTx.get(id) match
            case None => throw new Exception("wrong tx")
            case Some(tx) => se.txQueryIter(stmt,tx)
    def exec(stmt:String):Result = 
        se.dbTx.get(id) match
            case None => throw new Exception("wrong tx")
            case Some(tx) => se.txExec(stmt,tx)

object SQLEngine:
    val maxColumnNameLen:Int = 1024
    val maxTableNameLen:Int = 1024
    val errValueType = new SQLException("the value doesn't match the column field type")
    val errClosed = new SQLException("db is closed state")
    val errNoTable = new SQLException("not found table")
    val errInnerTable = new SQLException("internal table error")
    val errNotSup = new SQLException("not support the sql now")
    def defaultCharset:Charset = StandardCharsets.UTF_8
    def apply(ops: SQLEngineOptions):SQLEngine = new SQLEngine(ops)
    def apply(path:String)(using ops:Options):SQLEngine = new SQLEngine(SQLEngineOptions(path,ops))

/**
  * The SQLEngine object encapsulates the underlying platdb KV storage interface. 
  * As a computing layer, it supports basic SQL statements and database transactions, 
  * making PlatDB usable as a simple embedded relational database.
  * 
  */
class SQLEngine(ops:SQLEngineOptions):
    import tableProto._
    // The metadata table of the database.
    private val metaTb:String = "platdb_info_meta"
    // Database table information, storing JSON data for all table names and corresponding table information.
    private val tableTb:String = "platdb_info_table"
    private val indexTb:String = "platdb_info_index"
    private val charRgx:Regex = """char\((\d*)\)""".r
    private val varcharRgx:Regex = """varchar\((\d*)\)""".r
    private var openFlag:Boolean = false
    private var db:DB = null
    // Cache table information.
    private[platdb] var tbs:Map[String,tableInfo] = Map[String,tableInfo]()
    private[platdb] var dbTx:Map[Long,Transaction] = Map[Long,Transaction]()

    /**
      * The default encoding of the database cannot be changed currently.
      */
    def defaultCharset:Charset = StandardCharsets.UTF_8
    /**
      * Initialize the database. Before using SQLEngine, 
      * this method must be called once to open the underlying storage.
      * 
      */
    def open():Try[Unit] =
        try
            if openFlag then
                return Success(None)
            db = new DB(ops.path)(using ops.dbOps)
            db.open() match
                case Failure(e) => throw e
                case Success(_) => None
            // 1. create catalog table if not exists
            //  platdb_info_meta bucket --> table_id,indx_id
            //  platdb_info_tables bucket --> key:table_name, value:table_info
            //  platdb_info_index bucket  --> key:table_name, value:index_info
            
            // table(raw bucket) --> bucket_name:table_name, 
            //                   key: tablePrefix_{TableID}_recordPrefixSep_{RowID}
            //                   value: record_head + column_value
            db.update (
                (tx:Transaction) =>
                    tx.createBucketIfNotExists(metaTb) 
                    tx.createBucketIfNotExists(tableTb) 
                    tx.createBucketIfNotExists(indexTb) 
            ) match
                case Failure(e) => throw e
                case Success(_) => None
            openFlag = true 
            Success(None)
        catch
            case ex:Exception => Failure(ex)
            case er:Error => Failure(er)
    /**
      * Close the database. 
      * After using SQLEngine (or exiting due to an exception), 
      * this method needs to be executed once to shut down the database, 
      * which will perform some necessary cleaning operations.
      */
    def close():Try[Unit] = 
        if !openFlag then return Success(None)
        try
            for (id,tx) <- dbTx do 
                tx.rollback() 
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
    /**
      * Execute SQL query statements and return query results or exception information. 
      * Note that this statement will be executed as a transaction.
      */
    def query(stmt:String):Try[Rows] =  
        if !openFlag then 
            return Failure(SQLEngine.errClosed)
        var tx:Transaction = null 
        try
            db.begin(false) match
                case Failure(e) => throw e
                case Success(t) => tx = t 
            val res = txQuery(stmt,tx) 
            Success(res)
        catch
            case e:Exception => Failure(e)
            case e:Error => Failure(e)
        finally
            if tx != null && !tx.closed then tx.rollback()
    //
    def queryIter(stmt:String):Try[RowsIter] =  
        if !openFlag then 
            return Failure(SQLEngine.errClosed)
        var tx:Transaction = null 
        try
            db.begin(false) match
                case Failure(e) => throw e
                case Success(t) => tx = t 
            val res = parseQuery(stmt,tx,true) 
            res.open()
            Success(res)
        catch
            case e:Exception => 
                if tx != null && !tx.closed then 
                    tx.rollback()
                Failure(e)
    //
    def txQueryIter(stmt:String,tx:Transaction):RowsIter =  
        if !openFlag then 
            throw SQLEngine.errClosed
        val res = parseQuery(stmt,tx,false) 
        res.open()
        res
    /**
      * Execute SQL statements and return execution results or exception information. 
      * Note that this statement will be executed as a transaction.
      */
    def exec(stmt:String):Try[Result] = 
        if !openFlag then return Failure(SQLEngine.errClosed)
        var tx:Transaction = null
        try
            db.begin(true) match
                case Failure(e) => throw e 
                case Success(t) => tx = t 
            val res = txExec(stmt,tx) 
            tx.commit() 
            Success(res)
        catch 
            case e:Exception => Failure(e)
            case e:Error => Failure(e)
        finally
            if tx != null && !tx.closed then tx.rollback()
    /**
      * Open a read-only or read-write transaction.
      */
    def beginTx(readOnly:Boolean):Try[DBTransaction] = 
        if !openFlag then 
            Failure(SQLEngine.errClosed)
        else 
            db.begin(!readOnly) match
                case Failure(e) => Failure(e)
                case Success(tx) =>
                    dbTx.put(tx.id,tx)
                    Success(new sqlTx(tx.id,this))
    // Check if the table exists.
    private def contains(tx:Transaction,name:String):Boolean =
        if tbs.contains(name) then
            true
        else
            tx.openBucket(tableTb) match
                case Some(bk) => bk.get(name) match
                    case Some(s) => 
                        val tb = s.parseJson.convertTo[tableInfo]
                        tbs.put(name,tb)
                        true
                    case None => false
                case None => throw SQLEngine.errInnerTable
    // Preprocess SQL statements.
    private def preprocess(s:String):String =
        if s.count(p => p ==';') > 1 then
            throw new SQLException("not support muti-rows statement now")
        //(for ss <- s.split(";") if s.length > 0 yield ss.replaceAll("`","").replaceAll("\"","")).toArray
        s.replaceAll("`","").replaceAll("\"","")
    // Execute statements in the specified transaction environment.
    private[platdb] def txExec(stmt:String,tx:Transaction):Result = 
        if !openFlag then throw SQLEngine.errClosed
        val sql = preprocess(stmt)
        val stat: Statement = CCJSqlParserUtil.parse(sql) // JSQLParserException
        stat match
            case ct:CreateTable => 
                val tb = parseCreateTable(ct)
                // check table if exists.
                if contains(tx,tb.name) then
                    if ct.isIfNotExists() then
                        return Result(0,0)
                    else
                        throw new SQLException(s"already exists table ${tb.name}")
                // 4.create a tables bucket with table_name. 
                // reate a table_info record, save it to tableTb
                val tbInfo:String = tb.toJson.compactPrint
                tx.createRawBucket(tb.name) 
                tx.openBucket(tableTb) match 
                    case None => throw SQLEngine.errInnerTable
                    case Some(bk) => 
                        bk.put(tb.name,tbInfo) 
                        tbs.put(tb.name,tb)
                        Result(0,0)
            case dp:Drop => 
                val name = dp.getName().getName().toLowerCase()
                if !contains(tx,name) then
                    return Result(0,0)
                // 2. delete table bucket, delete table_info
                tx.deleteRawBucket(name)
                tx.openBucket(tableTb) match 
                    case None => throw SQLEngine.errInnerTable
                    case Some(bk) =>
                        bk.delete(name) 
                        tbs.remove(name)
                        Result(0,0) // TODO get table length.
            case ins:Insert => 
                val (table,data) = parseInsert(ins,tx)
                // insert the data to db
                tx.openRawBucket(table) match
                    case None => throw SQLEngine.errNoTable
                    case Some(bk) =>
                        for (k,v) <- data do 
                            if bk.contains(k) then
                                throw new SQLException(s"duplicate primary key ${k}")
                            else
                                bk.put(k,v)
                tx.openBucket(tableTb) match
                    case Some(bk) => tbs.get(table) match
                        case Some(info) => 
                            bk.put(table,info.toJson.compactPrint) 
                            Result(0,data.length)
                        case None => throw SQLEngine.errInnerTable
                    case None => throw SQLEngine.errInnerTable
            case up:Update => execUpdate(up,tx)
            case del:Delete => execDelete(del,tx)
            case _ => throw SQLEngine.errNotSup
    // Execute statements in the specified transaction environment.
    private[platdb] def txQuery(stmt:String,tx:Transaction):Rows = 
        if !openFlag then throw SQLEngine.errClosed
        val sql = preprocess(stmt)
        val stat: Statement = CCJSqlParserUtil.parse(sql) // JSQLParserException
        stat match
            case sel:Select => sel.getSelectBody() match
                case ps:PlainSelect =>
                    // 1.check table
                    var name:String = ""
                    Option(ps.getFromItem).collect { case table: Table =>
                        name = table.getName
                    }
                    if !contains(tx,name) then throw SQLEngine.errNoTable
                    val tbi = tbs.get(name) match
                        case Some(v) => v 
                        case None => throw SQLEngine.errNoTable
                    
                    // 2.check column
                    var allFlag:Boolean = false 
                    var cntFlag:Boolean = false 
                    val columns = ArrayBuffer[(String,String)]()

                    if ps.getSelectItems() == null then
                        throw new SQLException("select statement columns is empty")
                    val items = ps.getSelectItems().asScala
                    items.foreach( item =>
                        item.accept(new SelectItemVisitorAdapter[Unit] {
                            override def visit[S](si: SelectItem[? <: Expression], context: S): Unit = 
                                si.getExpression() match
                                    case col:Column =>
                                        val al = Option(si.getAlias).map(_.getName).getOrElse("")
                                        columns.append((col.getColumnName().toLowerCase(),al))
                                    case all:AllColumns => allFlag = true 
                                    case f:Function => 
                                        if f.getName().toLowerCase() == "count"  then
                                            if f.getParameters() != null then
                                                f.getParameters().asScala.toList.foreach( arg =>
                                                    if arg.toString() == "*" then cntFlag = true 
                                                )
                                        else
                                            throw new SQLException(s"not support such function ${f.toString()} now")
                                    case _ => None
                        },null)
                    )
                    if columns.length == 0 && !allFlag && !cntFlag then 
                        throw new SQLException("query column is empty")
                    else if allFlag && columns.length != 0 then 
                        throw new SQLException("not support such columns")
                    else if cntFlag && (columns.length != 0 || allFlag) then
                        throw new SQLException("not support such columns")

                    if cntFlag then 
                        tx.openRawBucket(name) match
                            case Some(bk) => 
                                val data = Array[Array[Any]](Array[Any](bk.length))
                                return Rows(Array[String]("count(*)"),Array[String]("int"),data)
                            case None => throw SQLEngine.errInnerTable
                    // get columns type info
                    val ftypes = ArrayBuffer[String]()
                    val cidx = ArrayBuffer[Int]()
                    if allFlag then 
                        for (col,i) <- tbi.cols.zipWithIndex do 
                            columns.append((col.name,""))
                            ftypes.append(col.ctype)
                            cidx.append(i)
                    else
                        for (c,_) <- columns do 
                            var i:Int = -1
                            breakable(
                                for (col,j) <- tbi.cols.zipWithIndex do 
                                    if col.name == c then 
                                        i = j 
                                        break()
                            )
                            if i < 0 then throw new SQLException(s"not found column ${c}")
                            ftypes.append(tbi.cols(i).ctype)
                            cidx.append(i)
                    // 3.filter row by where 
                    val rd = new ArrayBuffer[Array[Any]]()
                    ps.getWhere() match
                        case null => tx.openRawBucket(name) match
                            case None => None 
                            case Some(bk) => 
                                for kv <- bk.iterator do 
                                    kv match
                                        case Some(_,str) =>
                                            val row = decode(str,tbi)
                                            rd.append(project(cidx,ftypes,row))
                                        case None => None 
                        case exp:Expression => 
                            val eva = new SQLEvaluator(tbi,null)
                            tx.openRawBucket(name) match
                                case None => None 
                                case Some(bk) =>
                                    for kv <- bk.iterator do kv match 
                                        case Some(_,str) => 
                                            val row = decode(str,tbi)
                                            eva.row = row 
                                            val (ok,_) = exp.accept(eva,null)
                                            if ok.length > 0 && (ok(0)&1) != 0 then
                                                rd.append(project(cidx,ftypes,row))
                                        case None => None
                    val cols = (for (a,b) <- columns yield if b != "" then b else a).toArray
                    Rows(cols,ftypes.toArray,rd.toArray)
                case _ => throw SQLEngine.errNotSup  
            case st:ShowTablesStatement => 
                // list all tables name
                val ts = new ArrayBuffer[Array[Any]]
                tx.openBucket(tableTb) match
                    case None => None
                    case Some(bk) => 
                        for kv <- bk.iterator do kv match
                            case Some(k,_) => ts.append(Array[Any](k))
                            case None => None
                Rows(Array[String]("tables"),Array[String]("string"),ts.toArray)
            case sc:ShowColumnsStatement => 
                val table = sc.getTableName().toLowerCase()
                tx.openBucket(tableTb) match
                    case None => throw SQLEngine.errInnerTable
                    case Some(bk) => bk.get(table) match
                        case None => throw SQLEngine.errNoTable
                        case Some(v) => 
                            val info = v.parseJson.convertTo[tableInfo]
                            val rows = new Array[Array[Any]](info.cols.length)
                            for (col,i) <- info.cols.zipWithIndex do 
                                rows(i) = Array[Any](col.name,col.ctype,"")
                                if col.name == info.pk then
                                    rows(i)(2) = "primary key"
                            Rows(Array[String]("column_name","data_type","specs"),Array[String]("string","string","string"),rows)
            case _ => throw SQLEngine.errNotSup
    //
    private def parseQuery(stmt:String,tx:Transaction,sysTx:Boolean):RowsIter = 
        if !openFlag then throw SQLEngine.errClosed
        val sql = preprocess(stmt)
        val stat: Statement = CCJSqlParserUtil.parse(sql) // JSQLParserException
        val exec:QueryExecutor = new QueryExecutor(this,tx,sysTx)
        stat match
            case sel:Select => sel.getSelectBody() match
                case ps:PlainSelect =>
                    // 1.check table
                    var name:String = ""
                    Option(ps.getFromItem).collect { case table: Table =>
                        name = table.getName
                    }
                    if !contains(tx,name) then throw SQLEngine.errNoTable
                    val tbi = tbs.get(name) match
                        case Some(v) => v 
                        case None => throw SQLEngine.errNoTable
                    exec.tbi = tbi
                    
                    // 2.check column
                    var allFlag:Boolean = false 
                    var cntFlag:Boolean = false 
                    val columns = ArrayBuffer[(String,String)]()

                    if ps.getSelectItems() == null then
                        throw new SQLException("select statement columns is empty")
                    val items = ps.getSelectItems().asScala
                    items.foreach( item =>
                        item.accept(new SelectItemVisitorAdapter[Unit] {
                            override def visit[S](si: SelectItem[? <: Expression], context: S): Unit = 
                                si.getExpression() match
                                    case col:Column =>
                                        val al = Option(si.getAlias).map(_.getName).getOrElse("")
                                        columns.append((col.getColumnName().toLowerCase(),al))
                                    case all:AllColumns => allFlag = true 
                                    case f:Function => 
                                        if f.getName().toLowerCase() == "count"  then
                                            if f.getParameters() != null then
                                                f.getParameters().asScala.toList.foreach( arg =>
                                                    if arg.toString() == "*" then cntFlag = true 
                                                )
                                        else
                                            throw new SQLException(s"not support such function ${f.toString()} now")
                                    case _ => None
                        },null)
                    )
                    if columns.length == 0 && !allFlag && !cntFlag then 
                        throw new SQLException("query column is empty")
                    else if allFlag && columns.length != 0 then 
                        throw new SQLException("not support such columns")
                    else if cntFlag && (columns.length != 0 || allFlag) then
                        throw new SQLException("not support such columns")

                    if cntFlag then 
                        tx.openRawBucket(name) match
                            case Some(bk) => 
                                val data = Array[Array[Any]](Array[Any](bk.length))
                                exec.columns = Array[String]("count(*)")
                                exec.dataTypes = ArrayBuffer[ColumnValueType](ColumnValueType.INT)
                                val r = new RowsIter()
                                r.data = data 
                                return r 
                            case None => throw SQLEngine.errInnerTable
                    // get columns type info
                    val ftypes = ArrayBuffer[ColumnValueType]()
                    val cidx = ArrayBuffer[Int]()
                    if allFlag then 
                        for (col,i) <- tbi.cols.zipWithIndex do 
                            columns.append((col.name,""))
                            ftypes.append(col.getValueType())
                            cidx.append(i)
                    else
                        for (c,_) <- columns do 
                            var i:Int = -1
                            breakable(
                                for (col,j) <- tbi.cols.zipWithIndex do 
                                    if col.name == c then 
                                        i = j 
                                        break()
                            )
                            if i < 0 then throw new SQLException(s"not found column ${c}")
                            ftypes.append(tbi.cols(i).getValueType())
                            cidx.append(i)
                    //
                    exec.cidx = cidx
                    exec.dataTypes = ftypes
                    exec.pred = ps.getWhere() match
                        case null => null
                        case exp:Expression => exp
                    exec.columns = (for (a,b) <- columns yield if b != "" then b else a).toArray
                    val r = new RowsIter()
                    r.qExec = exec
                    r
                case _ => throw SQLEngine.errNotSup  
            case st:ShowTablesStatement => 
                // list all tables name
                val ts = new ArrayBuffer[Array[Any]]
                tx.openBucket(tableTb) match
                    case None => None
                    case Some(bk) => 
                        for kv <- bk.iterator do kv match
                            case Some(k,_) => ts.append(Array[Any](k))
                            case None => None
                exec.columns = Array[String]("tables")
                exec.dataTypes = ArrayBuffer[ColumnValueType](ColumnValueType.CHAR)
                exec.dummy = true
                val r = new RowsIter()
                r.qExec = exec
                r.data = ts.toArray
                r
            case sc:ShowColumnsStatement => 
                val table = sc.getTableName().toLowerCase()
                tx.openBucket(tableTb) match
                    case None => throw SQLEngine.errInnerTable
                    case Some(bk) => bk.get(table) match
                        case None => throw SQLEngine.errNoTable
                        case Some(v) => 
                            val info = v.parseJson.convertTo[tableInfo]
                            val rows = new Array[Array[Any]](info.cols.length)
                            for (col,i) <- info.cols.zipWithIndex do 
                                rows(i) = Array[Any](col.name,col.ctype,"")
                                if col.name == info.pk then
                                    rows(i)(2) = "primary key"
                            //
                            exec.columns = Array[String]("column_name","data_type","specs")
                            exec.dataTypes = ArrayBuffer[ColumnValueType](ColumnValueType.CHAR,ColumnValueType.CHAR,ColumnValueType.CHAR)
                            exec.dummy = true
                            val r = new RowsIter()
                            r.qExec = exec
                            r.data = rows
                            r
            case _ => throw SQLEngine.errNotSup
    // Projection operation. Get the required columns.
    private[platdb] def project(cidx:ArrayBuffer[Int],ftype:ArrayBuffer[String],row:Array[Array[Byte]]):Array[Any] = 
        val res = new Array[Any](ftype.length)
        for (j,i) <- cidx.zipWithIndex do 
            res(i) = (
                if row(j) == null || row(j).length == 0 then 
                    null 
                else ftype(i) match
                    case "char"|"varchar" => new String(row(j),defaultCharset) 
                    case "int" => Util.bytesToLong(row(j))
                    case "bigint" => Util.bytesToLong(row(j)) 
                    case "float" =>  Util.bytesToDouble(row(j)) 
                    case "double" => Util.bytesToDouble(row(j))
                    case _ => null
            )
        res
    private[platdb] def projectAndConvert(cidx:ArrayBuffer[Int],ftype:ArrayBuffer[ColumnValueType],row:Array[Array[Byte]]):Array[Any] = 
        val res = new Array[Any](ftype.length)
        for (j,i) <- cidx.zipWithIndex do 
            res(i) = (
                if row(j) == null || row(j).length == 0 then 
                    null 
                else ftype(i) match
                    case ColumnValueType.CHAR|ColumnValueType.VRCHAR => new String(row(j),defaultCharset) 
                    case ColumnValueType.INT => Util.bytesToLong(row(j))
                    case ColumnValueType.BIGINT => Util.bytesToLong(row(j)) 
                    case ColumnValueType.FLOAT =>  Util.bytesToDouble(row(j)) 
                    case ColumnValueType.DOUBLE => Util.bytesToDouble(row(j))
                    case _ => null
            )
        res
    private[platdb] def convert(row:Array[Array[Byte]],ftype:ArrayBuffer[ColumnValueType]):Array[Any] = 
        val res = new Array[Any](ftype.length)
        for (v,i) <- row.zipWithIndex do 
            res(i) = (
                if v == null || v.length == 0 then 
                    null 
                else ftype(i) match
                    case ColumnValueType.CHAR|ColumnValueType.VRCHAR => new String(v,defaultCharset) 
                    case ColumnValueType.INT => Util.bytesToLong(v)
                    case ColumnValueType.BIGINT => Util.bytesToLong(v) 
                    case ColumnValueType.FLOAT =>  Util.bytesToDouble(v) 
                    case ColumnValueType.DOUBLE => Util.bytesToDouble(v)
                    case _ => null
            )
        res
    // Resolve the 'create table...' statement.
    private def parseCreateTable(ct:CreateTable):tableInfo = 
        val name = ct.getTable.getName.toLowerCase()
        if name.length() > SQLEngine.maxTableNameLen then
            throw new SQLException("table name too long")
        // init a table info obj
        val tb = new tableInfo(name)
        // 2.check field type
        val cols = new ArrayBuffer[columnInfo]()
        val cd = (if ct.getColumnDefinitions != null then 
                ct.getColumnDefinitions.asScala.toList
            else 
                throw new SQLException("columns definations is empty")
        )
        cd.foreach( col => 
            val cname = col.getColumnName.toLowerCase()
            if cname.length() > SQLEngine.maxColumnNameLen then
                throw new SQLException("column name too long")
            val ctype = col.getColDataType.getDataType.toLowerCase.replace(" ","")
            var cargs:Array[String] = Array[String]()
            if col.getColDataType.getArgumentsStringList() != null then
                cargs = col.getColDataType.getArgumentsStringList().asScala.toArray
            //val targs = ctype.getArgumentsStringList.asScala.toArray
            val tname = ctype match
                case varcharRgx(n) => 
                    if n.toInt <= 0 || n.toInt > 65535 then
                        throw new SQLException(s"invalid field ${ctype}")
                    "varchar"
                case charRgx(n) => 
                    if n.toInt <= 0 || n.toInt > 255 then
                        throw new SQLException(s"invalid field type ${ctype}")
                    "char"
                case "char" | "varchar" => ctype
                case "int"|"bigint"|"double"|"float" => ctype
                case _ => throw new SQLException(s"not support field type ${ctype}} now")
            val c = new columnInfo(cname,tname)
            val specs = if col.getColumnSpecs != null then col.getColumnSpecs.asScala.toList else List[String]()
            specs.foreach( sp => 
                if sp.toLowerCase() == "auto_increment" then
                    if tname == "int" || tname == "bigint" then 
                        c.auto = true
                    else
                        throw new SQLException(s"${cname} field type is '${tname}',not support AUTO_INCREMENT")
                        
                if sp.toLowerCase == "primary" then
                    if tb.pk != "" then 
                        throw new SQLException("not support composite keys now")
                    else
                        tb.pk = cname
                // TODO: process other options,example, not null, default value
            )
            cols.append(c)
        )
        tb.cols = cols.toArray
        // 
        var index:List[Index] = List[Index]()
        if ct.getIndexes != null then
            index = ct.getIndexes.asScala.toList
        index.foreach( idx =>
            if (idx.getType == "PRIMARY KEY") then 
                val pks = idx.getColumns.asScala.map(_.getColumnName).toArray
                
                if pks.length != 1 then 
                    throw new SQLException("not support composite keys now")

                if tb.pk != "" && tb.pk != pks(0) then
                    throw new SQLException("primary key duplication")

                tb.pk = pks(0)
                var ok:Boolean = false 
                for c <- tb.cols do
                    if c.name == tb.pk then ok = true 
                if !ok then 
                    throw new SQLException("primary key field not exists")
            else
                throw new SQLException("not support secondary index now")
        )
        tb 
    // Resolve the 'insert into...' statement.
    private def parseInsert(ins:Insert,tx:Transaction):(String,ArrayBuffer[(Array[Byte],Array[Byte])]) = 
        val table = ins.getTable().getName().toLowerCase()
        if !contains(tx,table) then 
            throw new SQLException(s"table '${table}' not exists")
        tbs.get(table) match
            case None => throw SQLEngine.errInnerTable
            case Some(tbi) => 
                // check the field exists
                var cols = Array[Column]()
                var cidx:Array[Int] = null
                if ins.getColumns() != null then
                    cols = ins.getColumns().asScala.toArray
                    if cols.length > tbi.cols.length then 
                        throw new SQLException("too much columns")
                    cidx = new Array[Int](cols.length)
                    for (c,i) <- cols.zipWithIndex do 
                        var j = -1 
                        breakable ( 
                            for (col,k) <- tbi.cols.zipWithIndex do 
                                if c.getColumnName().toLowerCase() == col.name then 
                                    j = k
                                    break()
                        )
                        if j < 0 then 
                            throw new SQLException(s"field ${c.getColumnName()} not exists in table ${table}")
                        else
                            cidx(i) = j 
                else
                    // all columns
                    cidx = new Array[Int](tbi.cols.length)
                    for i <- 0 until cidx.length do cidx(i) = i 
                // process values. check data type.
                val data = ArrayBuffer[(Array[Byte],Array[Byte])]() // (key,row_record)
                Option(ins.getValues()) match
                    case None => throw new SQLException("insert values is empty")
                    case Some(vals:Values) => 
                        val rows = vals.getExpressions.asScala.toArray
                        breakable(
                            for exp <- rows do exp match
                                case row:ParenthesedExpressionList[Expression] =>
                                    // muti-rows insert
                                    val r = row.getExpressions().asScala.toArray 
                                    if r.length != cidx.length then
                                        throw new SQLException("columns number and values number not matching")
                                    data.append(encode(tbi,cidx,r))
                                case _:Expression => break() // TODO: single row insert 
                                case null => throw new SQLException("insert values is empty")
                        )
                        if data.length == 0 then 
                            data.append(encode(tbi,cidx,rows))
                (table,data)
    // Encode the data rows of the table into a key value pair. The value format is the column offset+data format.
    private[platdb] def encode(tbi:tableInfo,cidx:Array[Int],row:Array[Expression]):(Array[Byte],Array[Byte]) =
        // head: offset array
        var key:Array[Byte] = null
        val data = new Array[Array[Byte]](tbi.cols.length)
        for (j,i) <- cidx.zipWithIndex do 
            val col = tbi.cols(j)
            data(j) = row(i) match
                case iv:LongValue => 
                    if col.ctype == "int" || col.ctype == "bigint" then
                        Util.longToBytes(iv.getValue)
                    else
                        throw SQLEngine.errValueType
                case dv:DoubleValue => 
                    if col.ctype == "float" || col.ctype == "double" then
                        Util.doubleToBytes(dv.getValue)
                    else
                        throw SQLEngine.errValueType
                case sv:StringValue => 
                    if col.ctype == "char" || col.ctype == "varchar" then 
                        sv.getValue.getBytes(defaultCharset)
                    else
                        throw SQLEngine.errValueType
                case _  => throw new SQLException(s"not support value type ${row(i).toString()}")
            
            if col.name == tbi.pk then 
                if col.auto then // TODO: if exp not null,return a error 
                    key = Util.longToBytes(col.autoNext)
                    tbi.cols(j).autoNext += 1
                else
                    key = row(i).toString().getBytes(defaultCharset)
                data(j) = key
            
        for (col,i) <- tbi.cols.zipWithIndex do  // TODO process default value
            if col.auto && data(i) == null then 
                data(i) = Util.longToBytes(col.autoNext)
                tbi.cols(i).autoNext += 1
                if col.name == tbi.pk then key = data(i)
        // default primary key.
        if key == null && tbi.pk == "" then 
            key = Util.longToBytes(tbi.rowId)
            tbi.rowId += 1

        (key,makeRow(tbi.cols.length*4,data))
    //
    private def makeRowStr(base:Int,data:Array[Array[Byte]]):String = 
        var size:Int = base
        for d <- data do size += (if d != null then d.length else 0)
        var buf:ByteBuffer = ByteBuffer.allocate(size)
        size = base
        for d <- data do 
            size += (if d != null then d.length else 0)
            buf.putInt(size)
        for d <- data if (d != null && d.length > 0) do buf.put(d)
        Base64.getEncoder().encodeToString(buf.array())
    //
    private def makeRow(base:Int,data:Array[Array[Byte]]):Array[Byte] = 
        var size:Int = base
        for d <- data do size += (if d != null then d.length else 0)
        var buf:ByteBuffer = ByteBuffer.allocate(size)
        size = base
        for d <- data do 
            size += (if d != null then d.length else 0)
            buf.putInt(size)
        for d <- data if (d != null && d.length > 0) do buf.put(d)
        buf.array()
    // Decoding a string formatted row data into a table with a record row.
    private def decode(row:String,tbi:tableInfo):Array[Array[Byte]] = 
        val arr = Base64.getDecoder().decode(row)
        val res = new Array[Array[Byte]](tbi.cols.length)
        var l:Int = tbi.cols.length*4
        for i <- 0 until tbi.cols.length do 
            val r = Util.bytesToInt(arr.slice(4*i,4*i+4))
            if r > l  then res(i) = arr.slice(l,r)
            l = r 
        res
    private[platdb] def decode(arr:Array[Byte],tbi:tableInfo):Array[Array[Byte]] = 
        val res = new Array[Array[Byte]](tbi.cols.length)
        var l:Int = tbi.cols.length*4
        for i <- 0 until tbi.cols.length do 
            val r = Util.bytesToInt(arr.slice(4*i,4*i+4))
            if r > l  then res(i) = arr.slice(l,r)
            l = r 
        res
    // Analyze the delete statement and filter the keys that need to be deleted.
    private def parseDelete(del:Delete,tx:Transaction):(String,ArrayBuffer[Array[Byte]],Boolean) = 
        val table = del.getTable().getName().toLowerCase()
        if !contains(tx,table) then throw SQLEngine.errNoTable
        tbs.get(table) match
            case None => throw SQLEngine.errNoTable
            case Some(tbi) => del.getWhere() match
                case null => (table,null,true)
                case exp:Expression => 
                    val eva = new SQLEvaluator(tbi,null)
                    val arr = ArrayBuffer[Array[Byte]]()
                    tx.openRawBucket(table) match
                        case None => None 
                        case Some(bk) =>
                            for kv <- bk.iterator do kv match
                                case None => None
                                case Some(k,value) => 
                                    eva.row = decode(value,tbi)
                                    val (res,_) = exp.accept(eva,null)
                                    if res.length > 0 && (res(0).toInt & 1) != 0 then 
                                        arr.append(k)
                    (table,arr,false)
    // Analyze and execute the update statement.
    private def execUpdate(up:Update,tx:Transaction):Result = 
        val table = up.getTable().getName().toLowerCase()
        if !contains(tx,table) then 
            throw SQLEngine.errNoTable
        tbs.get(table) match
            case Some(tbi) =>
                // check set columns and values
                val cidx = new ArrayBuffer[Int]()
                val newVal = new ArrayBuffer[Array[Byte]]()
                val eva = new SQLEvaluator(tbi,null)
                Option(up.getUpdateSets) match
                    case Some(sets) => sets.asScala.foreach( s =>
                        val cols = s.getColumns().asScala
                        val exps = s.getValues().asScala
                        cols.zip(exps).foreach( (col,exp) => 
                            val name = col.getColumnName().toLowerCase()
                            breakable(
                                for (col,i) <- tbi.cols.zipWithIndex do 
                                    if col.name == name then 
                                        cidx.append(i)
                                        if tbi.pk == name then 
                                            throw new SQLException(s"'${name}' column is primary key")
                                        if col.auto then 
                                            throw new SQLException(s"'${name}' column is auto_increment")
                                        val (v,tp) =  exp.accept(eva,null)
                                        if !eva.checkType(tp,col.ctype) then 
                                            throw new SQLException(s"column ${name} type is ${tbi.cols(i).ctype}")
                                        newVal.append(v)
                                        break()
                            )
                        )
                    )
                    case None => throw new SQLException("set columns is empty")
                if cidx.length == 0 then throw new SQLException("set columns is empty")
                if cidx.length != newVal.length then 
                    throw new SQLException(s"found ${cidx.length} columns but new value ${newVal.length}")
                // filter table rows,generate new row
                val w = Option(up.getWhere())
                val keys = new ArrayBuffer[Array[Byte]]()
                val rows = new ArrayBuffer[Array[Array[Byte]]]()
                tx.openRawBucket(table) match
                    case None => throw SQLEngine.errNoTable
                    case Some(bk) =>
                        for kv <- bk.iterator do kv match
                            case Some(k,str) => w match
                                case None => 
                                    keys.append(k)
                                    rows.append(decode(str,tbi))
                                case Some(exp) => 
                                    eva.row = decode(str,tbi)
                                    val (ok,_) = exp.accept(eva,null)
                                    if ok.length > 0 && (ok(0)&1) != 0 then 
                                        keys.append(k)
                                        rows.append(eva.row)
                            case None => None 
                        // update table
                        keys.zip(rows).foreach( (key,row) => 
                            // update column,and write to kv-store
                            for (i,j) <- cidx.zipWithIndex do row(i) = newVal(j)
                            bk.put(key,makeRow(tbi.cols.length*4,row))
                        )
                // update table info if need.
                //tx.openBucket(tableTb) match4
                //    case None => throw SQLEngine.errInnerTable
                //    case Some(bk) => bk.put(table,tbi.toJson.compactPrint)
                Result(0,keys.length.toLong)
            case None => throw SQLEngine.errNoTable
    //
    private def execDelete(del:Delete,tx:Transaction):Result = 
        val table = del.getTable().getName()
        if !contains(tx,table) then throw SQLEngine.errNoTable
        val (keys,all) = tbs.get(table) match
            case None => throw SQLEngine.errNoTable
            case Some(tbi) => del.getWhere() match
                case null => (null,true)
                case exp:Expression => 
                    val eva = new SQLEvaluator(tbi,null)
                    val arr = ArrayBuffer[Array[Byte]]()
                    tx.openRawBucket(table) match
                        case None => throw SQLEngine.errInnerTable
                        case Some(bk) =>
                            for kv <- bk.iterator do kv match
                                case None => None
                                case Some(k,value) => 
                                    eva.row = decode(value,tbi)
                                    val (res,_) = exp.accept(eva,null)
                                    if res.length > 0 && (res(0).toInt & 1) != 0 then 
                                        arr.append(k)
                            (arr,false)
        tx.openRawBucket(table) match
            case None => throw SQLEngine.errInnerTable
            case Some(bk) => 
                if all then 
                    val r = bk.length
                    bk.clean()
                    Result(0,r)
                else 
                    for key <- keys do bk.delete(key)
                    Result(0,keys.length.toLong)
