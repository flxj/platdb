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

import scala.util.control.Breaks._
import scala.collection.mutable.{ArrayBuffer}
import net.sf.jsqlparser.expression.Expression

private[platdb] class Record(val pk:Array[Byte],var row:Array[Array[Byte]]):
    def get(i:Int):Any = row(i)
    
private[platdb] trait Operator:
    def open():Unit
    def close():Unit
    def next():Option[Record]

private[platdb] class SeqScanOperator(val tx:Transaction,val tbi:tableInfo,val db:SQLEngine) extends Operator:
    private var iter:RawIterator = null
    private var flag:Boolean = false

    def open():Unit = 
        tx.openRawBucket(tbi.name) match
            case None => throw new Exception(s"table ${tbi.name} open failed")
            case Some(bk) => iter = bk.iterator
    def close():Unit = iter = null
    def next():Option[Record] = 
        if flag then
            iter.next() match
                case None => None
                case Some(k,v) => Some(new Record(k,db.decode(v,tbi)))
        else
            flag = true
            iter.first() match
                case None => None
                case Some(k,v) => Some(new Record(k,db.decode(v,tbi)))
    
private[platdb] class FilterOperator(val child:Operator,val exp:Expression,val pred:SQLEvaluator) extends Operator:
    def open(): Unit = child.open()
    def close():Unit = child.close()
    def next():Option[Record] = 
        var r:Option[Record] = None
        breakable (
            while true do 
                r = child.next() 
                r match
                    case None => break()
                    case Some(re) =>
                        if exp != null then
                            pred.row = re.row 
                            val (ok,_) = exp.accept(pred,null)
                            if ok.length > 0 && (ok(0)&1) != 0 then
                                break()
                        else
                            break()
        )
        r

private[platdb] class ProjectOperator(val child:Operator,val db:SQLEngine,val cidx:ArrayBuffer[Int]) extends Operator:
    private def project(cidx:ArrayBuffer[Int],row:Array[Array[Byte]]):Array[Array[Byte]] = 
        val res = new Array[Array[Byte]](cidx.length)
        for (j,i) <- cidx.zipWithIndex do 
            res(i) = if (row(j) == null || row(j).length == 0) then null else row(j) 
        res
    def open(): Unit =  child.open()
    def close(): Unit = child.close()
    def next(): Option[Record] = 
        child.next() match
            case Some(v) => Some(new Record(v.pk,project(cidx,v.row)))
            case None => None

private[platdb] class QueryExecutor(db:SQLEngine,tx:Transaction,rbWhenClose:Boolean):
    private var project:Operator = null
    private var openFlag:Boolean = false

    var dataTypes:ArrayBuffer[ColumnValueType] = null 
    var columns:Array[String] = null 
    var tbi:tableInfo = null
    var pred:Expression = null 
    var cidx:ArrayBuffer[Int] = null
    var dummy:Boolean = false

    def open():Unit = 
        if tx.closed then
            throw new Exception("transaction has been closed, and the current excetor has expired")
        if openFlag then
            return None
        // create operator
        val scan = new SeqScanOperator(tx,tbi,db)
        val filter = new FilterOperator(scan,pred,new SQLEvaluator(tbi,null))
        project = new ProjectOperator(filter,db,cidx)
        project.open()
        openFlag = true

    def close():Unit = 
        try 
            if !openFlag then
                return None
            openFlag = true
        finally
            if rbWhenClose && tx != null then
                tx.rollback()
    
    def getColumns:Array[String] = columns
    def getTypes:Array[ColumnValueType] = dataTypes.toArray
    def getData():Array[Array[Any]] = 
        if !openFlag || tx.closed then
            null
        else
            val res = new ArrayBuffer[Array[Any]]()
            tx.openRawBucket(tbi.name) match
                case None => throw new Exception(s"table ${tbi.name} open failed")
                case Some(bk) =>
                    val eva = new SQLEvaluator(tbi,null)
                    for kv <- bk.iterator do 
                        kv match
                            case None => None
                            case Some(_,v) => 
                                val row = db.decode(v,tbi)
                                var hit:Boolean = true
                                if pred != null then
                                    eva.row = row
                                    val (ok,_) = pred.accept(eva,null)
                                    hit = ok.length > 0 && (ok(0)&1) != 0
                                        
                                if hit then
                                    val r = db.projectAndConvert(cidx,dataTypes,row)
                                    res.append(r)
            res.toArray
    def next():Option[Array[Any]] = 
        if !openFlag || tx.closed then
            null
        else
            project.next() match
                case None => None
                case Some(r) => Some(db.convert(r.row,dataTypes))
                    
