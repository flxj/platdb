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

import akka.{NotUsed,Done}
import akka.util.ByteString
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpEntity, HttpResponse }
import akka.http.scaladsl.common.{EntityStreamingSupport,JsonEntityStreamingSupport}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.scaladsl.Source
import akka.stream.scaladsl._
import scala.concurrent.ExecutionContext
import scala.concurrent.{Future,Promise,Await}
import scala.concurrent.duration.Duration
import scala.io.StdIn
import scala.util.{Failure,Success,Try}
import scala.util.control.Breaks._
import scala.collection.mutable.Map
import scala.sys.ShutdownHookThread
import spray.json._
import spray.json.{DefaultJsonProtocol,RootJsonFormat}
import java.io.{File,RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.file.Paths

/**
  * 
  *
  * @param path
  * @param conf
  */
case class ServerOptions(path:String,conf:Options,host:String,port:Int,logLevel:String)

/**
  * 
  */
object Server:
    case class CollectionCreateOptions(name:String,collectionType:String,ignoreExists:Boolean,dimension:Int)
    case class CollectionDeleteOptions(name:String,collectionType:String,ignoreNotExists:Boolean)
    case class CollectionInfo(name:String,collectionType:String)
    case class CollectionGetResponse(collections:List[CollectionInfo])
    //
    case class BucketCreateOptions(name:String,ignoreExists:Boolean)
    case class BucketDeleteOptions(name:String,ignoreNotExists:Boolean)
    //
    case class KVPair(key:String,value:String)
    case class BucketPutOptions(name:String,elems:Array[KVPair])
    case class BucketRemoveOptions(name:String,keys:Array[String])
    //
    case class BSetCreateOptions(name:String,ignoreExists:Boolean)
    case class BSetDeleteOptions(name:String,ignoreNotExists:Boolean)
    //
    case class BSetElement(order:String,value:String)
    case class BSetPutOptions(name:String,elems:Array[String])
    case class BSetRemoveOptions(name:String,elems:Array[String])
    //
    case class BListCreateOptions(name:String,ignoreExists:Boolean)
    case class BListDeleteOptions(name:String,ignoreNotExists:Boolean)
    //
    case class BListElement(index:String,value:String)
    case class BListPendOptions(name:String,prepend:Boolean,elems:Array[String])
    case class BListRemoveOptions(name:String,index:Int,count:Int)
    case class BListInsertOptions(name:String,index:Int,elems:Array[String])
    case class BListUpdateOptions(name:String,index:Int,elem:String)
    //
    case class TxOperation(collection:String,collectionOp:String,elementOp:String,index:Int,count:Int,elems:Array[KVPair])
    case class Tx(readonly:Boolean,operations:Array[TxOperation])
    case class TxOperationResult(success:Boolean,err:String,data:List[KVPair])
    case class TxResult(success:Boolean,err:String,results:List[TxOperationResult])

/**
  * 
  */
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
    implicit val collectionCreateFormat: RootJsonFormat[Server.CollectionCreateOptions] = jsonFormat4(Server.CollectionCreateOptions.apply)
    implicit val collectionDeleteFormat: RootJsonFormat[Server.CollectionDeleteOptions] = jsonFormat3(Server.CollectionDeleteOptions.apply)
    implicit val collectionInfoFormat: RootJsonFormat[Server.CollectionInfo] = jsonFormat2(Server.CollectionInfo.apply)
    implicit val collectionGetRespFormat: RootJsonFormat[Server.CollectionGetResponse] = jsonFormat1(Server.CollectionGetResponse.apply)
    //
    implicit val kvPairFormat: RootJsonFormat[Server.KVPair] = jsonFormat2(Server.KVPair.apply)
    implicit val bucketCreateFormat: RootJsonFormat[Server.BucketCreateOptions] = jsonFormat2(Server.BucketCreateOptions.apply)
    implicit val bucketDeleteFormat: RootJsonFormat[Server.BucketDeleteOptions] = jsonFormat2(Server.BucketDeleteOptions.apply)
    implicit val bucketPutFormat: RootJsonFormat[Server.BucketPutOptions] = jsonFormat2(Server.BucketPutOptions.apply)
    implicit val bucketRemoveFormat: RootJsonFormat[Server.BucketRemoveOptions] = jsonFormat2(Server.BucketRemoveOptions.apply)
    //
    implicit val bsetCreateFormat: RootJsonFormat[Server.BSetCreateOptions] = jsonFormat2(Server.BSetCreateOptions.apply)
    implicit val bsetDeleteFormat: RootJsonFormat[Server.BSetDeleteOptions] = jsonFormat2(Server.BSetDeleteOptions.apply)
    implicit val bsetElementFormat: RootJsonFormat[Server.BSetElement] = jsonFormat2(Server.BSetElement.apply)
    implicit val bsetPutFormat: RootJsonFormat[Server.BSetPutOptions] = jsonFormat2(Server.BSetPutOptions.apply)
    implicit val bsetRemoveFormat: RootJsonFormat[Server.BSetRemoveOptions] = jsonFormat2(Server.BSetRemoveOptions.apply)
    //
    implicit val blistCreateFormat: RootJsonFormat[Server.BListCreateOptions] = jsonFormat2(Server.BListCreateOptions.apply)
    implicit val blistDeleteFormat: RootJsonFormat[Server.BListDeleteOptions] = jsonFormat2(Server.BListDeleteOptions.apply)
    implicit val blistElementFormat: RootJsonFormat[Server.BListElement] = jsonFormat2(Server.BListElement.apply)
    implicit val blistPendFormat: RootJsonFormat[Server.BListPendOptions] = jsonFormat3(Server.BListPendOptions.apply)
    implicit val blistRemoveFormat: RootJsonFormat[Server.BListRemoveOptions] = jsonFormat3(Server.BListRemoveOptions.apply)
    implicit val blistInsertFormat: RootJsonFormat[Server.BListInsertOptions] = jsonFormat3(Server.BListInsertOptions.apply)
    implicit val blistUpdateFormat: RootJsonFormat[Server.BListUpdateOptions] = jsonFormat3(Server.BListUpdateOptions.apply)
    //
    implicit val txOperationFormat: RootJsonFormat[Server.TxOperation] = jsonFormat6(Server.TxOperation.apply)
    implicit val txFormat: RootJsonFormat[Server.Tx] = jsonFormat2(Server.Tx.apply)
    implicit val txOperationResultFormat: RootJsonFormat[Server.TxOperationResult] = jsonFormat3(Server.TxOperationResult.apply)
    implicit val txResultFormat: RootJsonFormat[Server.TxResult] = jsonFormat3(Server.TxResult.apply)
    //
    implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()
}

/**
  * 
  *
  * @param ops
  */
class Server(val ops:ServerOptions) extends JsonSupport:
    // needed to run the route
    implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "platdb")
    // needed for the future map/flatmap in the end and future in fetchItem and saveOrder
    implicit val executionContext: ExecutionContext = system.executionContext
    // 
    import Collection._ 
    import Server._
    
    /**
      * 
      */
    def run():Unit = 
        val db = DB.open(ops.path)(using ops.conf)
        println(s"open database ${ops.path} success")
    
        val route =
            pathPrefix("v1"){
                concat (
                    (pathEnd | pathSingleSlash) {
                        get {
                            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>hello platdb</h1>"))
                        }
                    },
                    path("backup")(routeBackup(db)),
                    path("collections")(routeCollection(db)),
                    pathPrefix("buckets")(routeBucket(db)),
                    pathPrefix("bsets")(routeSet(db)),
                    pathPrefix("blists")(routeList(db)),
                    pathPrefix("txns")(routeTx(db)),

                    pathPrefix("foo")(routeTest(db)),
                    pathPrefix("bar")(routeTest2(db)),
                )
            }
        //
        val bindingFuture = Http().newServerAt("localhost", 8080).bind(route)

        println(s"Server now online. Please navigate to http://localhost:8080/v1")

        /*
        StdIn.readLine() // let it run until user presses return
        bindingFuture
            .flatMap(_.unbind()) // trigger unbinding from the port
            .onComplete(_ => 
                db.close() match
                    case Failure(e) => println(s"close db failed:${e.getMessage()}")
                    case Success(_) => println("db closed")
                system.terminate()
            ) // and shutdown when done
        */
        val waitOnFuture = Promise[Done].future 
        val shutdownHook = ShutdownHookThread{
                println("Shutdown hook is running")
                // cleanup logic
                //
                bindingFuture.flatMap(_.unbind()).onComplete(_ => println("unbind"))
                //
                println("close db...")
                db.close() match
                    case Failure(e) => println(s"close db failed:${e.getMessage()}")
                    case Success(_) => println("db closed")
                println("terminate system...")
                system.terminate()
                println(s"shutdown platdb server")
        }
        println(s"waiting...")
        Await.ready(waitOnFuture, Duration.Inf)
    
    //
    import java.text.SimpleDateFormat
    import java.util.Date 
    private def routeTest(db:DB):Route =
        pathEnd {
            get {
                var resp:Option[Try[HttpResponse]] = None
                val t:Future[Try[Unit]] = Future {
                    try
                        println(s"start work...")
                        for i <- 0 to 10 do
                            if i == 5 then
                                val hr = HttpResponse(200, entity = "OOOOOOOOOOOOOOKKKKKKKKKKKKKKK!")
                                resp = Some(Success(hr))
                                println(s"[resp] prepared the response")
                            Thread.sleep(1000)
                            val tt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
                            println(s"[work] time is ${tt}")
                        Success(None)
                    catch
                        case e:Exception => Failure(e)
                    finally 
                        println(s"[work] completed")
                }
                println("waiting httpResponse....")
                var flag = false
                while !flag do
                    resp match
                        case None => Thread.sleep(500)
                        case Some(r) => flag = true
                
                println("get response")
                resp match
                    case None => complete(StatusCodes.InternalServerError,"failed")
                    case Some(r) => 
                        r match 
                            case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            case Success(tp) => complete(tp)
            }
        }
    private def routeTest2(db:DB):Route =
        pathEnd {
            get {
                var file:Option[Try[(String,Long)]] = None
                val t:Future[Try[Unit]] = Future {
                    try
                        println(s"[work] start...")
                        val path:String =s"C:${File.separator}Users${File.separator}flxj_${File.separator}test${File.separator}platdb${File.separator}server.test"
                        // TODO1: create a file
                        val f = new File(path)
                        if !f.exists() then
                            f.createNewFile()
                        var accesser:RandomAccessFile = new RandomAccessFile(f,"rw")
                        val ch = accesser.getChannel()

                        val s1 = "aaa".getBytes
                        val size = s1.length*4
                        // TODO 2: write some content

                        val buf = ByteBuffer.allocate(64)
                        buf.put(s1)
                        buf.flip()
                        while buf.hasRemaining do
                            ch.write(buf)
                        ch.force(true)
                        buf.clear()

                        // TODO 3: (file name, file size)
                        file = Some(Success((path,size)))
                        
                        // TODO 4: write continue
                        for s <- List[String]("bbb","ccc","ddd") do
                            buf.put(s.getBytes)
                            buf.flip()
                            while buf.hasRemaining do
                                ch.write(buf)
                            buf.clear()

                        // TODO 5. stop
                        println(s"[work] write file completed")
                        ch.close()
                        Success(None)
                    catch
                        case e:Exception => 
                           file = Some(Failure(e))
                           Failure(e)
                    finally 
                        // TODO: close
                        println(s"[work] completed")
                }
                println("waiting ....")
                var flag = false
                while !flag do
                    file match
                        case None => Thread.sleep(500)
                        case Some(r) => flag = true
                //
                println("response...")
                file match
                    case None => complete(StatusCodes.InternalServerError,"failed")
                    case Some(info) =>  
                        info match
                            case Failure(e) => complete(StatusCodes.InternalServerError,"failed")
                            case Success((name,size)) =>
                                // TODO httpresponse
                                val source = FileIO.fromPath(Paths.get(name))
                                complete(HttpEntity(ContentTypes.`application/octet-stream`,size,source))     
            }
        }
    /**
      * 
      *
      * @param db
      */
    private def routeTx(db:DB):Route =
        pathEnd {
            post { // TODO timeout
                entity(as[Server.Tx]) { ops =>
                    val res:Future[Option[TxResult]] = Future {
                        var opRes = List[TxOperationResult]()
                        var r:Option[TxResult] = None 
                        if ops.operations.length != 0 then
                            try
                                // check tx request operations.
                                for op <- ops.operations do 
                                    op.collectionOp match
                                        case "" => 
                                            op.elementOp match
                                                case "" => throw new Exception("collectionOp and elementOp is null")
                                                case "get"|"slice" => None
                                                case "delete"|"put"|"append"|"insert"|"prepend" => 
                                                    if ops.readonly then
                                                        throw new Exception(s"readonly transaction not allow such element operation: ${op.elementOp}")
                                                case _ => throw new Exception(s"not support such element operation: ${op.elementOp}")
                                        case "get" => None
                                        case "create"|"delete" => 
                                            if ops.readonly then
                                                throw new Exception(s"readonly transaction not allow such collection operation: ${op.collectionOp}") 
                                        case _ => throw new Exception(s"not support such collection operation: ${op.collectionOp}") 

                                // exec tx request
                                val txExec = if ops.readonly then db.view else db.update
                                txExec(
                                    (tx:Transaction) =>
                                        given t:Transaction = tx
                                        // get all collections.
                                        t.allCollection() match
                                            case Failure(e) => throw e
                                            case Success(cols) =>
                                                val mp = Map[String,String]()
                                                for (k,v) <- cols do mp(k) = v
                                                for op <- ops.operations do
                                                    execOperation(t,op,mp) match
                                                        case Success(r) => opRes:+=r
                                                        case Failure(e) => throw e
                                ) match
                                    case Failure(e) => Some(TxResult(false,e.getMessage(),opRes))
                                    case Success(_) => Some(TxResult(true,"",opRes))
                            catch
                                case e:Exception => Some(TxResult(false,e.getMessage(),opRes))
                        else
                            Some(TxResult(false,"tx request is empty",opRes))
                    }
                    onSuccess(res){
                        case None => complete(StatusCodes.InternalServerError,"tx exec failed")
                        case Some(result) => complete(StatusCodes.OK,result)
                    }
                }  
            }
        }
    //
    private val successResult = Success(TxOperationResult(true,"",List[KVPair]()))
    /**
      * 
      *
      * @param txn
      * @param op
      * @param cols
      */
    private def execOperation(txn:Transaction,op:TxOperation,cols:Map[String,String]):Try[TxOperationResult] = 
        given tx:Transaction = txn
        if op.collection != "" then
            val tp = cols.get(op.collection) match
                case None => CollectionType.Nothing
                case Some(t) => collectionType(t)
            tp match
                case CollectionType.Bucket =>
                    op.collectionOp match
                        case "delete" =>
                            tx.deleteBucket(op.collection) match
                                case Success(_) => 
                                    cols.remove(op.collection)
                                    successResult
                                case Failure(e) => Failure(e)
                        case "create" =>
                            tx.createBucketIfNotExists(op.collection) match
                                case Success(_) => 
                                    cols(op.collection) = dataTypeName(bucketDataType)
                                    successResult
                                case Failure(e) => Failure(e)
                        case "get" => 
                            tx.openBucket(op.collection) match
                                case Failure(e) => Failure(e)
                                case Success(bk) => 
                                    val info = List[KVPair](KVPair("name",op.collection),KVPair("length",bk.length.toString))
                                    Success(TxOperationResult(true,"",info))
                        case "" => 
                            try
                                val bk = openBucket(op.collection)
                                op.elementOp match
                                    case "put" =>  
                                        val elems = for e <- op.elems yield (e.key,e.value)
                                        bk+=(elems)
                                        successResult 
                                    case "get" =>
                                        var kvs = List[KVPair]()
                                        for e <- op.elems yield 
                                            bk.get(e.key) match
                                                case Failure(e) => throw e
                                                case Success(v) => kvs:+=KVPair(e.key,v)
                                        Success(TxOperationResult(true,"",kvs))
                                    case "delete" =>
                                        val keys = for e <- op.elems yield e.key
                                        bk-=(keys)
                                        successResult
                                    case "" => Failure(new Exception(s"bucket operation is null"))
                                    case _ => Failure(new Exception(s"not support bucket operation: ${op.elementOp}"))
                            catch
                                case e:Exception => Failure(e)
                        case _ => Failure(new Exception(s"not support bucket operation: ${op.collectionOp}"))
                case CollectionType.BList => 
                    op.collectionOp match
                        case "delete" =>
                            tx.deleteList(op.collection) match
                                case Success(_) => 
                                    cols.remove(op.collection)
                                    successResult
                                case Failure(e) => Failure(e)
                        case "create" =>
                            tx.createListIfNotExists(op.collection) match
                                case Success(_) => 
                                    cols(op.collection) = dataTypeName(blistDataType)
                                    successResult
                                case Failure(e) => Failure(e)
                        case "get" => 
                            tx.openList(op.collection) match
                                case Failure(e) => Failure(e)
                                case Success(list) => 
                                    val info = List[KVPair](KVPair("name",op.collection),KVPair("length",list.length.toString))
                                    Success(TxOperationResult(true,"",info))
                        case "" => 
                            try
                                val list = openList(op.collection)
                                op.elementOp match
                                    case "put" =>
                                        for e <- op.elems do 
                                            val i = e.key.toInt
                                            list(i) = e.value
                                        successResult
                                    case "append" =>  
                                        list.append(for e <- op.elems yield e.value) match
                                            case Failure(e) => Failure(e)
                                            case Success(_) => successResult
                                    case "prepend" =>
                                        list.prepend(for e <- op.elems yield e.value) match
                                            case Failure(e) => Failure(e)
                                            case Success(_) => successResult
                                    case "insert" => 
                                        val i = op.index.toInt
                                        val elems = for e <- op.elems yield e.value
                                        list.insert(i,elems) match
                                            case Failure(e) => Failure(e)
                                            case Success(_) => successResult
                                    case "get" => 
                                        val data = (for e <- op.elems yield KVPair(e.key,list(e.key.toInt)))
                                        Success(TxOperationResult(true,"",data.toList))
                                    case "slice" =>
                                        val i = op.index.toInt
                                        val count = op.count.toInt
                                        list.slice(i,i+count) match
                                            case Failure(e) => Failure(e)
                                            case Success(s) => 
                                                var data = List[KVPair]()
                                                for (k,v) <- s.iterator do
                                                    (k,v) match
                                                        case (_,None) => None
                                                        case (None,_) => None
                                                        case (Some(kk),Some(vv)) => data:+=KVPair(kk,vv)
                                                Success(TxOperationResult(true,"",data))
                                    case "delete" =>
                                        val i = op.index.toInt
                                        val count = op.count.toInt
                                        list.remove(i,count) match
                                            case Failure(e) =>  Failure(e)
                                            case Success(_) => successResult
                                    case "" => Failure(new Exception(s"list operation is null"))
                                    case _ => Failure(new Exception(s"not support list operation: ${op.elementOp}"))
                            catch
                                case e:Exception => Failure(e)
                        case _ => Failure(new Exception(s"not support list operation: ${op.collectionOp}"))
                case CollectionType.BSet =>
                    op.collectionOp match
                        case "delete" =>
                            tx.deleteBSet(op.collection) match
                                case Success(_) => 
                                    cols.remove(op.collection)
                                    successResult
                                case Failure(e) => Failure(e)
                        case "create" =>
                            tx.createBSetIfNotExists(op.collection) match
                                case Success(_) => 
                                    cols(op.collection) = dataTypeName(bsetDataType)
                                    successResult
                                case Failure(e) => Failure(e)
                        case "get" =>
                            tx.openBSet(op.collection) match
                                case Failure(e) => Failure(e)
                                case Success(set) => 
                                    val info = List[KVPair](KVPair("name",op.collection),KVPair("length",set.length.toString))
                                    Success(TxOperationResult(true,"",info))
                        case "" => 
                            try
                                val set = openSet(op.collection)
                                op.elementOp match
                                    case "put" =>
                                        val elems = for e <- op.elems yield e.key
                                        set.add(elems) match
                                            case Failure(e) => throw e
                                            case Success(_) => successResult 
                                    case "get" =>
                                        val elems = for e <- op.elems yield 
                                            set.contains(e.key) match
                                                case Failure(e) => throw e
                                                case Success(ok) => 
                                                    if ok then
                                                        KVPair(e.key,"true")
                                                    else
                                                        KVPair(e.key,"false")
                                        Success(TxOperationResult(true,"",elems.toList))
                                    case "delete" =>
                                        val keys = for e <- op.elems yield e.key
                                        set.remove(keys) match
                                            case Failure(e) => throw e
                                            case Success(_) => successResult
                                    case "" => Failure(new Exception(s"bset operation is null"))
                                    case _ => Failure(new Exception(s"not support bset operation: ${op.elementOp}"))
                            catch
                                case e:Exception => Failure(e)
                        case _ => Failure(new Exception(s"not support bset operation: ${op.collectionOp}"))
                case CollectionType.Nothing => Failure(new Exception(s"collection ${op.collection} not exists"))
                case _ => Failure(new Exception(s"not support such collection type in transaction:${tp}"))
        else
            Success(TxOperationResult(false,"ignore",List[KVPair]()))
    /**
      * 
      *
      * @param db
      * @return
      */
    private def routeBackup(db:DB):Route = 
        concat (
            get {
                // TODO: timeout
                var backupFile:Option[(String,Long)] = None
                val copy: Future[Try[Unit]] = Future {
                    db.view(
                        (tx:Transaction) =>
                            // template file
                            try
                                val path = db.tmpDir+File.separator+s"backup-${System.currentTimeMillis()}"
                                val tmpFile = new File(path)
                                if !tmpFile.exists() then
                                    tmpFile.createNewFile()
                                //
                                backupFile = Some((path,tx.size))
                                //
                                tx.copyToFile(path) match
                                    case Failure(e) => throw e
                                    case Success(_) => None
                            catch
                                case e:Exception => throw e
                            finally
                                println(s"[debug] copy backup file completed")
                    ) match
                        case Failure(e) => Failure(e)
                        case Success(_) => Success(None)
                }
                println("waiting ....")
                var flag = false
                while !flag do
                    backupFile match
                        case None => Thread.sleep(500)
                        case Some(_) => flag = true
                //
                //onSuccess(copy) { 
                //    case Success(_) => complete(StatusCodes.OK,s"backup success")
                //    case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                //}

                println("response...")
                backupFile match
                    case None => complete(StatusCodes.InternalServerError,"failed")
                    case Some((name,size)) => 
                        val source = FileIO.fromPath(Paths.get(name)).watchTermination() {
                            case (_,result) =>
                                result.onComplete( _ =>
                                    val file = new File(name)
                                    if file.delete() then
                                        println(s"[debug] template deleted.")
                                )
                        }
                        complete(HttpEntity(ContentTypes.`application/octet-stream`,size,source))              
            }
        )
    /**
      * 
      *
      * @param db
      * @return
      */
    private def routeCollection(db:DB):Route =
        pathEnd {
            concat (
                get {
                    val res:Future[Try[Seq[(String,String)]]] = Future{
                        db.listCollection("") match
                            case Failure(e) => Failure(e)
                            case Success(s) => Success(s)
                    }
                    onSuccess(res) {
                        case Success(value) =>
                            val s = (for (k,v) <- value yield Server.CollectionInfo(k,v)).toList
                            complete(StatusCodes.OK,Server.CollectionGetResponse(s))
                        case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                    }      
                },
                post {
                    entity(as[Server.CollectionCreateOptions]) { ops =>
                        val created: Future[Try[Unit]] = Future {
                            db.createCollection(ops.name,ops.collectionType,ops.dimension,ops.ignoreExists)
                        }
                        onSuccess(created) { 
                            case Success(_) => complete(StatusCodes.OK,s"create ${ops.collectionType} ${ops.name} success\n")
                            case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                        }
                    }
                },
                delete {
                    entity(as[Server.CollectionDeleteOptions]) { ops =>
                        val deleted: Future[Try[Unit]] = Future {
                            db.deleteCollection(ops.name,ops.collectionType,ops.ignoreNotExists)
                        }
                        onSuccess(deleted) { 
                            case Success(_) => complete(StatusCodes.OK,s"delete ${ops.collectionType} ${ops.name} success\n")
                            case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                        }
                    }
                },
            )
        }
    /**
      * 
      *
      * @param db
      * @return
      */
    private def routeBucket(db:DB):Route = 
        concat(
            pathEnd{
                concat(
                    get {
                        val res:Future[Try[Seq[(String,String)]]] = Future{
                            db.listCollection(DB.collectionTypeBucket) match
                                case Failure(e) => Failure(e)
                                case Success(s) => Success(s)
                        }
                        onSuccess(res) {
                            case Success(value) =>
                                val s = (for (k,v) <- value yield Server.CollectionInfo(k,v)).toList
                                complete(StatusCodes.OK,Server.CollectionGetResponse(s))
                            case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                        }
                    },
                    post {
                        entity(as[Server.BucketCreateOptions]) { ops =>
                            val created: Future[Try[Unit]] = Future {
                                db.createCollection(ops.name,DB.collectionTypeBucket,0,ops.ignoreExists)
                            }
                            onSuccess(created) { 
                                case Success(_) => complete(StatusCodes.OK,s"create Bucket ${ops.name} success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    },
                    delete {
                        entity(as[Server.BucketDeleteOptions]) { ops =>
                            val deleted: Future[Try[Unit]] = Future {
                                db.deleteCollection(ops.name,DB.collectionTypeBucket,ops.ignoreNotExists)
                            }
                            onSuccess(deleted) { 
                                case Success(_) => complete(StatusCodes.OK,s"delete Bucket ${ops.name} success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    },
                )
            },
            path("elements"){
                concat(
                    (get & parameter("name")) { name =>
                        //
                        val res:Future[Try[List[Server.KVPair]]] = Future{
                            var list = List[Server.KVPair]()
                            db.view (
                                (tx:Transaction) =>
                                    given t:Transaction = tx
                                    val bk = openBucket(name)
                                    for (k,v) <- bk.iterator do
                                        (k,v) match
                                            case (Some(key),Some(value)) => list:+=Server.KVPair(key,value)
                                            case (Some(key),None) => None
                                            case (None,_) => throw new Exception(s"found null elements in bucket $name\n")
                            ) match
                                case Failure(e) => Failure(e)
                                case Success(_) => Success(list)
                        }
                        onSuccess(res) {
                            case Success(value) =>
                                val pairs: Source[KVPair, NotUsed] = Source{value}
                                complete(pairs)
                            case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                        }
                    },
                    post {
                        entity(as[Server.BucketPutOptions]) { ops => 
                            val add: Future[Try[Unit]] = Future {
                                val elems = for p <- ops.elems yield (p.key,p.value)
                                db.put(ops.name,elems)
                            }
                            onSuccess(add) { 
                                case Success(_) => complete(StatusCodes.OK,s"put elements to Bucket ${ops.name} success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    },
                    delete {
                        entity(as[Server.BucketRemoveOptions]) { ops => 
                            val add: Future[Try[Unit]] = Future {
                                db.delete(ops.name,true,ops.keys)
                            }
                            onSuccess(add) { 
                                case Success(_) => complete(StatusCodes.OK,s"remove elements from Bucket ${ops.name} success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    }
                )
            }
        )
    /**
      * 
      *
      * @param db
      * @return
      */
    private def routeList(db:DB):Route = 
        concat(
            pathEnd{
                concat(
                    get {
                        val res:Future[Try[Seq[(String,String)]]] = Future{
                        db.listCollection(DB.collectionTypeBList) match
                            case Failure(e) => Failure(e)
                            case Success(s) => Success(s)
                        }
                        onSuccess(res) {
                            case Success(value) =>
                                val s = (for (k,v) <- value yield Server.CollectionInfo(k,v)).toList
                                complete(StatusCodes.OK,Server.CollectionGetResponse(s))
                            case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                        }
                    },
                    post {
                        entity(as[Server.BListCreateOptions]) { ops =>
                            val created: Future[Try[Unit]] = Future {
                                db.createCollection(ops.name,DB.collectionTypeBList,0,ops.ignoreExists)
                            }
                            onSuccess(created) { 
                                case Success(_) => complete(StatusCodes.OK,s"create BList ${ops.name} success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    },
                    delete {
                        entity(as[Server.BListDeleteOptions]) { ops =>
                            val deleted: Future[Try[Unit]] = Future {
                                db.deleteCollection(ops.name,DB.collectionTypeBList,ops.ignoreNotExists)
                            }
                            onSuccess(deleted) { 
                                case Success(_) => complete(StatusCodes.OK,s"delete BList ${ops.name} success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    }
                )
            }, 
            path("elements"){
                concat (
                    (get & parameter("name")) { name =>
                        val res:Future[Try[List[Server.BListElement]]] = Future{
                            var list = List[Server.BListElement]()
                            db.view (
                                (tx:Transaction) =>
                                    given t:Transaction = tx
                                    val blist = openList(name)
                                    for (k,v) <- blist.iterator do
                                        (k,v) match
                                            case (Some(key),Some(value)) => list:+=Server.BListElement(key,value)
                                            case (Some(key),None) => None
                                            case (None,_) => throw new Exception(s"found null elements in blist $name\n")          
                            ) match
                                case Failure(e) => Failure(e)
                                case Success(_) => Success(list)
                        }
                        onSuccess(res) {
                            case Success(value) =>
                                val elems: Source[Server.BListElement, NotUsed] = Source{value}
                                complete(elems)
                            case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                        }
                    },
                    post {
                        entity(as[Server.BListPendOptions]) { ops =>
                            val pended: Future[Try[Unit]] = Future {
                                db.update(
                                    (tx:Transaction) =>
                                        given t:Transaction = tx
                                        val list = openList(ops.name)
                                        if ops.prepend then
                                            list.prepend(ops.elems) match
                                                case Success(_) => None
                                                case Failure(e) => throw e
                                        else 
                                            list.append(ops.elems) match
                                                case Success(_) => None
                                                case Failure(e) => throw e
                                )
                            }
                            onSuccess(pended) { 
                                case Success(_) => complete(StatusCodes.OK,s"add elements to BList ${ops.name} success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    },
                    put {
                        entity(as[Server.BListUpdateOptions]) { ops =>
                            val pended: Future[Try[Unit]] = Future {
                                db.update(
                                    (tx:Transaction) =>
                                        given t:Transaction = tx
                                        val list = openList(ops.name)
                                        list(ops.index) = ops.elem 
                                )
                            }
                            onSuccess(pended) { 
                                case Success(_) => complete(StatusCodes.OK,s"update BList ${ops.name} element success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    },
                    delete {
                        entity(as[Server.BListRemoveOptions]) { ops =>
                            val deleted: Future[Try[Unit]] = Future {
                                db.update(
                                    (tx:Transaction) =>
                                        given t:Transaction = tx
                                        val list = openList(ops.name)
                                        list.remove(ops.index,ops.count) match
                                            case Success(_) => None
                                            case Failure(e) => throw e
                                )
                            }
                            onSuccess(deleted) { 
                                case Success(_) => complete(StatusCodes.OK,s"delete elements from BList ${ops.name} success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    },
                )
            }
        )
    /**
      * 
      *
      * @param db
      * @return
      */
    private def routeSet(db:DB):Route = 
        concat(
            pathEnd{
                concat(
                    get{
                        val res:Future[Try[Seq[(String,String)]]] = Future{
                            db.listCollection(DB.collectionTypeBSet) match
                                case Failure(e) => Failure(e)
                                case Success(s) => Success(s)
                        }
                        onSuccess(res) {
                            case Success(value) =>
                                val s = (for (k,v) <- value yield Server.CollectionInfo(k,v)).toList
                                complete(StatusCodes.OK,Server.CollectionGetResponse(s))
                            case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                        }
                    },
                    post {
                        entity(as[Server.BSetCreateOptions]) { ops =>
                            val created: Future[Try[Unit]] = Future {
                                db.createCollection(ops.name,DB.collectionTypeBSet,0,ops.ignoreExists)
                            }
                            onSuccess(created) { 
                                case Success(_) => complete(StatusCodes.OK,s"create BSet ${ops.name} success")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    },
                    delete {
                        entity(as[Server.BSetDeleteOptions]) { ops =>
                            val deleted: Future[Try[Unit]] = Future {
                                db.deleteCollection(ops.name,DB.collectionTypeBSet,ops.ignoreNotExists)
                            }
                            onSuccess(deleted) { 
                                case Success(_) => complete(StatusCodes.OK,s"delete BSet ${ops.name} success")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    }
                )
            },
            path("elements"){
                concat(
                    (get & path("") & parameter("name")) { name =>
                        val res:Future[Try[List[Server.BSetElement]]] = Future{
                            var list = List[Server.BSetElement]()
                            db.view (
                                (tx:Transaction) =>
                                    given t:Transaction = tx
                                    val set = openSet(name)
                                    for (k,v) <- set.iterator do
                                        (k,v) match
                                            case (Some(key),Some(value)) => list:+=Server.BSetElement(key,value)
                                            case (Some(key),None) => None
                                            case (None,_) => throw new Exception(s"found null elements in BSet $name")
                            ) match
                                case Failure(e) => Failure(e)
                                case Success(_) => Success(list)
                        }
                        onSuccess(res) {
                            case Success(value) =>
                                val elems: Source[Server.BSetElement, NotUsed] = Source{value}
                                complete(elems)
                            case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                        }
                    },
                    post {
                        entity(as[Server.BSetPutOptions]) { ops =>
                            val add: Future[Try[Unit]] = Future {
                                db.update(
                                    (tx:Transaction) =>
                                        given t:Transaction = tx
                                        val set = openSet(ops.name)
                                        set.add(ops.elems) match
                                            case Failure(e) => throw e
                                            case Success(_) => None
                                )
                            }
                            onSuccess(add) { 
                                case Success(_) => complete(StatusCodes.OK,s"put elements to BSet ${ops.name} success")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    },
                    delete {
                        entity(as[Server.BSetRemoveOptions]) { ops => 
                            val deleted: Future[Try[Unit]] = Future {
                                db.update(
                                    (tx:Transaction) =>
                                        given t:Transaction = tx
                                        val set = openSet(ops.name)
                                        set.remove(ops.elems) match
                                            case Failure(e) => throw e
                                            case Success(_) => None
                                )  
                            }
                            onSuccess(deleted) { 
                                case Success(_) => complete(StatusCodes.OK,s"remove elements from BSet ${ops.name} success")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    }
                )
            }
        )
