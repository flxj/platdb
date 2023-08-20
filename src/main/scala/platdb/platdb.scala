package platdb

import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


object PlatDB:
  @main def main(args: String*) =
    println("hello,platdb")

case class Options(val timeout:Int)

class DB(val path:String,val ops:Options):
  private[platdb] var fileManager:FileManager = _ 
  private[platdb] var freelist:FreeList = _ 
  private[platdb] var blockBuffer:BlockBuffer = _ 
  private[platdb] var meta:Meta
  private var closeFlag:Boolean
  private var rTx:ArrayBuffer[Tx]
  private var rwTx:Option[Tx]
  private var rwLock:ReentrantReadWriteLock

  
  def name:String = path
  def open():Boolean 
  def close():Option[Boolean]
  def closed:Boolean
  def sync():Unit

  def begin(writable:Boolean):Option[Tx]
  def update(op:(Tx)=>Unit):Unit 
  def view(op:(Tx)=>Unit):Unit
  def snapshot(path:String):Option[Int]
  
  private[platdb] def grow(sz:Int):Boolean

  private[platdb] def removeTx(txid:Int):Unit =
    var idx = -1
    breakable(
      for (i,tx) <- rTx do
          if txid == tx.id then
              idx = i
              break()
    )
    if idx >=0 then
      rTx.remove(idx,1)
    else
      rwTx match
        case Some(tx) => 
          if tx.id == txid then
            rTx = None

