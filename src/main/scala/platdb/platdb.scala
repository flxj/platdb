package platdb

import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.ArrayBuffer



object PlatDB:
  @main def main(args: String*) =
    println("hello,platdb")

class Options:
  val a:String 

class Info:
  val b:String

class DB:
  private[platdb] var filemanager:FileManager = _ 
  private[platdb] var freelist:FreeList = _ 
  private[platdb] var blockpool:BlockPool = _ 
  private[platdb] var rTx:ArrayBuffer[Tx]
  private[platdb] var rwTx:Option[Tx]
  private[platdb] var rwLock:ReentrantReadWriteLock
  
  def name:String 
  def open(path:String,ops:Options):Option[Boolean]
  def close():Option[Boolean]
  def closed:Boolean
  def sync():Unit
  def info:Info

  def begin(writable:Boolean):Option[Tx]
  def update(op:(Tx)=>Unit):Unit
  def view(op:(Tx)=>Unit):Unit
  def snapshot(path:String):Option[Int]

  private[platdb] def removeTx(id:Int):Unit




