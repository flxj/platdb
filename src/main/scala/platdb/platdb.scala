package platdb

import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream


object PlatDB:
  @main def main(args: String*) =
    println("hello,platdb")

class Options:
  val a:String 

class Info:
  val b:String

class DB:
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




