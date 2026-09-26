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

import java.nio.ByteBuffer
import java.io.File
import java.io.IOError
import java.io.RandomAccessFile
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try,Success,Failure}
import java.nio.channels.FileLock
import java.nio.channels.FileChannel
import java.util.Timer
import java.util.Date
import java.util.concurrent.locks.ReentrantLock
import scala.util.control.Breaks._
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayDeque
import java.nio.channels.OverlappingFileLockException
import java.io.FileInputStream
import java.io.FileOutputStream
import scala.util.control.NonFatal

private[platdb] trait FileManager:
    def size:Long 
    def open(timeoutMs:Long)(using retryIntervalMs:Long = 100L):Unit 
    def close():Unit
    def sync():Unit 
    def grow(sz:Long):Unit
    def readAt(id:Long,size:Int):Array[Byte]
    def readBlock(bid:Long):(BlockHeader,Array[Byte])
    def writeBlock(bk:Block):Boolean
    def copyToFile(dst:File,srcOffset:Long,sz:Long,dstOffset:Long):Try[Long]

/*
private[platdb] class FileMgr(val path:String,val readonly:Boolean) extends FileManager:
    var opend:Boolean = false
    var lockpath:String = ""
    var lockfile:File = null
    var lock:Option[FileLock] = None

    var file:File = null
    var writer:Option[RandomAccessFile] = None // writer

    def sync():Unit = 
        if !opend then 
            throw new Exception(s"file ${path} not open")
        if readonly then
            throw new Exception("readonly mode not allow sync file.")
        val w:RandomAccessFile = writer match
            case Some(wr) => wr
            case None => 
                val ra = new RandomAccessFile(file,"rw")
                writer = Some(ra)
                ra
        var channel = w.getChannel()
        channel.force(true)
    def size:Long = 
        if !opend then 
            throw new Exception(s"file ${path} not open")
        file.length()
    def open(timeout:Long)(using interval:Long=200L):Unit =
        if opend then return None 
        val i = path.lastIndexOf(File.separator)
        if i >= 0 then
            lockpath = path.substring(0,i) + File.separator + "db.lock"
        else
            throw new Exception(s"illegal db file path ${path}")
        
        val mode:String = if  readonly then "r" else "rw"
        lockfile = new File(lockpath)
        if !lockfile.exists() then
            lockfile.createNewFile()
        
        var accesser:RandomAccessFile = null 
        val start = new Date()
        while !opend do
            try 
                accesser = new RandomAccessFile(lockfile,mode)
                var channel = accesser.getChannel()
                val lk = channel.tryLock(0L,Long.MaxValue,readonly)
                if lk != null then
                    file = new File(path)
                    if !file.exists() then
                        if readonly then
                            lk.release()
                            throw new Exception(s"db file not exists ${path}")
                        file.createNewFile()
                    lock = Some(lk)
                    opend = true
                    if !readonly then
                        writer = Some(new RandomAccessFile(file,mode))
                else
                    val now = new Date()
                    if  now.getTime() - start.getTime() > timeout then
                        throw new Exception(s"open database timeout:${timeout}ms")
                    else 
                        Thread.sleep(interval)
            catch
                case e:OverlappingFileLockException =>
                    val now = new Date()
                    if  now.getTime() - start.getTime() > timeout then
                        throw new Exception(s"open database timeout:${timeout}ms")
                    else 
                        Thread.sleep(interval)
                case e:Exception => throw e
    def close():Unit =
        if !opend then
            return None
        lock match
            case Some(lk) => lk.release()
            case None => None 
        writer match
            case Some(w) => w.close()
            case None => None
        lock = None
        writer = None
        opend = false
    def grow(sz:Long):Unit = 
        if readonly then
            throw new Exception("readonly mode not allow grow file.")
        if sz <= size then 
            return None
        var w:RandomAccessFile = null
        writer match
            case Some(wr) => w = wr
            case None => 
                w = new RandomAccessFile(file,"rw")
                writer = Some(w)
        var channel = w.getChannel()
        channel.truncate(sz)
    def readAt(id:Long,size:Int):Array[Byte] =
        if !opend then
            throw new Exception("db file closed")
        if id<0 then 
            throw new Exception(s"illegal page id ${id}")
        var reader:RandomAccessFile = null
        try 
            reader = new RandomAccessFile(file,"r")
            reader.seek(id*DB.pageSize)
            var data = new Array[Byte](size)
            if reader.read(data,0,size) != size then
                throw new Exception(s"read size is unexpected,except $size bytes")
            data
        catch
            case e:Exception => throw e
        finally
            if reader!=null then
                reader.close()
    def readBlock(bid:Long):(BlockHeader,Array[Byte]) = 
        if !opend then
            throw new Exception("db file closed")
        if bid<0 then 
            throw new Exception(s"illegal block id ${bid}")
        val offset = bid*DB.pageSize
        var reader:RandomAccessFile = null
        try
            reader = new RandomAccessFile(file,"r")
            // 1. use pgid to seek block header location in file
            reader.seek(offset)
            // 2. read the block header content
            var d = new Array[Byte](BlockHeader.size)
            if reader.read(d,0,BlockHeader.size)!= BlockHeader.size then
                throw new Exception(s"read block header ${bid} error")
            
            BlockHeader(d) match
                case None => throw new Exception(s"parse block header ${bid} error")
                case Some(hd) =>
                    // 3. read data.
                    val sz = hd.size
                    var data = new Array[Byte](sz)
                    reader.seek(offset)
                    if reader.read(data,0,sz) != sz then
                        throw new Exception(s"read block data size unexpected,expect $sz bytes.")
                    (hd,data) 
        finally
            if reader != null then
                reader.close()
    def writeBlock(bk:Block):Boolean = 
        if !opend then
            throw new Exception("db file closed")
        if readonly then
            throw new Exception("readonly mode not allow write.")
        if bk.size == 0 then 
            return true
        if bk.id < 0 then
            throw new Exception(s"block type error: block id is ${bk.id},flag is ${bk.btype}")
        if bk.id <= 1 && bk.btype != Block.typeMeta then // TODO remove this check to tx
            throw new Exception(s"block type error: block id is ${bk.id} type is ${bk.btype}")
        
        var w:RandomAccessFile = null
        writer match
            case Some(wr) => w = wr 
            case None =>
                w = new RandomAccessFile(file,"rw")
                writer = Some(w)

        w.seek(bk.id*DB.pageSize)
        w.write(bk.all)
        true
    
    def copyToFile(dst:File,srcOffset:Long,sz:Long,dstOffset:Long):Try[Long] = 
        var src:FileChannel = null
        var dest:FileChannel = null
        try
            src = new FileInputStream(file).getChannel()
            dest = new FileOutputStream(dst).getChannel()
            src.position(srcOffset)
            dest.position(dstOffset)
            val n = dest.transferFrom(src,0L,sz)
            if n!=sz then
                throw new Exception(s"copy data length unexpected: expect $sz actual $n")
            Success(n)
        catch
            case e:Exception => Failure(e)
        finally
            if src != null then
                src.close()
            if dest != null then
                dest.close()
*/

private[platdb] class FileMgr(val path:String,val readonly:Boolean) extends FileManager:
    private var opend:Boolean = false
    private var file:File = null
    private var fileLock:Option[FileLock] = None
    private var fileCh:Option[FileChannel] = None
    private var raf:Option[RandomAccessFile] = None

    def sync():Unit = 
        if !opend then 
            throw new Exception(s"file ${path} not open")
        if readonly then
            throw new Exception("readonly mode not allow sync file.")
        val w:RandomAccessFile = raf match
            case Some(wr) => wr
            case None => 
                val ra = new RandomAccessFile(file,"rw")
                raf = Some(ra)
                ra
        var channel = w.getChannel()
        channel.force(true)
    /**
      * file size.
      *
      * @return
      */
    def size:Long = 
        if !opend || file == null then 
            throw new Exception(s"file ${path} not open")
        file.length()
    //
    def open(timeoutMs:Long)(using retryIntervalMs: Long = 100L):Unit = 
        if opend then 
            throw new Exception("file manager already opened")
        
        val mode:String = if readonly then "r" else "rw"
        val f = new RandomAccessFile(path, mode)
        try 
            val channel: FileChannel = f.getChannel
            val startTime = System.currentTimeMillis()
            var lock:FileLock = null 
            while lock == null do 
                try 
                    lock = channel.tryLock()
                catch
                    case _: OverlappingFileLockException => lock = null 
                    case NonFatal(e) => 
                        f.close()
                        throw e 
                if lock == null then 
                    if (System.currentTimeMillis() - startTime >= timeoutMs) then 
                        f.close()
                        throw new Exception(s"open file timeout:${timeoutMs}ms")
                    Thread.sleep(retryIntervalMs)
            file = new File(path)
            if !file.exists() then
                if readonly then
                    lock.release()
                    throw new Exception(s"file not exists ${path}")
                file.createNewFile()
            fileCh = Some(channel)
            fileLock = Some(lock)
            raf = Some(f)
            opend = true
        catch
            case NonFatal(e) => 
                try 
                    f.close()
                catch
                    case NonFatal(_) => None 
                throw e  
    /**
      * close file and release resource.
      */
    def close():Unit =
        if !opend then
            return None
        fileLock match
            case Some(lk) => lk.release()
            case None => None 
        fileCh match
            case Some(ch) => ch.close()
            case None => None
        raf match
            case Some(f) => f.close()
            case None => None 
        file = null
        fileLock = None
        fileCh = None
        raf = None
        opend = false
    /**
      * grow file to size.
      *
      * @param sz
      */
    def grow(sz:Long):Unit = 
        if readonly then
            throw new Exception("readonly mode not allow grow file.")
        if sz <= size then 
            return None
        
        fileCh match
            case Some(ch) => ch.truncate(sz)
            case None => None 
    /**
      * read bytes at file offset.
      *
      * @param id
      * @param size
      * @return
      */
    def readAt(id:Long,size:Int):Array[Byte] =
        if !opend then
            throw new Exception("file manager closed")
        if id<0 then 
            throw new Exception(s"illegal page id ${id}")

        raf match
            case None => throw new Exception("file already closed")
            case Some(reader) => 
                reader.seek(id*DB.pageSize)
                val data = new Array[Byte](size)
                if reader.read(data,0,size) != size then
                    throw new Exception(s"read size is unexpected,except $size bytes")
                data
    /**
      * read block data from file.
      *
      * @param bid
      * @return
      */
    def readBlock(bid:Long):(BlockHeader,Array[Byte]) = 
        if !opend then
            throw new Exception("file manager closed")
        if bid < 0 then 
            throw new Exception(s"illegal block id ${bid}")
        val offset = bid*DB.pageSize
        raf match
            case Some(reader) => 
                // 1. use pgid to seek block header location in file
                reader.seek(offset)
                // 2. read the block header content
                var d = new Array[Byte](BlockHeader.size)
                if reader.read(d,0,BlockHeader.size)!= BlockHeader.size then
                    throw new Exception(s"read block header ${bid} error")
                BlockHeader(d) match
                    case None => throw new Exception(s"parse block header ${bid} error")
                    case Some(hd) =>
                        // 3. read data.
                        val sz = hd.size
                        var data = new Array[Byte](sz)
                        reader.seek(offset)
                        if reader.read(data,0,sz) != sz then
                            throw new Exception(s"read block data size unexpected,expect $sz bytes.")
                        (hd,data) 
            case None => throw new Exception("file already closed")
    /**
      * write block to file.
      *
      * @param bk
      * @return
      */ 
    def writeBlock(bk:Block):Boolean = 
        if !opend then
            throw new Exception("file manager closed")
        if readonly then
            throw new Exception("readonly mode not allow write.")
        if bk.size == 0 then 
            return true
        if bk.id < 0 then
            throw new Exception(s"block type error: block id is ${bk.id},flag is ${bk.btype}")
        if bk.id <= 1 && bk.btype != Block.typeMeta then // TODO remove this check to tx
            throw new Exception(s"block type error: block id is ${bk.id} type is ${bk.btype}")
        
        raf match
            case Some(w) => 
                w.seek(bk.id*DB.pageSize)
                w.write(bk.all)
                true
            case None => throw new Exception("file already closed")
    /**
      * 
      *
      * @param dst
      * @param srcOffset
      * @param sz
      * @param dstOffset
      * @return
      */
    def copyToFile(dst:File,srcOffset:Long,sz:Long,dstOffset:Long):Try[Long] = 
        var src:FileChannel = null
        var dest:FileChannel = null
        try
            src = new FileInputStream(file).getChannel()
            dest = new FileOutputStream(dst).getChannel()
            src.position(srcOffset)
            dest.position(dstOffset)
            val n = dest.transferFrom(src,0L,sz)
            if n != sz then
                throw new Exception(s"copy data length unexpected: expect $sz actual $n")
            Success(n)
        catch
            case e:Exception => Failure(e)
        finally
            if src != null then src.close()
            if dest != null then dest.close()
