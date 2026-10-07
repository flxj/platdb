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
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayDeque
import java.nio.channels.OverlappingFileLockException
import java.io.FileInputStream
import java.io.FileOutputStream
import scala.util.control.NonFatal

private[platdb] trait FileManager:
    /**
      * File size (in bytes)
      *
      * @return
      */
    def size:Long 
    /**
      * Before reading or writing this file, 
      * it is necessary to open it first
      *
      * @param timeoutMs
      * @param retryIntervalMs
      */
    def open(timeoutMs:Long)(using retryIntervalMs:Long = 100L):Unit 
    /**
      * Close file
      */
    def close():Unit
    /**
      * Synchronize the contents of the system buffer 
      * corresponding to the file to disk
      */
    def sync():Unit 
    /**
      * Expand files to a specified size
      *
      * @param sz
      */
    def grow(sz:Long):Unit
    /**
      * Read the file content starting from the specified location
      *
      * @param id
      * @param size
      * @return
      */
    def readAt(id:Long,size:Int):Array[Byte]
    /**
      * Read the content of the block based on the block_id
      *
      * @param bid
      * @return
      */
    def readBlock(bid:Long):(BlockHeader,Array[Byte])
    /**
      * Write the content of the Block to a file
      *
      * @param bk
      * @return
      */
    def writeBlock(bk:Block):Boolean
    /**
      * Copy file content
      *
      * @param dst
      * @param srcOffset
      * @param sz
      * @param dstOffset
      * @return
      */
    def copyToFile(dst:File,srcOffset:Long,sz:Long,dstOffset:Long):Try[Long]

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
