/*
 * Copyright (C) 2023 flxj(https://github.com/flxj)
 *
 * All Rights Reserved.
 *
 * Use of this source code is governed by an Apache-style
 * license that can be found in the LICENSE file.
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
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.control.Breaks._
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayDeque
import java.nio.channels.OverlappingFileLockException
import java.io.FileInputStream
import java.io.FileOutputStream

private[platdb] trait Persistence:
    /** return object bytes size. */
    def size():Int
    /**
      * write object content to block.
      *
      * @param block
      * @return size of writed into
      */
    def writeTo(block:Block):Int

// block flag.
private[platdb] val metaType:Byte = 1 
private[platdb] val branchType:Byte = 2
private[platdb] val leafType:Byte = 3
private[platdb] val freelistType:Byte = 4

// node element type
private[platdb] val bucketType:Byte = 5
private[platdb] val regionType:Byte = 6

/**
  * 
  *
  * @param pgid
  * @param flag
  * @param count
  * @param overflow
  * @param size
  */
private[platdb] class BlockHeader(var pgid:Long,var flag:Byte,var count:Int,var overflow:Int,var size:Int):
    def getBytes():Array[Byte] =
        var buf:ByteBuffer = ByteBuffer.allocate(BlockHeader.size)
        buf.putLong(pgid)
        buf.putInt(count)
        buf.putInt(overflow)
        buf.putInt(size)
        buf.put(flag)
        buf.array()

private[platdb] object BlockHeader:
    val size = 21
    def apply(bs:Array[Byte]):Option[BlockHeader] =
        if bs.length != size then 
            None 
        else
            val arr = for i <- 0 to 4 yield
                (bs(4*i) & 0xff) << 24 | (bs(4*i+1) & 0xff) << 16 | (bs(4*i+2) & 0xff) << 8 | (bs(4*i+3) & 0xff)
            val id = (arr(0) & 0x00000000ffffffffL) << 32 | (arr(1) & 0x00000000ffffffffL)
            Some(new BlockHeader(id,bs(size-1),arr(2),arr(3),arr(4)))

/**
  * 
  *
  * @param cap
  */
private[platdb] class Block(val cap:Int):
    var header:BlockHeader = new BlockHeader(-1L,0,0,0,0)
    //
    private var data:Array[Byte] = new Array[Byte](cap)
    // index of the data tail.
    private var idx:Int = 0 
    
    def id:Long = header.pgid
    // the actual length of data that has been used.
    def size:Int = idx
    // total capacity.
    def capacity:Int = data.length
    // block type.
    def btype:Int = header.flag 
    //
    def setid(id:Long):Unit = header.pgid = id
    //
    def reset():Unit = idx = 0
    //
    def write(offset:Int,d:Array[Byte]):Unit =
        if offset<0 || d.length == 0 then 
            return None
        if offset+d.length > capacity then 
            data++=new Array[Byte](offset+d.length-capacity)
        val n = d.copyToArray(data,offset)
        if n!=d.length then
            throw new Exception("write data to block failed")
        if n+offset > idx then
            idx = n+offset
    // write data to tail.
    def append(d:Array[Byte]):Unit = write(idx,d)
    // all data.
    def all:Array[Byte] = data
    // user data.
    def getBytes():Option[Array[Byte]] = 
        if header.size >= BlockHeader.size then
            return Some(data.slice(BlockHeader.size,header.size))
        else if idx>=BlockHeader.size then
            return Some(data.slice(BlockHeader.size,idx))
        None
    // all data except header.
    def tail:Option[Array[Byte]] = 
        if capacity > BlockHeader.size then Some(data.slice(BlockHeader.size,data.length)) else None 

/**
  * 
  *
  * @param maxsize
  * @param fm
  */
private[platdb] class BlockBuffer(val maxsize:Int,var fm:FileManager):
    // Save some useless blocks that have been kicked out of the cache queue to speed up the creation of block structures.
    val poolsize:Int = 16
    var idleLock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    var idle:ArrayBuffer[Block] = new ArrayBuffer[Block]() // TODO: 加入一些统计，对get频率高的size多缓存几个
    
    // Records the blocks that are currently in use and maintains a reference count of them.
    var lock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    var count:Int = 0
    var blocks:Map[Long,Block] = Map[Long,Block]()
    var pinned:Map[Long,Int] = Map[Long,Int]() 
    
    // LRU
    var link:ArrayDeque[Long] = new ArrayDeque[Long]()

    private def full:Boolean = count>=maxsize
    /**
      * 
      *
      * @param bk
      */
    private def drop(bk:Block):Unit=
        if idle.length < poolsize then
            try 
                idleLock.writeLock().lock()
                idle+=bk
            finally
                idleLock.writeLock().unlock()
    /**
      * 
      *
      * @param size
      * @return
      */
    def get(size:Int):Block =
        try 
            idleLock.writeLock().lock()
            var idx:Int = -1
            breakable(
                for i <- 0 until idle.length do
                    if idle(i).capacity >= size then
                        idx = i
                        break()
            )
            var bk:Block = null
            if idx >= 0 then
                bk = idle.remove(idx)
            else
                bk = new Block(size)
            bk.reset()
            bk
        finally
            idleLock.writeLock().unlock()
    /**
      * 
      *
      * @param id
      */
    def revert(id:Long):Unit = 
        try 
            lock.writeLock().lock()

            val n = pinned.getOrElse(id,0)
            if n>1 then
                pinned(id) = n-1
            else
                pinned.remove(id)
        finally
            lock.writeLock().unlock()
    /**
      * 
      *
      * @param pgid
      * @return
      */
    def read(pgid:Long):Try[Block] = 
        try 
            lock.writeLock().lock()
           
            var block:Option[Block] = None
            var cached:Boolean = false
            // query cache.
            blocks.get(pgid) match
                case Some(bk) => 
                    pinned(pgid)=pinned.getOrElse(pgid,0)+1
                    cached = true
                    block = Some(bk)
                case None => 
                    fm.read(pgid) match
                        case (None,_) => 
                            throw new Exception(s"not found block header for pgid ${pgid}")
                        case (Some(hd),None) => 
                            throw new Exception(s"not found block data for pgid ${pgid}")
                        case (Some(hd),Some(data)) =>
                            var bk = get(hd.size)  // get a block from idle.
                            bk.header = hd 
                            bk.append(data)
                            block = Some(bk)
            block match
                case None => Failure(new Exception(s"not found block for paid $pgid"))
                case Some(bk) =>
                    var idx:Int = -1 // try to remove the element link(idx) from link.
                    var ignore:Boolean = false
                    if !cached then
                        // cache not full, so cache the block directly.
                        if !full then
                            blocks(bk.id) = bk
                            count+=1
                            pinned(bk.id) = pinned.getOrElse(bk.id,0)+1
                            cached = true
                        else
                            // cache already full, so try to select a element to eliminate.
                            breakable(
                                for i <- Range(link.length-1,-1,-1) do
                                    if !pinned.contains(link(i)) then
                                        idx = i
                                        break()
                            )
                            if idx < 0 then // cache is busy,so we just ignore current block.
                                ignore = true
                    else
                        // the block has cached, so just move it to head of link.
                        breakable(
                            for i <- Range(0,link.length,1) do
                                if link(i) == bk.id then
                                    idx = i
                                    break()
                        )
                    // update lru queue.
                    if idx >= 0 then
                        val id = link(idx)
                        if !cached then
                            // remove the selected block from blocks, and throw it to idle list.
                            blocks.remove(id) match
                                case None => None
                                case Some(blk) => drop(blk)
                            blocks(bk.id) = bk
                        
                        link.remove(idx)
            
                    if !ignore then
                        link.prepend(bk.id)

                    Success(bk)
        catch
            case e:Exception => Failure(e)
        finally
            lock.writeLock().unlock()
    /**
      * 
      *
      * @param bk
      * @return
      */
    def write(bk:Block):Try[Boolean] = 
        var writed:Boolean = false 
        try 
            fm.write(bk)
            lock.writeLock().lock()
            writed = true

            if !full then
                if !blocks.contains(bk.id) then
                    blocks(bk.id) = bk
                    count+=1
            else
                None // TODO 
            Success(writed)
        catch
            case e:Exception => return Failure(e)
        finally
            if writed then
                lock.writeLock().unlock()
        
    // TODO: sync将所有脏block写入文件？
    def sync():Unit = None
    def close():Unit = 
        link.clear()
        pinned.clear()
        blocks.clear()
        idle.clear()

// 
private[platdb] class FileManager(val path:String,val readonly:Boolean):
    var opend:Boolean = false
    var lockpath:String = ""
    var lockfile:File = null
    var lock:Option[FileLock] = None

    var file:File = null
    var writer:Option[RandomAccessFile] = None // writer
    
    /**
      * file size.
      *
      * @return
      */
    def size:Long = 
        if !opend then 
            throw new Exception(s"file ${path} not open")
        file.length()
    
    /**
      * open db file.
      *
      * @param timeout
      */
    def open(timeout:Int):Unit =
        if opend then return None 
        val i = path.lastIndexOf(File.separator)
        if i>=0 then
            lockpath = path.substring(0,i) +File.separator+"db.lock"
        else
            throw new Exception(s"illegal db file path ${path}")
        
        var mode:String = "rw"
        if readonly then
            mode = "r"
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
                        Thread.sleep(50)
            catch
                case e:OverlappingFileLockException =>
                    val now = new Date()
                    if  now.getTime() - start.getTime() > timeout then
                        throw new Exception(s"open database timeout:${timeout}ms")
                    else 
                        Thread.sleep(50)
                case e:Exception => throw e
    /**
      * close file and release resource.
      */
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
        var w:RandomAccessFile = null
        writer match
            case Some(wr) => w = wr
            case None => 
                w = new RandomAccessFile(file,"rw")
                writer = Some(w)
        var channel = w.getChannel()
        channel.truncate(sz)

    /**
      * read bytes at file offset.
      *
      * @param id
      * @param size
      * @return
      */
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
            if reader.read(data,0,size)!= size then
                throw new Exception(s"read size is unexpected,except $size bytes")
            data
        catch
            case e:Exception => throw e
        finally
            if reader!=null then
                reader.close()
    /**
      * read data from file.
      *
      * @param bid
      * @return
      */
    def read(bid:Long):(Option[BlockHeader],Option[Array[Byte]]) = 
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
                    (Some(hd),Some(data)) 
        finally
            if reader!=null then
                reader.close()
    /**
      * write block to file.
      *
      * @param bk
      * @return
      */ 
    def write(bk:Block):Boolean = 
        if !opend then
            throw new Exception("db file closed")
        if readonly then
            throw new Exception("readonly mode not allow write.")
        if bk.size == 0 then 
            return true
        if bk.id < 0 then
            throw new Exception(s"block type error: block id is ${bk.id}")
        if bk.id <=1 && bk.btype != metaType then // TODO remove this check to tx
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
