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

import scala.util.{Try,Success,Failure}
import scala.util.control.Breaks._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, LongAdder}
import java.util.concurrent.locks.ReentrantLock

/**
  * This interface is used to manage block objects, 
  * so that the Tx layer does not need to pay attention 
  * to disk read and write. 
  * In addition, this interface also provides caching 
  * for disk page objects (not yet implemented).
  */
private[platdb] trait CacheManager:
    def getPage(id:Long):Unit = None
    def putPage():Unit = None
    /**
      * Get a free block that is greater than or equal to the specified size.
      *
      * @param size
      * @return
      */
    def getIdleBlock(size:Int):Block
    /**
      * Put the block back into the cache.
      *
      * @param blockId
      */
    def putBlock(blockId:Long):Unit
    /**
      * Query a data page and return the result as a block object.
      *
      * @param pageid
      * @return
      */
    def readBlock(pageid:Long):Try[Block]
    /**
      * Write block content to disk.
      *
      * @param bk
      * @return
      */
    def writeBlock(bk:Block):Try[Boolean]
    /**
      * Write all dirty pages in the current cache to disk.
      */
    def sync():Unit
    /**
      * Close cache, clear resources.
      */
    def close():Unit


private class BlockFrame(val frameId: Long, val block: Block) extends DoubleLinkedNode[BlockFrame]:
    //@volatile var dirty: Boolean = false
    //private[platdb] def reset(): Unit = dirty = false

    private val pinCount = new AtomicInteger(0)

    def pin(): Unit = pinCount.incrementAndGet()

    def unpin(): Unit =
        val v = pinCount.decrementAndGet()
        if v < 0 then
            throw new Exception(s"unpin called more than pin on frame $frameId")

    def isPinned: Boolean = pinCount.get() > 0

/*
private[platdb] case class CacheStats(
    hits: Long,
    misses: Long,
    evictions: Long,
    writes: Long,
    cachedPages: Int,
    poolSize: Int
):
    def hitRate: Double =
        val total = hits + misses
        if total == 0 then 0.0 else hits.toDouble / total

    override def toString: String =
        f"BufferPool[hits=$hits, misses=$misses, " +
        f"hitRate=${hitRate * 100}%.2f%%, evictions=$evictions, " +
        f"writes=$writes, cached=$cachedPages/$poolSize]"
*/

private[platdb] class BlockBuffer(val poolSize: Int,val fm:FileManager) extends CacheManager:
    require(poolSize > 0, "poolSize must be positive")
    
    private val idleSize = 32
    private val idleLock:ReentrantLock = new ReentrantLock()
    private val idle = new DoubleLinkedList[BlockFrame]()

    private val index = new ConcurrentHashMap[Long, BlockFrame]()
    private val lruLock = new ReentrantLock()
    private val lru = new DoubleLinkedList[BlockFrame]()
  
    private def full:Boolean = index.size() >= poolSize
    
    private def drop(bf:BlockFrame):Unit=
        if idle.length >= idleSize then
            return None
        idleLock.lock()
        try
            if idle.length < idleSize then
                idle.pushHead(bf)
        finally
            idleLock.unlock()
    
    def getIdleBlock(size:Int):Block =
        idleLock.lock()
        try 
            var bk:Block = null 
            breakable(
                for b <- idle.iterator do
                    if b.block.capacity >= size then
                        bk = b.block
                        idle.remove(b)
                        break()
            )
            if bk == null then
                bk = new Block(size)
            bk.reset()
            bk
        finally
            idleLock.unlock()
    
    def putBlock(id:Long):Unit = 
        lruLock.lock()
        try 
            val bf = index.get(id)
            if bf != null then
                if bf.isPinned then
                    bf.unpin()
                lru.moveToTail(bf)
        finally
            lruLock.unlock()
    
    def readBlock(pgid:Long):Try[Block] = 
        lruLock.lock()
        try 
            var block:Block = null
            var cached:Boolean = false
            // query cache.
            val bf = index.get(pgid) 
            if bf != null then
                bf.pin()
                cached = true
                block = bf.block
            else
                val (hd,data) = fm.readBlock(pgid) 
                val bk = getIdleBlock(hd.size)  // get a block from idle.
                bk.header = hd 
                bk.append(data)
                block = bk
            if block == null then
                Failure(new Exception(s"not found block for paid $pgid"))
            else
                if !cached then
                    var ok:Boolean = false
                    if full then
                        // cache already full, so try to select a element to eliminate.
                        breakable(
                            for bf <- lru.iterator do 
                                if !bf.isPinned then
                                    lru.remove(bf)
                                    index.remove(bf.frameId)
                                    drop(bf)
                                    ok = true
                                    break()
                        )
                    else
                        // cache not full, so cache the block directly.
                        ok = true
                    //
                    if ok then
                        val bf = new BlockFrame(block.id,block)
                        bf.pin()
                        lru.pushTail(bf)
                        index.put(block.id,bf)
                Success(block)
        catch
            case e:Exception => Failure(e)
        finally
            lruLock.unlock()
    
    def writeBlock(bk:Block):Try[Boolean] = 
        var writed:Boolean = false 
        try 
            fm.writeBlock(bk)
            lruLock.lock()
            writed = true
            if !full then
                val bf = new BlockFrame(bk.id,bk)
                lru.pushTail(bf)
                index.put(bk.id,bf)
            else
                if !index.contains(bk.id) then
                    var ok:Boolean = false 
                    breakable(
                        for bf <- lru.iterator do 
                            if !bf.isPinned then
                                lru.remove(bf)
                                index.remove(bf.frameId)
                                drop(bf)
                                ok = true
                                break()
                    )
                    if ok then
                        val bf = new BlockFrame(bk.id,bk)
                        lru.pushTail(bf)
                        index.put(bk.id,bf)
            Success(writed)
        catch
            case e:Exception => return Failure(e)
        finally
            if writed then
                lruLock.unlock()
    //
    def sync():Unit = None
    //
    def close():Unit = 
        lruLock.lock()
        try
            idle.clear()
            index.clear()
            lru.clear()
        finally
            lruLock.unlock()
