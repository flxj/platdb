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
import scala.collection.mutable.{ArrayBuffer,Map}
import scala.util.control.Breaks._
import scala.util.{Try,Success,Failure}

/**
  * 
  */
private[platdb] object Meta:
    val elementSize = 56
    val checkSumSize = 4
    def size:Int = BlockHeader.size+elementSize
    //
    def apply(data:Array[Byte]):Try[Meta] =
        if data.length < BlockHeader.size+ elementSize then
            Failure(throw new Exception("illegal meta data"))
        else
            BlockHeader(data.slice(0,BlockHeader.size)) match
                case None => Failure(throw new Exception("parse block header data failed"))
                case Some(hd) =>
                    var bk = new Block(data.length)
                    bk.header = hd
                    bk.write(0,data)
                    read(bk)
    // convert block data to meta.
    def read(bk:Block):Try[Meta] =
        if bk.header.flag != Block.typeMeta then 
            Failure(new Exception(s"block type ${bk.header.flag} is not meta type"))
        else
            bk.getBytes() match
                case None => Failure(new Exception("block data is empty")) 
                case Some(data) => 
                    var meta = new Meta(bk.id)
                    if data.length != elementSize then
                        return Failure(new Exception(s"block data length is not equel ${elementSize}"))
                    val arr = for i <- 0 to 5 yield
                        val a = (data(8*i) & 0xff) << 24 | (data(8*i+1) & 0xff) << 16 | (data(8*i+2) & 0xff) << 8 | (data(8*i+3) & 0xff)
                        val b = (data(8*i+4) & 0xff) << 24 | (data(8*i+5) & 0xff) << 16 | (data(8*i+6) & 0xff) << 8 | (data(8*i+7) & 0xff)
                        (a & 0x00000000ffffffffL) << 32 | (b & 0x00000000ffffffffL)
                    val sz =  (data(48) & 0xff) << 24 | (data(49) & 0xff) << 16 | (data(50) & 0xff) << 8 | (data(51) & 0xff)
                    val chk = (data(52) & 0xff) << 24 | (data(53) & 0xff) << 16 | (data(54) & 0xff) << 8 | (data(55) & 0xff)
                    meta.pageId = arr(0)
                    meta.freelistId = arr(1)
                    meta.txid = arr(2)
                    meta.root = new BucketValue(arr(3),arr(4),arr(5),Collection.typeBucket)
                    meta.pageSize = sz
                    meta.checkSum = chk
                    Success(meta)

/**
  * database meta info.
  *
  * @param id
  */
private[platdb] class Meta(val id:Long) extends Persistence:
    var pageSize:Int = 0
    var flag:Byte = Block.typeMeta
    var freelistId:Long = -1
    var pageId:Long = -1
    var txid:Long = -1
    var root:BucketValue = null
    var checkSum:Int = 0
    /**
      * 
      *
      * @return
      */
    override def clone:Meta =
        var m = new Meta(id)
        m.pageSize = pageSize
        m.flag = flag
        m.freelistId = freelistId
        m.pageId = pageId
        m.txid = txid
        m.checkSum = checkSum
        m.root = new BucketValue(root.root,root.count,root.sequence,Collection.typeBucket)
        m
    def size():Int = Meta.size
    def writeTo(bk:Block):Int =
        bk.header.flag = flag
        bk.header.overflow = 0
        bk.header.count = 1
        bk.header.size = size() 
        bk.append(bk.header.getBytes())
        bk.append(getBytes())
        size()

    /**
      * 
      *
      * @return
      */
    def getBytes():Array[Byte] =
        var buf:ByteBuffer = ByteBuffer.allocate(Meta.elementSize)
        buf.putLong(pageId)
        buf.putLong(freelistId)
        buf.putLong(txid)
        buf.putLong(root.root)
        buf.putLong(root.count)
        buf.putLong(root.sequence)
        buf.putInt(pageSize)
        buf.putInt(checkSum)
        buf.array()
    //
    def getMetaBytes():Array[Byte] = getBytes().take(Meta.elementSize-Meta.checkSumSize)
    //
    def writeHeader(bk:Block):Unit =
        bk.header.flag = flag
        bk.header.overflow = 0
        bk.header.count = 1
        bk.header.size = size()
/**
 * 
 *
 * @return
 */
private val MOD_ADLER = 65521
private def getCheckSum32(data:Array[Byte]):Int = 
    var a = 1
    var b = 0
    for d <- data do
        a = (d + a) % MOD_ADLER
        b = (b + a) % MOD_ADLER
    b * 65536 + a 
