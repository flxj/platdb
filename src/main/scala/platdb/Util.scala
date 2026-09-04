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

private[platdb] object Util:
    def intToBytes(v: Long): Array[Byte] = 
        Array(
            ((v >> 24) & 0xFF).toByte,
            ((v >> 16) & 0xFF).toByte,
            ((v >> 8) & 0xFF).toByte,
            (v & 0xFF).toByte
        )
    def bytesToInt(bs: Array[Byte]): Int = 
        require(bs.length >= 4)
        ((bs(0) & 0xFF) << 24) |
        ((bs(1) & 0xFF) << 16) |
        ((bs(2) & 0xFF) << 8) |
        (bs(3) & 0xFF)
    def longToBytes(v: Long): Array[Byte] = 
        Array(
            ((v >> 56) & 0xFF).toByte,
            ((v >> 48) & 0xFF).toByte,
            ((v >> 40) & 0xFF).toByte,
            ((v >> 32) & 0xFF).toByte,
            ((v >> 24) & 0xFF).toByte,
            ((v >> 16) & 0xFF).toByte,
            ((v >> 8) & 0xFF).toByte,
            (v & 0xFF).toByte
        )
    def bytesToLong(bs: Array[Byte]): Long = 
        require(bs.length >= 8)
        ((bs(0) & 0xFFL) << 56) |
        ((bs(1) & 0xFFL) << 48) |
        ((bs(2) & 0xFFL) << 40) |
        ((bs(3) & 0xFFL) << 32) |
        ((bs(4) & 0xFFL) << 24) |
        ((bs(5) & 0xFFL) << 16) |
        ((bs(6) & 0xFFL) << 8) |
        (bs(7) & 0xFFL)
    //def floatToBytes(f: Float): Array[Byte] = ByteBuffer.allocate(4).putFloat(f).array()
    //def bytesToFloat(bytes: Array[Byte]): Float = ByteBuffer.wrap(bytes).getFloat()
    //def doubleToBytes(v:Double): Array[Byte] = ByteBuffer.allocate(8).putDouble(v).array()
    //def bytesToDouble(bs: Array[Byte]): Double = ByteBuffer.wrap(bs).getDouble()
    def compareLong(a:Array[Byte],b:Array[Byte]):Int = ???
    def min(a:Int,b:Int):Int = if a < b then a else b 
    def floatToBytes(f: Float): Array[Byte] = 
        val bits = java.lang.Float.floatToIntBits(f)
        Array[Byte](
            (bits >> 24).toByte,
            (bits >> 16).toByte,
            (bits >> 8).toByte,
            bits.toByte
        )
    def bytesToFloat(bytes: Array[Byte]): Float = 
        val bits = ((bytes(0) & 0xff) << 24) |
                ((bytes(1) & 0xff) << 16) |
                ((bytes(2) & 0xff) << 8)  |
                (bytes(3) & 0xff)
        java.lang.Float.intBitsToFloat(bits)
    def doubleToBytes(d: Double): Array[Byte] = 
        val bits = java.lang.Double.doubleToLongBits(d)
        Array[Byte](
            (bits >> 56).toByte,
            (bits >> 48).toByte,
            (bits >> 40).toByte,
            (bits >> 32).toByte,
            (bits >> 24).toByte,
            (bits >> 16).toByte,
            (bits >> 8).toByte,
            bits.toByte
        )
    def bytesToDouble(bytes: Array[Byte]): Double = 
        val bits = ((bytes(0) & 0xffL) << 56) |
                ((bytes(1) & 0xffL) << 48) |
                ((bytes(2) & 0xffL) << 40) |
                ((bytes(3) & 0xffL) << 32) |
                ((bytes(4) & 0xffL) << 24) |
                ((bytes(5) & 0xffL) << 16) |
                ((bytes(6) & 0xffL) << 8)  |
                (bytes(7) & 0xffL)
        java.lang.Double.longBitsToDouble(bits)