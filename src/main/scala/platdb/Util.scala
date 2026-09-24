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

import scala.util.Random
import scala.compiletime.ops.double
import scala.util.control.Breaks._
import java.nio.ByteBuffer

private object Util:
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
    def compare(a:Array[Byte],b:Array[Byte]):Int = 
        if a == null then 
            if b == null then 0 else -1
        else
            var r:Int = 0 
            if b != null then 
                breakable(
                    for i <- 0 until min(a.length,b.length) do 
                        if a(i) > b(i) then 
                            r = 1
                            break()
                        else if a(i) < b(i) then
                            r = -1
                            break()
                )
                if r != 0 then r else a.length - b.length
            else 
                1

//
private[platdb] trait DoubleLinkedNode[A <: DoubleLinkedNode[A]]:
    var prev: Option[A] = None
    var next: Option[A] = None

private[platdb] class DoubleLinkedList[A <: DoubleLinkedNode[A]]:
    private var head: Option[A] = None
    private var tail: Option[A] = None
    private var count: Int = 0

    def length: Int = count
    def isEmpty: Boolean = head == None
    def headNode: Option[A] = head
    def tailNode: Option[A] = tail

    def pushTail(node: A): Unit =
        node.prev = tail
        node.next = None
        tail match
            case Some(n) => n.next = Some(node)
            case None => head = Some(node)
        tail = Some(node)
        count += 1

    def pushHead(node: A): Unit =
        node.prev = None
        node.next = head
        head match
            case Some(n) => n.prev = Some(node)
            case None => tail = Some(node)
        head = Some(node)
        count += 1

    def remove(node: A): Unit =
        node.prev match
            case Some(n) => n.next = node.next
            case None => head = node.next
        node.next match
            case Some(n) => n.prev = node.prev
            case None => tail = node.prev

        node.prev = None
        node.next = None
        count -= 1

    def moveToTail(node: A): Unit =
        val eq = tail match
            case Some(n) => node == n 
            case None => false

        if !eq then
            remove(node)
            pushTail(node)

    def moveToHead(node: A): Unit =
        val eq = head match
            case Some(n) => node == n 
            case None => false
        
        if !eq then
            remove(node)
            pushHead(node)

    def popHead(): Option[A] =
        val h = head
        h match
            case Some(n) => remove(n)
            case None => None
        h

    def popTail(): Option[A] =
        val t = tail
        t match
            case Some(n) => remove(n)
            case None => None
        t

    def clear(): Unit =
        var cur = head
        while cur != None do
            cur = cur match
                case Some(n) => 
                    val nxt = n.next
                    n.prev = None 
                    n.next = None
                    nxt
                case None => None
            
        head = None
        tail = None
        count = 0

    def iterator: Iterator[A] = new Iterator[A]:
        private var cur = head
        def hasNext: Boolean = cur != None
        def next(): A =
            cur match 
                case Some(n) => 
                    cur = n.next
                    n
                case None => null.asInstanceOf[A]

private[platdb] class SkipListNode[K, V](var level: Int,val key: K,var value: V):
    var next: Array[Option[SkipListNode[K, V]]] = Array.fill(level + 1)(None)

private[platdb] class SkipList[K, V](maxLevel: Int = 16,probability: Double = 0.5)(using ord: Ordering[K]):
    private val head: SkipListNode[K, V] =
        new SkipListNode[K, V](maxLevel,null.asInstanceOf[K], null.asInstanceOf[V])

    private val rand = new Random()
    private var curLevel: Int = 0
    private var size: Int = 0

    def contains(key: K): Boolean = get(key).isDefined
    def length: Int = size

    private def randomLevel(): Int =
        var level = 0
        while level < maxLevel && rand.nextDouble() < probability do
            level += 1
        level

    private def findPrev(key: K): Array[Option[SkipListNode[K, V]]] =
        val preds:Array[Option[SkipListNode[K, V]]] = Array.fill(maxLevel + 1)(None)
        var cur: SkipListNode[K, V] = head
        var i = curLevel
        while i >= 0 do
            breakable(
                while true do cur.next(i) match
                    case None => break()
                    case Some(node) => 
                        if ord.lt(node.key,key) then
                            cur = node 
                        else
                            break()
            )
            preds(i) = Some(cur)
            i -= 1
        preds

    def put(key: K, value: V): Unit =
        val preds = findPrev(key)
        val node = preds(0) match
            case None => None
            case Some(n) => n.next(0)
        
        node match
            case None => None 
            case Some(n) => 
                if ord.equiv(n.key,key) then
                    n.value = value
                    return

        val newLevel = randomLevel()
        if newLevel > curLevel then
            var i = curLevel + 1
            while i <= newLevel do
                preds(i) = Some(head)
                i += 1
            curLevel = newLevel

        val newNode = new SkipListNode[K, V](newLevel,key, value)
        var i = 0
        while i <= newLevel do
            preds(i) match
                case None => None 
                case Some(node) => 
                    newNode.next(i) = node.next(i)
                    node.next(i) = Some(newNode)
            i += 1
        size += 1

    def get(key: K): Option[V] =
        var cur: SkipListNode[K, V] = head
        var i = curLevel
        while i >= 0 do
            breakable(
                while true do cur.next(i) match
                    case None => break()
                    case Some(node) => 
                        if ord.lt(node.key,key) then 
                            cur = node 
                        else
                            break()
            )
            i -= 1

        cur.next(0) match
            case None => None 
            case Some(node) =>
                if ord.equiv(node.key, key) then
                    Some(node.value)
                else
                    None

    def remove(key: K): Boolean =
        val preds = findPrev(key)
        val target = preds(0) match
            case None => None
            case Some(node) => node.next(0)
        
        target match
            case None => return false 
            case Some(node) => 
                if !ord.equiv(node.key, key) then
                    false
                else
                    var i = 0 
                    while i <= curLevel do preds(i) match
                        case None => None
                        case Some(n) => n.next(i) match
                            case None => None
                            case Some(nxt) => 
                                if nxt eq node then 
                                    n.next(i) = node.next(i)

                    while curLevel > 0 && head.next(curLevel) == None do
                        curLevel -= 1

                    size -= 1
                    true
    //
    def getPrevNode(key:K):Option[SkipListNode[K,V]] = findPrev(key)(0)
    //
    def getNode(key:K):Option[SkipListNode[K,V]] = 
        val preds = findPrev(key)
        preds(0) match
            case None => None
            case Some(node) => node.next(0)
    //
    def putNode(newNode:SkipListNode[K,V]):Unit = 
        val preds = findPrev(newNode.key)
        val node = preds(0) match
            case None => None
            case Some(n) => n.next(0)
        
        node match
            case None => None 
            case Some(n) => 
                if ord.equiv(n.key,newNode.key) then
                    n.value = newNode.value
                    return

        val newLevel = randomLevel()
        if newLevel > curLevel then
            var i = curLevel + 1
            while i <= newLevel do
                preds(i) = Some(head)
                i += 1
            curLevel = newLevel
        
        newNode.level = newLevel
        newNode.next = Array.fill(newLevel + 1)(None)

        var i = 0
        while i <= newLevel do
            preds(i) match
                case None => None 
                case Some(node) => 
                    newNode.next(i) = node.next(i)
                    node.next(i) = Some(newNode)
            i += 1
        size += 1
    //
    def iterator: Iterator[V] = new Iterator[V]:
        private var cur = head.next(0)
        def hasNext: Boolean = cur != None
        def next(): V =
            cur match 
                case Some(node) => 
                    cur = node.next(0)
                    node.value
                case None => null.asInstanceOf[V]
    //
    def nodeIterator: Iterator[SkipListNode[K,V]] = new Iterator[SkipListNode[K,V]]:
        private var cur = head.next(0)
        def hasNext: Boolean = cur != None
        def next(): SkipListNode[K,V] =
            cur match 
                case Some(node) => 
                    cur = node.next(0)
                    node
                case None => null.asInstanceOf[SkipListNode[K,V]]
