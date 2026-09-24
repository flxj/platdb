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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import scala.util.control.NonFatal

/**
  * a simple lru cache.
  *
  * @param capacity
  * @param onEvict
  */
class LRU[K, V](val capacity: Int,var onEvict: (K, V) => Unit = (_:K, _:V) => ()):
    require(capacity > 0, "capacity must be positive")
    /**
      * cache element.
      *
      * @param key
      * @param value
      */
    private class Entry(val key: K, var value: V) extends DoubleLinkedNode[Entry]
    private val list = new DoubleLinkedList[Entry]
    private val index = new ConcurrentHashMap[K, Entry]()
    private val lock = new ReentrantLock()
    /**
      * query cache element.
      *
      * @param key
      * @return
      */
    def get(key: K): Option[V] =
        val e = index.get(key)
        if e == null then 
            None
        else
            lock.lock()
            try
                list.moveToTail(e)
                Some(e.value)
            finally 
                lock.unlock()
    /**
      * cache a element.
      *
      * @param key
      * @param value
      */
    def put(key: K, value: V): Unit =
        lock.lock()
        try
            val e = index.get(key)
            if e != null then
                e.value = value
                list.moveToTail(e)
            else
                val e = new Entry(key, value)
                index.put(key, e)
                list.pushTail(e)
                evictIfNeeded()
        finally 
            lock.unlock()
    /**
      * query element,if not exists then return a compute value.
      *
      * @param key
      * @param compute
      * @return
      */
    def getOrElseUpdate(key: K)(compute: => V): V =
        get(key) match
            case Some(v) => v
            case None =>
                val v = compute
                put(key, v)
                v
    /**
      * delete element.
      *
      * @param key
      * @return
      */
    def remove(key: K): Option[V] =
        lock.lock()
        try
            val e = index.remove(key)
            if e != null then
                list.remove(e)
                Some(e.value)
            else 
                None
        finally 
            lock.unlock()

    def contains(key: K): Boolean = index.containsKey(key)

    def size: Int = index.size()

    def clear(): Unit =
        lock.lock()
        try
            index.clear()
            list.clear()
        finally 
            lock.unlock()

    def keysOrder: Vector[K] =
        lock.lock()
        try 
            list.iterator.map(_.key).toVector
        finally 
            lock.unlock()

    private def evictIfNeeded(): Unit =
        while list.length > capacity do
            list.popHead() match
                case None => None
                case Some(victim) => 
                    index.remove(victim.key)
                    try 
                        onEvict(victim.key,victim.value)
                    catch
                        case NonFatal(e) => throw e
