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

/**
  * A list of strings
  */
trait BList extends PlatDBIterable:
    /**
      * name
      *
      * @return
      */
    def name:String 
    /**
      * Is the current list empty
      *
      * @return
      */
    def isEmpty: Boolean
    /**
      * Retrieve elements based on subscripts
      *
      * @param idx
      * @return
      */
    def get(idx:Int):Option[String]
    /**
      * Return list header element
      *
      * @return
      */
    def first:Option[String]
    /**
      * Return the element at the end of the list
      *
      * @return
      */
    def last:Option[String]
    /**
      * List slicing operation, obtaining a sub list in read-only mode
      *
      * @param from
      * @param until
      * @return
      */
    def slice(from:Int,until:Int):Option[BList]
    /**
      * Invert the list in place.
      *
      * @return
      */
    def reverseInPlace:Unit
    /**
      * Invert the list, and the obtained inverse list is in read-only mode
      *
      * @return
      */
    def reverse:BList
    /**
      * The list obtained after removing the end element is in read-only mode
      *
      * @return
      */
    def head:BList
    /**
      * The list obtained after removing the header element is in read-only mode
      *
      * @return
      */
    def tail:BList
    /**
      * Filter elements to obtain a read-only sublist
      *
      * @param p
      * @return
      */
    def filter(pred:(String) => Boolean): BList
    /**
      * Find the index of the first element that meets the condition, and return -1 if it does not exist
      *
      * @param p
      * @return
      */
    def find(pred:(String) => Boolean):Int
    /**
      * Get the first n elements, and the obtained sublist is read-only
      *
      * @param n
      * @return
      */
    def take(n: Int): Option[BList]
    /**
      * Obtain the last n elements, and the obtained sublist is read-only
      *
      * @param n
      * @return
      */
    def takeRight(n: Int): Option[BList]
    /**
      * Delete the first n elements
      *
      * @param n
      * @return
      */
    def drop(n: Int):Unit
    /**
      * Delete n elements at the end of the list
      *
      * @param n
      * @return
      */
    def dropRight(n: Int):Unit
    /**
      * Insert an element at the index position
      *
      * @param index
      * @param elem
      * @return
      */
    def insert(index: Int, elem:String):Unit
    /**
      * Insert multiple elements at index position
      *
      * @param index
      * @param elems
      * @return
      */
    def insert(index: Int, elems:Seq[String]):Unit
    /**
      * Add an element to the tail
      *
      * @param elem
      * @return
      */
    def append(elem:String):Unit
    /**
      * Add several elements to the tail
      *
      * @param elems
      * @return
      */
    def append(elems:Seq[String]):Unit
    /**
      * Insert an element into the head
      *
      * @param elem
      * @return
      */
    def prepend(elem: String):Unit
    /**
      * Insert multiple elements into the head
      *
      * @param elems
      * @return
      */
    def prepend(elems: Seq[String]):Unit
    /**
      * Delete an element
      *
      * @param index
      * @return
      */
    def remove(index: Int):Unit
    /**
      * Delete multiple elements
      *
      * @param index
      * @param count
      * @return
      */
    def remove(index: Int, count: Int):Unit
    /**
      * Update element
      *
      * @param index
      * @param elem
      * @return
      */
    def set(index: Int, elem:String):Unit
    /**
      * Does the element that meets the condition exist
      *
      * @param p
      * @return
      */
    def exists(pred:(String) => Boolean): Boolean
    /**
      * Convenient methods for retrieving elements
      *
      * @param n
      * @return
      */
    def apply(n: Int):String
    /**
      * Convenient method, equivalent to append
      *
      * @param elem
      */
    def :+=(elem:String):Unit
    /**
      * Convenient method, equivalent to prepend
      *
      * @param elem
      */
    def +:=(elem:String):Unit
    /**
      * A convenient method for updating elements, equivalent to set
      *
      * @param index
      * @param elem
      */
    def update(index:Int,elem:String):Unit
