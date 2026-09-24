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

private[platdb] class Record(val offset:Array[Int],val row:Array[Byte]):
    def get(i:Int):Array[Byte] = ???
    def columns:Array[Array[Byte]] = ???
    
private[platdb] trait Operator:
    def open():Unit
    def next():Record
    def close():Unit

private[platdb] class SeqScanOperator(val table:RawBucket) extends Operator:
    var iter:RawIterator = null
    def open():Unit = iter = table.iterator
    def next():Record = 
        iter.next() match
            case None => null 
            case Some(k,v) => new Record(null,v)
    def close():Unit = iter = null
