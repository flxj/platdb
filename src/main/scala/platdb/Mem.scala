package platdb

class MemDB(val name:String,val path:String):
    var store:DB = null

// TODO 是否需要支持在同一个事务中同时操作内存桶和非内存桶？--> 貌似将内存桶持久化到普通桶中就得在同一个事务中操作
class MemTx( val id:Int):
    def openMemMap(name:String):Option[MemMap] = None 

// TODO 实现一种内存桶,使用自适应基数树，内存桶的内容能持久化到普通桶中
class MemMap(val path:String):
    var db:DB = null
    var tx:MemTx = null

    def put(key:String,value:String):Boolean = false 
    def get(key:String):Option[String] = None

    def writeTo(path:String):Unit = None // 将该内存map持久化到某个文件中 --> 新建一个磁盘DB对象，然后向其写入当前桶的所有内容 （注意版本）
    def appendTo(path:String):Unit = None // 追加到某个文件

class MemSet:
    var bk:MemMap = null 

class MemPriorityQueue:
    var tx:MemTx = null 

class MemFIFO:
    var pq:MemPriorityQueue = null