// TODO 实现一种内存桶,使用自适应基数树，内存桶的内容能持久化到普通桶中

// TODO 是否需要支持在同一个事务中同时操作内存桶和非内存桶？--> 貌似将内存桶持久化到普通桶中就得在同一个事务中操作

class MemDB:
    val name:String 
    var store:DB = _

class MemTx:
    val id:Int

    def openMemMap(name:String):Option[MemMap] = None 

class MemMap:
    var db:DB = _ 
    var tx:MemTx = _ 

    def put(key:String,value:String):Boolean
    def get(key:String):Option[String]

    def writeTo(path:String):Unit // 将该内存map持久化到某个文件中 --> 新建一个磁盘DB对象，然后向其写入当前桶的所有内容 （注意版本）

class MemSet:
    var bk:MemMap = _ 

class MemPriorityQueue:
    var tx:MemTx = _ 

class MemFIFO:
    var pq:MemPriorityQueue = _