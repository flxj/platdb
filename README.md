## platdb

[logo]


paltdb是一个嵌面向磁盘存储的嵌入式key-value引擎,目标是提供一种使用简单，轻量级的数据持久化方案。具有如下特点 
1)使用单一文件组织数据，便于迁移 
2)支持ACID事务
3)支持读写事务并发执行(mvcc,一写多读)
3)支持多种常用数据结构(Map/Set/List/RTree) 
4)server模式下支持http接口访问数据 （TODO）

platdb实现参考了boltdb等项目，本人开发platdb的主要目的之一是学习数据库相关知识

### 使用

在你的项目中导入platdb

marven

sbt

使用platdb非常简单，你只需要提供一个数据文件路径，创建一个DB实例并打开它即可

如下示例将打开一个数据库
```scala
import platdb._
import platdb.defaultOptions // 默认的db配置

val path = "/tmp/my.db" // 如果文件不存在则platdb会尝试创建并初始化它
var db = new DB(path)
db.open() match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None

// 对该DB实例做一些读写操作...

db.close() match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None
```
platdb允许同一时刻只有一个进程能够打开数据文件，因此其它尝试打开的进程或线程将一直阻塞或超时退出(超时时间可在Options中设置)。

多个线程操作同一个DB实例是安全的。


### 事务

所有针对platdb数据对象的操作都应该封装到事务上下文中进行，以确保db数据内容的一致性.
当前platdb支持在DB实例上创建读或写事务，其中可读写事务允许更新数据对象状态，只读事务中只允许读取数据对象信息。
platdb支持一个DB实例上同时打开多个只读事务和至多一个读写事务(读写事务串行执行)，并使用mvcc机制控制事务并发.

platdb的数据对象都是线程不安全的，因此不要在一个事务中并发的操作同一个数据对象，如果有该需求则应该每个线程各自打开一个事务

platdb的Transaction支持对内置数据结构对象的管理方法
```shell
createBucket:
createBucketIfNotExists:
openBucket:
deleteBucket:
...
createList:
createListIfNotExists:
openList:
deleteList:
```

如下示例执行一个只读事务,view是DB实例提供的执行只读事务的便捷方法
```scala
import platdb._
import platdb.defaultOptions

// 打开一个DB实例

db.view(
    (tx:Transaction) =>
        // 做一些读取操作，比如此处代码打开并读取了bucket中的一些内容
        tx.openBucket("bucketName") match
            case Failure(e) => throw e
            case Success(bk) =>
                bk.get("key") match
                    case Failure(e) => throw e
                    case Success(value) => println(value)
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None    

// 关闭该DB实例
```
如果你不喜欢写太多match语句来处理每一步操作可能抛出的异常，上述示例也可以写成如下形式
```scala
import platdb._
import platdb.Collection._ // 导入collection对象中的便捷方法

db.view(
    (tx:Transaction) =>
        given tx = tx
        
        val bk = openBucket("bucketName") 
        val value = bk("key")
        println(value)
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None    
```

如下示例显执行了一个读写事务，update是DB实例提供的执行读写事务的便捷方法
```scala
import platdb._
import platdb.defaultOptions

// 打开一个DB实例

db.update(
    (tx:Transaction) =>
        // 做一些读写操作，比如此处代码打开并修改了list中的一些内容
        tx.openList("listName") match
            case Failure(e) => throw e
            case Success(list) =>
                list.append("value1") match
                    case Failure(e) => throw e
                    case Success(_) => None
                list.prepend("value2") match
                    case Failure(e) => throw e
                    case Success(_) => None
                list.update(3,"value3") match
                    case Failure(e) => throw e
                    case Success(_) => None
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None    

// 关闭该DB实例
```
同样地，上述示例也可写为
```scala
import platdb._
import platdb.Collection._ // 导入collection对象中的便捷方法

db.update(
    (tx:Transaction) =>
        given tx = tx
        
        val list = openList("listName") 
        list:+="value1"
        list+:="value2"
        list(3) = "value3"
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None    
```
推荐用户的事务操作都通过view/update等便捷方法进行，因为这些方法会自动回滚/提交事务，并把异常信息返回

当然用户也可以手动管理事务，DB实例的begin方法用于打开一个事务对象
```scala
import platdb._

var tx:Transaction = null
try
    db.begin(true) match
        case Failure(e) => throw e
        case Success(tx) =>
            // use the transaction here
            tx.commit() match
                case Failure(e) => throw e
                case Success(_) => None
catch
    case e:Exception => 
        if tx!=null then
            tx.rollback() match
                case Failure(e) => // process the error if need
                case Success(_) => None
        // process the error if need
   
```
手动管理事务时务必记住手动关闭该事务(显式的调用回滚/提交方法)。更多关于事务方法的文档，参考 [xxxxxx]

另外需要注意的是在事务中查询数据对象的结果仅在当前事务生命周期中有效，当事务关闭后会被垃圾回收。因此如果想要在事务之外使用读取到的内容，需要在事务中将其拷贝出来。


### 数据结构

platdb支持一些常见的数据结构:

`Bucket`:有序的key-value集合（可类比scala/java的TreeMap）其中key,value均为string类型数据(使用平台默认编码)，Bucket也支持嵌套。

`BSet`：有序的字符串集合(可类比scala/java的TreeSet)。

`KList`: 字符串列表，可使用下标检索元素，类比scala的ArrayBuffer和List。

`Region`：基于RTree的空间索引，每个Region对象可用来表示一个n维空间区域，提供对空间对象的增删改查操作能力。


Bucket常见操作如下
```scala
import platdb._
import platdb.Collection._

db.update(
    (tx:Transaction) =>
        given tx = tx
        
        // 创建bucket
        val bk = createBucketIfNotExists("bucketName") 

        // 打开一个已经存在的bucket
        val bk2 = openBucket("bucketName2")

        // 添加或更新bucket元素(如果key已经存在，则会覆盖其旧值)
        bk+=("key1","value1")
        bk("key2") = "value2"

        // 批量添加或修改bucket元素
        val elems = List[(String,String)](("key2","value2"),("key3","value3"),("key4","value4"))
        bk+=(elems)
        
        // 读取
        val v = bk("key4")

        // 删除bucket元素
        bk-=("key1")

        // 遍历bucket(按照key的字典升序)
        for e <- bk.iterator do
            e match 
                case (None,None) => None
                case (Some(k),None) => 
                case (Some(k),Some(v)) =>

        // 打开一个嵌套子bucket
        bk.openBucket("subBucketName") match
            case Failure(e) => throw e
            case Success(sbk) => None
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None  
```
更多关于bucket的方法介绍，参考[xxxxxxxxxx]


BSet常见操作如下所示
```scala
import platdb._
import platdb.Collection._

db.update(
    (tx:Transaction) =>
        given tx = tx
        
        // 创建集合
        val set = createBSetIfNotExists("setName") 

        // 添加元素
        set+=("elem1")

        // 批量添加元素
        val elems = List[String]("elem1","elem2","elem3")
        set+=(elems)
        
        // 判断元素是否存在
        set.contains("elem4") match
            case Failure(e) => throw e
            case Success(flag) => println(flag)

        // 删除元素
        set-=("elem1")

        // 遍历(按照元素的字典升序)
        for e <- set.iterator do
            e match 
                case (None,_) => None
                case (Some(k),_) =>
        
        // 打开一个已经存在的集合
        val set2 = openSet("setName2")

        // 集合交集
        val set3 = set and set2

        // 集合并集
        val set4 = set union set2

        // 集合差
        val set5 = set - set2
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None  
```
更多关于set的方法介绍，参考[xxxxxxxxxx]


KList常见操作如下所示
```scala
import platdb._
import platdb.Collection._

db.update(
    (tx:Transaction) =>
        given tx = tx
        
        // 创建列表
        val list = createListIfNotExists("listName") 

        // 打开一个已经存在的列表
        val list1 = openList("listName") 

        // 在尾部添加元素
        list:+=("elem1")

        // 在头部添加元素
        list+:=("elem2")

        // 使用下标检索元素
        val e = list(1)

        // 更新元素
        list(1) = "newElem"

        // 删除一个元素
        list.remove(100) match
            case Failure(e) => throw e
            case Success(_) => None 

        // 插入元素
        list.insert(100, "value") match
            case Failure(e) => throw e
            case Success(_) => None 

        // 切片操作
        list.slice(400,500) match
            case Failure(e) => throw e
            case Success(sublist) => 
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None 
```
KList还支持其它诸如drop/dropRight,take/taskRight,find/flter等方法，关于这些方法的介绍，参考文档[xxxxxxxxxx]


Region常用方法如下
```scala
import platdb._
import platdb.Collection._

db.update(
    (tx:Transaction) =>
        given tx = tx
        
        // 创建一个二维region
        val reg = createRegionIfNotExists("regionName"，2) 

        // 添加空间对象
        val obj = SpatialObject(Rectange(Array[Double](10.2,10.5),Array[Double](17.8,19.3)),"key","data")
        reg+=(obj)

        // 查询对象
        

        // 删除对象
        
        // 范围查询
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None 
```

platdb使用Rectangle描述一个对象的边界，min字段为一个向量，依次表示各个维度最小的坐标值，max字段用来表示各个维度的最大值

platdb使用SpatialObject表示一个空间对象，coord字段表示其边界，key字段为全局唯一的名称，data字段为该对象关联的数据(可为空)


更多其它Region方法的介绍，参考文档 [xxxxxxx]


[其它操作]

platdb的备份很简单，只需要对一个已经打开状态的DB实例，调用backup方法即可. 该方法会开启一个只读事务，复制一个一致的db视图到目标文件中，备份过程不会阻塞其它读/写事务。

```scala
import platdb._
import platdb.defaultOptions

// 打开一个DB实例

val path = "/path/to/you/backup/file.db"
db.backup(path) match
    case Failure(e) => // backup failed
    case Success(_) => // backup success

```


[server模式](TODO)
