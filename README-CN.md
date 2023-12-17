## 😁 PlatDB

PlatDB是一个面向磁盘的key-value存储引擎,目标是提供一种简单易用，轻量级的数据持久化方案。它具有如下特点 

- 使用单一文件组织数据，便于迁移 👌
- 支持ACID事务👌
- 支持读写事务并发执行(mvcc,一写多读)👌
- 支持多种常用数据结构(Map/Set/List/RTree)👌
- 支持嵌入式的使用方式，也支持作为service独立部署并提供访问数据的http接口👌

platdb实现参考了boltdb等项目，本人开发platdb的主要目的之一是学习数据库相关知识. ⚠️注意当前本项目尚未进行充分的测试，请不要在生产环境中使用！

### 使用platdb 👉

首先需要在你的项目中导入platdb包

sbt
```scala
libraryDependencies += "io.github.flxj" %% "platdb" % "0.12.0-SNAPSHOT"
```

maven
```xml
<dependency>
  <groupId>io.github.flxj</groupId>
  <artifactId>platdb_3</artifactId>
  <version>0.12.0-SNAPSHOT</version>
</dependency>
```

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
platdb允许同一时刻只有一个进程能够打开数据文件，因此其它尝试打开的进程或线程将一直阻塞或超时退出(超时时间可在Options中设置)。多个线程操作同一个DB实例是安全的。

### 事务 👉

所有针对platdb数据对象的操作都应该封装到事务上下文中进行，以确保db数据内容的一致性.
当前platdb支持在DB实例上创建读或写事务，其中可读写事务允许更新数据对象状态，只读事务中只允许读取数据对象信息。
platdb支持一个DB实例上同时打开多个只读事务和至多一个读写事务(读写事务串行执行)，并使用mvcc机制控制事务并发.

platdb的数据对象都是线程不安全的，因此不要在一个事务中并发的操作同一个数据对象，如果有该需求则应该每个线程各自打开一个事务

platdb的Transaction支持对内置数据结构对象的管理方法，比如
```shell
createBucket: 创建map(别名bucket)
createBucketIfNotExists: 如果不存在则创建map
openBucket: 打开一个已经存在的map
deleteBucket: 删除一个map
...
createList: 创建列表
createListIfNotExists: 如果不存在则创建列表
openList: 打开一个已经存在的列表
deleteList: 删除一个列表
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


### 数据结构 👉

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
        val set3 = set & set2

        // 集合并集
        val set4 = set | set2

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


### 备份 👉

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

### 以service方式部署platdb 👉

当前支持将platdb作为一个serevr来运行，并通过http接口访问db数据. 

用户可以导入platdb项目，通过构造一个Server实例的方式运行platdb http服务

```scala

val ops = ServerOptions()
val svc = Server(ops)
svc.run()

```

也可以通过直接运行编译好的platdb jar包的方式运行platdb server
```shell
java -jar platdb-serevr-0.12.0.jar "path/to/you/config/file"
```


如下示例展示了如何通过http接口操作单个集合对象（每个请求在后端均以一个platdb事务来响应）

⭐ 1.查看当前db包含的集合对象名称

```shell
curl -H "Content-Type: application/json" -X GET "http://localhost:8080/v1/collections" | python -m json.tool
```
返回数据结构如下所示：
```json
{
    "collections": [
    {
        "collectionType": "Bucket",
        "name": "bucket2"
    },
    {
        "collectionType": "BList",
        "name": "list1"
    }
    ]
}
```

⭐ 2.查询当前db中的bucket对象名称

```shell
curl -H "Content-Type: application/json" -X GET "http://localhost:8080/v1/buckets" | python -m json.tool
```
返回数据结构如下所示：
```json
{
"collections": [
{
"collectionType": "Bucket",
"name": "bucket2"
}
]
}

```

⭐ 3.创建一个bucekt
```shell
curl -H "Content-Type: application/json" -X POST -d '{"name": "bucket1", "ignoreExists":true}' "http://localhost:8080/v1/buckets"
```

⭐ 4.向bucket中添加元素
```shell
curl -H "Content-Type: application/json" -X POST -d '{"name": "bucket1", "elems":[{"key":"key1","value":"aaa"},{"key":"key2","value":"aaa"},{"key":"key3","value":"ccc"}]}' "http://localhost:8080/v1/buckets/elements"
```


5.查询bucket中的元素(当前仅支持获取所有元素), url参数name为要查询的bucket名称
```shell
curl -H "Content-Type: application/json" -X GET "http://localhost:8080/v1/buckets/elements?name=bucket1" | python -m json.tool
```
返回数据结构如下所示：
```json
[
    {
        "key": "key1",
        "value": "aaa"
    },
    {
        "key": "key2",
        "value": "aaa"
    },
    {
        "key": "key3",
        "value": "ccc"
    }
]
```

⭐ 6.删除bucket中的元素
```shell
curl -H "Content-Type: application/json" -X DELETE -d '{"name":"bucket1","keys":["key1","key2"]}' "http://localhost:8080/v1/buckets/elements"
```

⭐ 7.删除bucket
```shell
curl -H "Content-Type: application/json" -X DELETE -d '{"name":"bucket1","ignoreNotExists":true}' "http://localhost:8080/v1/buckets"
```

⭐ 8.创建一个Blist
```shell
curl -H "Content-Type: application/json" -X POST -d '{"name": "list2", "ignoreExists":true}' "http://localhost:8080/v1/blists"
```

⭐ 9.向Blist尾部添加元素
```shell
curl -H "Content-Type: application/json" -X POST -d '{"name": "list2","prepend":false, "elems":["elem1","elem2","elem3"]}' "http://localhost:8080/v1/blists/elements"
```

⭐ 10.向Blist头部添加元素
```shell
curl -H "Content-Type: application/json" -X POST -d '{"name": "list2","prepend":true, "elems":["elem4","elem5","elem6"]}' "http://localhost:8080/v1/blists/elements"
```


⭐ 11.查询Blist元素(当前仅支持获取所有元素), url参数name为要查询的blist名称
```shell
curl -H "Content-Type: application/json" -X GET "http://localhost:8080/v1/blists/elements?name=list2" | python -m json.tool
```
返回数据结构如下所示：
```json
[
    {
        "index": "0",
        "value": "elem4"
    },
    {
        "index": "1",
        "value": "elem5"
    },
    {
        "index": "2",
        "value": "elem6"
    },
    {
        "index": "3",
        "value": "elem1"
    },
    {
        "index": "4",
        "value": "elem2"
    },
    {
        "index": "5",
        "value": "elem3"
    }
]
```

⭐ 12.更新Blist某个元素
```shell
curl -H "Content-Type: application/json" -X PUT -d '{"name":"list2","index":5,"elem":"vvvvvvvv"}' "http://localhost:8080/v1/blists/elements"
```

⭐ 13.删除Blist元素
```shell
curl -H "Content-Type: application/json" -X DELETE -d '{"name":"list2","index":2,"count":2}' "http://localhost:8080/v1/blists/elements"
```

⭐ 14.删除Blist
```shell
curl -H "Content-Type: application/json" -X DELETE -d '{"name":"list2","ignoreNotExists":true}' "http://localhost:8080/v1/blists"
```


如下示例展示了通过http接口执行一个包含多个操作的事务

1.只读事务

假设用户想要在一个事务中读取bucket中的某些元素，读取Blis中的某些元素，则可以定义如下的操作序列
```json
{
    "operations":[
        {"collection":"bucket1","collectionOp":"","elementOp":"get","elems":[{"key":"key1"},{"key":"key2"}]},
        {"collection":"list1","collectionOp":"","elementOp":"get","elems":[{"key":"0"},{"key":"1"}]}
    ]
}
```

最终于请求为：
```shell
curl -H "Content-Type: application/json" -X POST -d '{"readonly": true,"operations":[{"collection":"bucket1","collectionOp":"","elementOp":"get","elems":[{"key":"key1"},{"key":"key2"}]},{"collection":"list1","collectionOp":"","elementOp":"get","elems":[{"key":"0"},{"key":"1"}]}]}' "http://localhost:8080/v1/txns"
```
执行结果的结构如下所示：注意到如果执行成功，则请求中的每条操作均对应生成一个操作结果条目
```json
{
    "success":true,
    "err":"",
    "results":[
        {"success":true,"err":"","data":[{"key":"key1","value":"value1"},{"key":"key2","value":"value2"}]},
        {"success":true,"err":"","data":[{"key":"0","value":"elem1"},{"key":"1","value":"elem2"}]},
    ]
}
```
如果事务执行失败，则返回的json结构中success字段取值为false, err字段为对应的错误信息，results字段为空
```json
{
    "success":false,
    "err":"this is some error info",
    "results":[]
}
```


2.读写事务

读写事务的请求结构个只读事务一样，只不过需要将readonly参数设为false

假设用户想要在一个事务中创建一个bucket，创建一个list,并向其中写入一些元素，则可以定义如下的操作序列
```json
{
	"operations":[
		{"collection":"","collectionOp":"create","elementOp":"","index":0,"count":0,"elems":[{"key":"bucket2","value":"bucket"}]},
		{"collection":"","collectionOp":"create","elementOp":"","index":0,"count":0,"elems":[{"key":"list1","value":"list"}]},
		{"collection":"bucket2","collectionOp":"","elementOp":"put","index":0,"count":0,"elems":[{"key":"key1","value":"11111"},{"key":"key2","value":"22222"}]},
		{"collection":"list1","collectionOp":"","elementOp":"append","index":0,"count":0,"elems":[{"key":"","value":"aaaaa"},{"key":"","value":"bbbbb"}]},
	]
}
```

最终于请求为：
```shell
curl -H "Content-Type: application/json" -X POST -d '{"readonly": false,"operations":[{"collection":"","collectionOp":"create","elementOp":"","index":0,"count":0,"elems":[{"key":"bucket2","value":"bucket"}]},{"collection":"","collectionOp":"create","elementOp":"","index":0,"count":0,"elems":[{"key":"list1","value":"list"}]},{"collection":"bucket2","collectionOp":"","elementOp":"put","index":0,"count":0,"elems":[{"key":"key1","value":"11111"},{"key":"key2","value":"22222"}]},{"collection":"list1","collectionOp":"","elementOp":"append","index":0,"count":0,"elems":[{"key":"","value":"aaaaa"},{"key":"","value":"bbbbb"}]}]}'  "http://localhost:8080/v1/txns" | python -m json.tool
```
执行结果的结构如下所示：注意到如果执行成功，则请求中的每条操作均对应生成一个操作结果条目
```json
{
    "err": "",
    "results": [
        {
            "data": [],
            "err": "",
            "success": true
        },
        {
            "data": [],
            "err": "",
            "success": true
        },
        {
            "data": [],
            "err": "",
            "success": true
        },
        {
            "data": [],
            "err": "",
            "success": true
        }
    ],
    "success": true
}
```


其它没有列出的api可参看swagger文档(TODO)


### Docker 👉

编辑一份配置文件platdb.conf，放到example目录下， sbt assembly构建项目，然后执行docker build构建镜像

docker run --name xxxxx -p 8080:8080 -v /data:/var/lib/platdb image-name
