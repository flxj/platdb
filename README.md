## PlatDB

PlatDB is an embedded key value engine for disk storage, with the goal of providing a simple, easy-to-use, and lightweight data persistence solution. It has the following characteristics：

1) Organize data using a single file for easy migration

2) Support for ACID transactions

3) Supports concurrent execution of read and write transactions (mvcc)

3) Supports multiple commonly used data structures (Map/Set/List/RTree)

4) Support HTTP interface to access data in server mode (TODO)

### Usage


Import platdb into your project first


Sbt
```scala

```


Using platdb is very simple. You only need to provide a data file path, create a DB instance, and open it


The following example will open a database
```scala
import platdb._
import platdb.defaultOptions // Default db configuration

val path = "/tmp/my.db" // If the file does not exist, platdb will attempt to create and initialize it
var db = new DB(path)
db.open() match
    case Failure(e) => println(e.getMessage()) 
    case Success(_) => None

// Perform some read and write operations on this DB instance...

db.close() match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None
```

Platdb allows only one process to open a data file at the same time, so other processes or threads attempting to open will continue to block or timeout (the timeout can be set in Options). 

It is safe for multiple threads to operate on the same DB instance.

### Transaction


All operations on platdb data objects should be encapsulated in the transaction context to ensure consistency of db data content.

Currently, Platdb supports creating read-only or read-write transactions on DB instances, where read-write transactions allow updating of data object status, while read-only transactions only allow reading of data object information.

Platdb supports the simultaneous opening of multiple read-only transactions and at most one read-write transaction (read-write transaction serial execution) on a single DB instance, and uses the mvcc mechanism to control transaction concurrency.


Platdb's data objects are thread unsafe, so do not operate on the same data object concurrently in a transaction. If there is a requirement, each thread should open a transaction separately.


Platdb's Transaction supports management methods for built-in data structure objects. for example
```shell
createBucket: Create a map (alias bucket)
createBucketIfNotExists: Create a map if it does not exist
openBucket: Open an existing map
deleteBucket: Delete a map
...
createList: Create List
createListIfNotExists: Create a list if it does not exist
openList: Open an existing list
deleteList: Delete a list
```
The following example executes a read-only transaction, where `view` is a convenient method provided by the DB instance to execute read-only transactions
```scala
import platdb._
import platdb.defaultOptions

// Open a DB instance

db.view(
    (tx:Transaction) =>
        // Perform some reading operations, such as opening and reading some content from the bucket
        tx.openBucket("bucketName") match
            case Failure(e) => throw e
            case Success(bk) =>
                bk.get("key") match
                    case Failure(e) => throw e
                    case Success(value) => println(value)
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None 

// Close this DB instance
```


If you don't like to write too many match statements to handle the possible exceptions thrown by each step of the operation, the above example can also be written in the following form
```scala
import platdb._
import platdb.Collection._ // Convenient methods for importing collection objects

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
The following example shows the execution of a read/write transaction, and `update` is a convenient method provided by the DB instance to execute read/write transactions
```scala
import platdb._
import platdb.defaultOptions

// Open a DB instance

db.update(
    (tx:Transaction) =>
        // Perform some read and write operations, such as opening and modifying some content in the list at this point
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

// Close this DB instance
```

Similarly, the above example can also be written as
```scala
import platdb._
import platdb.Collection._ 

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
The recommended transaction operations for users are carried out through convenient methods such as view/update, as these methods automatically rollback/commit transactions and return exception information.


Of course, users can also manually manage transactions, and the start method of a DB instance is used to open a transaction object.
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
When manually managing a transaction, it is important to remember to manually close the transaction (explicitly calling the rollback/commit method). For more documentation on transaction methods, refer to [xxxxxx]


Additionally, it should be noted that the results of querying data objects in a transaction are only valid during the current transaction lifecycle, and will be garbage collected when the transaction is closed. Therefore, if you want to use the read content outside of a transaction, you need to copy it out in the transaction.

### data structure


Platdb supports some common data structures:


`Bucket`: An ordered set of key-values (similar to TreeMap in scala/Java), where both key and value are string type data (using platform default encoding), and buckets also support nesting.


`BSet`: An ordered set of strings (similar to TreeSet in scala/Java).


`KList`: A string list that can be used to retrieve elements using subscripts, similar to Scala's ArrayBuffer and List.


`Region`: A spatial index based on RTree, where each Region object can be used to represent an n-dimensional spatial region, providing the ability to add, delete, modify, and query spatial objects.


The common operations for buckets are as follows
```scala
import platdb._
import platdb.Collection._

db.update(
    (tx:Transaction) =>
        given tx = tx
        
        // create bucket
        val bk = createBucketIfNotExists("bucketName") 

        // Open an existing bucket
        val bk2 = openBucket("bucketName2")

        // Add or update bucket elements (if the key already exists, its old value will be overwritten)
        bk+=("key1","value1")
        bk("key2") = "value2"
        
        // Batch adding or modifying bucket elements
        val elems = List[(String,String)](("key2","value2"),("key3","value3"),("key4","value4"))
        bk+=(elems)
        
        // read
        val v = bk("key4")

        // Delete bucket element
        bk-=("key1")

        // Traverse buckets (in ascending dictionary order of keys)
        for e <- bk.iterator do
            e match 
                case (None,None) => None
                case (Some(k),None) => 
                case (Some(k),Some(v)) =>

        // Open a nested sub bucket
        bk.openBucket("subBucketName") match
            case Failure(e) => throw e
            case Success(sbk) => None
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None  
```
For more introduction to bucket methods, please refer to [xxxxxxxxxxx]

The common operations of BSet are as follows
```scala
import platdb._
import platdb.Collection._

db.update(
    (tx:Transaction) =>
        given tx = tx
        
        // create set
        val set = createBSetIfNotExists("setName") 

        // Adding Elements
        set+=("elem1")

        // Batch Add Elements
        val elems = List[String]("elem1","elem2","elem3")
        set+=(elems)
        
        // Determine whether an element exists
        set.contains("elem4") match
            case Failure(e) => throw e
            case Success(flag) => println(flag)

        // Delete Element
        set-=("elem1")

        // Traverse (in ascending dictionary order of elements)
        for e <- set.iterator do
            e match 
                case (None,_) => None
                case (Some(k),_) =>
        
        // Open an existing set
        val set2 = openSet("setName2")

        // set intersection
        val set3 = set & set2

        // Set union
        val set4 = set | set2

        // Set difference
        val set5 = set - set2
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None  
```
For more introduction to the method of set, please refer to [xxxxxxxxxxx]

The common operations for KList are as follows
```scala
import platdb._
import platdb.Collection._

db.update(
    (tx:Transaction) =>
        given tx = tx
        
        // create list
        val list = createListIfNotExists("listName") 

        // Open an existing list
        val list1 = openList("listName") 

        // Add elements at the tail
        list:+=("elem1")

        // Add elements to the header
        list+:=("elem2")

        // Retrieve elements using subscripts
        val e = list(1)

        // Update Element
        list(1) = "newElem"

        // Delete an element
        list.remove(100) match
            case Failure(e) => throw e
            case Success(_) => None 

        // Insert Element
        list.insert(100, "value") match
            case Failure(e) => throw e
            case Success(_) => None 

        // Slice operation
        list.slice(400,500) match
            case Failure(e) => throw e
            case Success(sublist) => 
) match
    case Failure(e) => println(e.getMessage())
    case Success(_) => None 
```
KList also supports other methods such as drop/dropRight, take/taskRight, find/filter, etc. For an introduction to these methods, please refer to the document [xxxxxxxxxxx]


The commonly used methods for Region are as follows
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
Platdb uses Rectangle to describe the boundary of an object, with the min field being a vector that sequentially represents the smallest coordinate value of each dimension, and the max field being used to represent the maximum value of each dimension

Platdb uses SpatialObject to represent a spatial object, the coord field to represent its boundaries, the key field to be a globally unique name, and the data field to be the data associated with the object (can be empty)

For more introductions to other region methods, refer to the document [xxxxxxx]

### Backup

The backup of platdb is very simple, just call the backup method on a DB instance that is already open This method will initiate a read-only transaction, copy a consistent db view to the target file, and the backup process will not block other read/write transactions.
```scala
import platdb._
import platdb.defaultOptions

// 打开一个DB实例

val path = "/path/to/you/backup/file.db"
db.backup(path) match
    case Failure(e) => // backup failed
    case Success(_) => // backup success
```

### TODO

1. Supplement some test cases

2. Implement the Server mode, allowing access to platdb DB instances through the HTTP interface

3. Implement some memory data structures

4. Implement a distributed version of platdb cluster
