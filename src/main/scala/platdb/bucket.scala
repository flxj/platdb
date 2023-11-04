package platdb

import java.nio.ByteBuffer
import scala.collection.mutable.{Map,ArrayBuffer}
import scala.util.control.Breaks._
import scala.util.Try
import scala.util.Success
import scala.util.Failure


// count is the number of keys in current bucket
private[platdb] class BucketValue(var root:Long,var count:Long,var sequence:Long,val dataType:Byte):
    // convert bucket value to byte array.
    def getBytes:Array[Byte] = 
        var r = root
        var c = count
        var s = sequence
        var arr = new Array[Byte](BTreeBucket.valueSize)
        for i <- 0 to 7 do
            arr(7-i) = (r & 0xff).toByte
            arr(15-i) = (c & 0xff).toByte
            arr(23-i) = (s & 0xff).toByte
            r = r >> 8
            c = c >> 8
            s = s >> 8
        arr(BTreeBucket.valueSize-1) = dataType
        arr
    override def toString(): String = new String(getBytes,"ascii")
    override def clone:BucketValue = new BucketValue(root,count,sequence,dataType)

//
private[platdb] object BTreeBucket:
    // bucket value size when convert byte array.
    // val valueSize:Int = 24
    val valueSize:Int = 25
    def apply(data:Array[Byte]):Option[BucketValue] =
        if data.length!=valueSize then
            return None 
        val arr = for i <- 0 to 2 yield
            val n:Long = (data(8*i) & 0xff) << 56 
            | (data(8*i+1) & 0xff) << 48 | (data(8*i+2) & 0xff) << 40 | (data(8*i+3) & 0xff) << 32 
            | (data(8*i+4) & 0xff) << 24 | (data(8*i+5) & 0xff) << 16 | (data(8*i+6) & 0xff) << 8 | (data(8*i+7) & 0xff)
            n
        Some(new BucketValue(arr(0),arr(1),arr(2),data(valueSize-1)))

/**
  * bucket trait implement by b+ tree.
  *
  * @param bkname
  * @param tx
  */
private[platdb] class BTreeBucket(val bkname:String,var tx:Tx) extends Bucket:
    var bkv:BucketValue = null
    var root:Option[Node] = None
    /** cache nodes about writeable tx. */
    var nodes:Map[Long,Node] = Map[Long,Node]() 
    /** cache sub-buckets */
    var buckets:Map[String,BTreeBucket] = Map[String,BTreeBucket]()

    // keys number
    def name:String = bkname 
    def length:Long = bkv.count
    def value:BucketValue = bkv 
    def closed:Boolean = tx == null || tx.closed
    /**
      * return a bucket iterator.
      *
      * @return BucketIterator
      */
    def iterator:CollectionIterator = new BTreeBucketIter(this)
    /**
      * 
      *
      * @param key
      * @param value
      */
    def +=(key:String,value:String):Unit = 
        put(key,value) match
            case Success(_) => None
            case Failure(e) => throw e
    /**
      * 
      *
      * @param elems
      */ 
    def +=(elems:Seq[(String,String)]):Unit =
        for (k,v) <- elems do
            put(k,v) match
                case Success(_) => None
                case Failure(e) =>  throw e
    /**
      * 
      *
      * @param key
      */
    def -=(key:String):Unit = 
        delete(key) match
            case Success(_) => None
            case Failure(e) => throw e
    /**
      * 
      *
      * @param keys
      */
    def -=(keys:Seq[String]):Unit =
        for k <- keys do
            delete(k) match
                case Success(_) => None
                case Failure(e) => throw e
    /**
      * 
      *
      * @param key
      * @param value
      */
    def update(key:String,value:String):Unit = this.+=(key,value)
    /**
      * 
      *
      * @param key
      * @return
      */
    def apply(key:String):String = 
        get(key) match
            case Success(v) => v
            case Failure(e) => throw e 
    /**
      * 
      *
      * @param key
      * @return
      */
    def contains(key:String):Try[Boolean] = 
        if tx.closed then
            return Failure(DB.exceptionTxClosed)
        else if key.length == 0 then 
            return Failure(DB.exceptionKeyIsNull)
        
        val c = iterator
        c.find(key) match 
            case (None,_) => Failure(new Exception(s"not found key:$key"))
            case (Some(k),v) => 
                if k == key then 
                    Success(true)
                else
                    Failure(DB.exceptionValueNotFound)
    /**
      * try to retrieve the value for a key in the bucket.
      * Returns is Failure if the key does not exist or the key is a subbucket name.
      * The returned value is only valid for the life of the transaction.
      * @return value of key
      */
    def get(key:String):Try[String] = 
        if tx.closed then
            return Failure(DB.exceptionTxClosed)
        else if key.length == 0 then 
            return Failure(DB.exceptionKeyIsNull)
        
        val c = iterator
        c.find(key) match 
            case (None,_) => return Failure(new Exception(s"not found key:$key"))
            case (Some(k),v) => 
                if k == key then 
                    v match
                        case None => return Failure(new Exception("key is a subbucket"))
                        case Some(s) => return Success(s) 
        Failure(DB.exceptionValueNotFound)
    /**
      * 
      *
      * @param key
      * @param defalutValue
      * @return
      */
    def getOrElse(key:String,defalutValue:String):String =
        get(key) match
            case Failure(_) => defalutValue
            case Success(v) => v 
    /**
      * put method insert or update(overwritten) the value for a key in the bucket.
      * Put operation will failed if the key is null or too large, or the value is too large.
      * If the bucket was managed by a readonly transaction, not allow put operation on it.
      * @param key
      * @param value
      * @return success flag
      */
    def put(key:String,value:String):Try[Unit] =
        if tx.closed then
            return Failure(DB.exceptionTxClosed) 
        else if !tx.writable then 
            return Failure(DB.exceptionNotAllowOp) 
        else if key.length == 0 then
            return Failure(DB.exceptionKeyIsNull)
        else if  key.length>=DB.maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)
        else if value.length>=DB.maxValueSize then
            return Failure(DB.exceptionValueTooLarge)

        var c = new BTreeBucketIter(this)
        c.search(key) match 
            //case (None,_,_) => return Failure(DB.exceptionValueNotFound) // TODO use Try
            case (None,_,_) => None
            case (Some(k),_,f) =>
                if k == key && f == bucketType then
                    return Failure(new Exception("the value is subbucket,not allow update it by put method"))
        c.node() match 
            case None => Failure(new Exception(s"not found insert node for key:$key"))
            case Some(node) =>
                node.put(key,key,value,leafType,0)
                bkv.count+=1
                Success(None)
         
    /**
      * try to remove a key from the bucket.
      * delete operation will be ignore if the key does not exist.
      * If the bucket was managed by a readonly transaction, not allow put operation on it.
      * @param key
      * @return success flag
      */
    def delete(key:String):Try[Unit] = 
        if tx.closed then
            return Failure(DB.exceptionTxClosed) 
        else if !tx.writable then 
            return Failure(DB.exceptionNotAllowOp) 
        else if key.length <=0 then
            return Failure(DB.exceptionKeyIsNull) 
        
        var c = new BTreeBucketIter(this)
        c.search(key) match 
            case (None,_,_) => return Failure(DB.exceptionValueNotFound)
            case (Some(k),_,f) =>
                if k == key && f == bucketType then
                    return Failure(new Exception("not allow delete subbucket value by delete method")) 
        c.node() match 
            case None => Failure(new Exception("not found delete object"))
            case Some(node) =>
                node.del(key) 
                bkv.count-=1
                buckets.remove(key)
                Success(None)
    /**
      * getBucket method retrieve a sub bucket in current bucket.
      * The returned bucket instance is only valid during transaction current lifecycle.
      * @param name: subbucket name
      * @return subbucket
      */
    def getBucket(name:String):Try[Bucket] = getBucket(name,bucketDataType)
    /**
      * createBucket try to create a new bucket and return it.
      * The create operation will failed if the key is already exists,or the name parameter is null or too large
      * The returned bucket instance is only valid during current transaction lifecycle.
      * @param name: bucket name
      * @return subbucket
      */
    def createBucket(name:String):Try[Bucket] = createBucket(name,bucketDataType)
    /**
      * create a new bucket if it doesn't exist,if already exists or create success then return it. 
      * create operation will failed if name is null or too large.
      * The returned bucket instance is only valid during current transaction lifecycle.
      * @param name: bucket name
      * @return subbucket
      */
    def createBucketIfNotExists(name:String):Try[Bucket] = createBucketIfNotExists(name,bucketDataType)
    /**
      * delete a subbucket.
      * delete opreation will failed if the bucket doesn't exist.
      * @param name: subbucket name
      * @return success flag
      */
    def deleteBucket(name:String):Try[Unit] = deleteBucket(name,bucketDataType)
    /*
    /**
      * getBucket method retrieve a sub bucket in current bucket.
      * The returned bucket instance is only valid during transaction current lifecycle.
      * @param name: subbucket name
      * @return subbucket
      */
    def getBucket(name:String):Try[BTreeBucket] = 
        if tx.closed then
            return Failure(DB.exceptionTxClosed) 
        else if name.length==0 then 
            return Failure(DB.exceptionKeyIsNull)

        if buckets.contains(name) then 
            buckets.get(name) match
                case Some(bk) => return Success(bk)
                case None =>  return Failure(new Exception(s"buckets cache failed,not found $name"))

        var c = new BTreeBucketIter(this)
        c.search(name) match
            case (None,_,_) => return Failure(new Exception(s"not found bucket $name"))
            case (Some(k),v,f) => 
                if k!=name || f!=bucketType then 
                    return Failure(new Exception(s"not found bucket $name"))
                v match 
                    case None => Failure(new Exception(s"query bucket $name value failed"))
                    case Some(data) =>
                        val bytes = data.getBytes("ascii")
                        BTreeBucket(bytes) match
                            case None => Failure(new Exception(s"parse bucket $name value failed,expect data length is ${BTreeBucket.valueSize} but actual get ${bytes.length}")) 
                            case Some(value) =>
                                var bk = new BTreeBucket(name,tx)
                                bk.bkv = value 
                                buckets(name) = bk
                                Success(bk)
    /**
      * createBucket try to create a new bucket and return it.
      * The create operation will failed if the key is already exists,or the name parameter is null or too large
      * The returned bucket instance is only valid during current transaction lifecycle.
      * @param name: bucket name
      * @return subbucket
      */
    def createBucket(name:String):Try[BTreeBucket] = 
        if tx.closed then
            return Failure(DB.exceptionTxClosed) 
        else if !tx.writable then 
            return Failure(DB.exceptionNotAllowOp) 
        else if name.length()<=0 then 
            return Failure(DB.exceptionKeyIsNull)
        else if name.length() >= DB.maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)
        else if buckets.contains(name) then 
            return Failure(new Exception(s"bucket $name is already exists"))
        
        var c = new BTreeBucketIter(this)
        c.search(name) match
            //case (None,_,_) => return Failure(new Exception("bucket create failed: not found create node"))
            case (None,_,_) => None
            case (Some(k),v,f) => 
                if k == name && f != bucketType then
                    return Failure(new Exception(s"bucket create failed: key $name is already exists")) 
                if k == name && f == bucketType then
                    v match 
                        case None => None
                        case Some(data) =>
                            BTreeBucket(data.getBytes("ascii")) match
                                case None => return Failure(new Exception(s"parse bucket $name value failed"))  
                                case Some(value) =>
                                    var bk = new BTreeBucket(name,tx)
                                    bk.bkv = value 
                                    bk.root = getNode(value.root)
                                    buckets(name) = bk
                    return Failure(new Exception(s"bucket create failed: bucket $name is already exists"))
        // create a new bucket
        var bk = new BTreeBucket(name,tx)
        bk.bkv = new BucketValue(-1,0,0,bucketDataType) // null bkv
        bk.root = Some(new Node(new BlockHeader(-1L,leafType,0,0,0))) // null root node
        buckets(name) = bk
        c.node() match 
            case None => Failure(new Exception("bucket create failed: not found create node"))
            case Some(n) =>
                n.put(name,name,bk.value.toString(),bucketType,0)
                bkv.count+=1
                Success(bk)
    /**
      * create a new bucket if it doesn't exist,if already exists or create success then return it. 
      * create operation will failed if name is null or too large.
      * The returned bucket instance is only valid during current transaction lifecycle.
      * @param name: bucket name
      * @return subbucket
      */
    def createBucketIfNotExists(name:String):Try[BTreeBucket] =
        if tx.closed then
            return Failure(DB.exceptionTxClosed) 
        else if !tx.writable then 
            return Failure(DB.exceptionNotAllowOp)  
        else if name.length()<=0 then 
            return Failure(DB.exceptionKeyIsNull)
        else if name.length() >= DB.maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)

        getBucket(name) match
            case Success(bk) => Success(bk)
            case Failure(e) => createBucket(name) // TODO check if the exception is not exists
    /**
      * delete a subbucket.
      * delete opreation will failed if the bucket doesn't exist.
      * @param name: subbucket name
      * @return success flag
      */
    def deleteBucket(name:String):Try[Unit] =
        if tx.closed then
            return Failure(DB.exceptionTxClosed)  
        else if !tx.writable then 
            return Failure(DB.exceptionNotAllowOp)
        else if name.length()<=0 then 
            return Failure(DB.exceptionKeyIsNull)
        else if name.length() >= DB.maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)
        
        var c = new BTreeBucketIter(this)
        c.search(name) match 
            case (None,_,_) => return Failure(new Exception(s"not found key $name")) 
            case (Some(k),_,f) => 
                // key not exists or exists but not a bucket
                if k!=name then 
                    return Failure(new Exception(s"$name not exists")) 
                if f!=bucketType then
                    return Failure(new Exception(s"$name is not a bucket"))

                // delete subbuckets recursively
                getBucket(name) match
                    case Failure(e) => return Failure(new Exception(s"query bucket $name failed:${e.getMessage()}")) 
                    case Success(childBk) => 
                        try 
                            for (k,v) <- childBk.iterator do
                                k match
                                    case None => throw new Exception(s"query get null key in bucket ${childBk.name}")
                                    case Some(key) =>
                                        v match
                                            case Some(_) => None // k is not a bucket,so do nothing for it
                                            case None => 
                                                childBk.deleteBucket(key) match
                                                    case Success(_) => None
                                                    case Failure(e) => throw e
                        catch
                            case e:Exception => return Failure(e)
                        // delete current bucket
                        buckets.remove(name) // clean cache
                        childBk.nodes.clear()  // clean cache nodes
                        childBk.root = None 
                        childBk.freeAll() // release all pages about the bucket
                        c.node() match 
                            case None => return Failure(new Exception(s"not found bucket $name node")) 
                            case Some(node) => 
                                node.del(name) // delete bucket record from the node.
                                bkv.count-=1
                Success(None)
    */
    /** 
     * try to get node or block by block id.
     * 
     */
    def nodeOrBlock(id:Long):(Option[Node],Option[Block]) = 
        if nodes.contains(id) then 
            (nodes.get(id),None)
        else 
            if id < 0 then
                // if id <0 means the bucket is a new created in memory, not loaded from disk.
                return (root,None)
            tx.block(id) match
                case Success(bk) => (None,Some(bk))
                case Failure(e) => (None,None)
    /**
      * parse node elements info from block.
      *
      * @param bk
      * @return
      */
    def nodeElements(bk:Option[Block]):Option[ArrayBuffer[NodeElement]] = 
        bk match
            case None => None 
            case Some(block) => 
                if nodes.contains(block.id) then
                    return Some(nodes(block.id).elements)
                else
                    Node.elements(block)
    /**
      * try to get the idx node element from the block. 
      *
      * @param bk
      * @param idx
      * @return
      */
    def getNodeElement(bk:Option[Block],idx:Int):Option[NodeElement] = 
        nodeElements(bk) match
            case None => None
            case Some(elems) =>
                if idx>=0 && elems.length>idx then 
                    return Some(elems(idx))
                None
    /**
      * convert block to node. 
      *
      * @param bk
      * @return
      */
    def getNodeByBlock(bk:Try[Block]):Option[Node] = 
        bk match
            case Failure(_) => None 
            case Success(block) => 
                if nodes.contains(block.id) then 
                    return nodes.get(block.id)
                Node(block) match
                    case None => None 
                    case Some(node) => 
                        nodes(node.id) = node
                        Some(node)
    /**
      *  get node by id, search from cache -> disk.
      *
      * @param id
      * @return
      */
    def getNode(id:Long):Option[Node] = 
        if nodes.contains(id) then 
            return nodes.get(id)
        getNodeByBlock(tx.block(id))
    
    /**
      * try to get a child node by index. 
      *
      * @param n
      * @param idx
      * @return
      */
    def getNodeChild(n:Option[Node],idx:Int):Option[Node] = 
        n match
            case None => None
            case Some(node) =>
                if node.isLeaf || idx<0 || idx>=node.length then
                    return None
                getNode(node.elements(idx).child) match
                    case None => None
                    case Some(child) =>
                        child.parent = Some(node)
                        node.children += child
                        Some(child)
    /**
      * try to get right brother node. 
      *
      * @param node
      * @return
      */
    private def getNodeRightSibling(node:Node):Option[Node] = 
        node.parent match
            case None => None 
            case Some(p) =>
                val idx = p.childIndex(node)
                if idx >=0 && idx < p.length-1 then 
                    getNodeChild(Some(p),idx+1)
                else
                    None

    /**
      *  try to get node left brother node. 
      *
      * @param node
      * @return
      */
    private def getNodeLeftSibling(node:Node):Option[Node] =
        node.parent match
            case None => None 
            case Some(p) =>
                val idx = p.childIndex(node)
                if idx >=1 then 
                    getNodeChild(Some(p),idx-1)
                else
                    None
    /**
     * rebalance the bucket,merge some small nodes.
     * 
     */
    def merge():Unit = 
        for (_,node) <- nodes do 
            mergeOnNode(node)
        for (_,rbk) <- regions do
            rbk.merge()
        for (_,bk) <- buckets do 
            bk.merge()
    /**
      * try to merge the node.
      *
      * @param node
      */  
    private def mergeOnNode(node:Node):Unit =
        if !node.unbalanced then return None
        node.unbalanced = false
        // check whether the node meets the threshold.
        val threshold:Int = DB.pageSize / 4
        if node.size() > threshold && node.length > Node.lowerBound(node.ntype) then
            return None
        // whether the current node is the root node.
        node.parent match
            case None =>
                /**
                  * If the root node is a branch node and has only one child node, then directly promote the child node to the new root node
                  * (obviously, if the root node is a leaf node, even if it contains one element, it does not need to be processed)
                  */
                if !node.isLeaf && node.length == 1 then 
                    getNodeChild(Some(node),0) match
                        case None => throw new Exception("merge root node failed: query child error")
                        case Some(child) => 
                            child.parent = None // set the child as new root and release old root
                            bkv.root = child.id
                            root = Some(child)
                            node.removeChild(child)
                            nodes.remove(node.id)
                            freeNode(node)
                    return None
            case Some(p) => 
                if node.length == 0 then // If node has no keys then just remove it.
                    p.del(node.minKey)
                    p.removeChild(node)
                    nodes.remove(node.id)
                    freeNode(node)
                    mergeOnNode(p)
                    return None
                // merge current node to right or left brother node.

                // The current node is the leftmost node of its parent node, 
                // so its right sibling node needs to be merged into the current node.
                if p.childIndex(node) == 0 then
                    getNodeRightSibling(node) match
                        case None => throw new Exception("merge node failed: get right brother node error")
                        case Some(mergeFrom) => 
                            for elem <- mergeFrom.elements do 
                                // If there are mergeFrom child nodes in the cache,then reset them parent as current node.
                                if nodes.contains(elem.child) then
                                    var child = nodes(elem.child)
                                    mergeFrom.removeChild(child)
                                    node.children+=child
                                    child.parent = Some(node)
                            // remove all elements from mergeFrom to current node.
                            node.elements ++= mergeFrom.elements
                            p.del(mergeFrom.minKey)
                            p.removeChild(mergeFrom)
                            nodes.remove(mergeFrom.id)
                            freeNode(mergeFrom)
                else
                    getNodeLeftSibling(node) match // Merges the current node into its left sibling.
                        case None => throw new Exception("merge node failed: get left brother node error")
                        case Some(mergeTo) => 
                            for elem <- node.elements do 
                                if nodes.contains(elem.child) then 
                                    var child = nodes(elem.child)
                                    node.removeChild(child)
                                    mergeTo.children+=child
                                    child.parent = Some(mergeTo)
                            // remove all elements from current node to mergeTo.
                            mergeTo.elements ++= node.elements
                            p.del(node.minKey)
                            p.removeChild(node)
                            nodes.remove(node.id)
                            freeNode(node)
                // Recursively processes the parent node of the current node.
                mergeOnNode(p)
    /**
      * rebalance bucket,split too large nodes.
      */
    def split():Unit =
        for (name,region) <- regions do
            region.split()
        // split all cache subbuckets.
        for (name,bucket) <- buckets do 
            bucket.split()
            // split operation maybe change the sub-bucket's root,so need update the newest root info in current bucket.
            val v = bucket.value
            bucket.root match 
                case None => None // Skip writing the bucket if there are no materialized nodes.
                case Some(n) =>
                    var c = new BTreeBucketIter(this)
                    c.search(name) match 
                        case (None,_,_) => throw new Exception(s"misplaced bucket header:$name")
                        case (Some(k),_,flag) =>
                            if k!=name then 
                                throw new Exception(s"misplaced bucket header:$name")
                            if flag!=bucketType then 
                                throw new Exception(s"unexpected bucket header: $name flag:$flag")
                            c.node() match 
                                case Some(node) => node.put(k,name,v.toString(),bucketType,0)
                                case None => throw new Exception(s"not found leaf node for bucket element:$name")
        // split current bucket
        root match
            case None => return None // Ignore if there's not a materialized root node.
            case Some(node) => splitOnNode(node)
        
        // update root info.
        root match
            case None => None
            case Some(node) =>
                var r = node.root
                if r.id >= tx.maxPageId then
                    throw new Exception(s"pgid ${r.id} above high water mark ${tx.maxPageId}")
                bkv.root = r.id 
                root = Some(r)
    /**
      * split node recursively.
      *
      * @param node
      */
    private def splitOnNode(node:Node):Unit =
        if node.spilled then return None
        // Recursively slice the children of the current node. 
        // Note that child node splitting may add more elements to the current node's children array, 
        // and these new elements do not need to be repartitioned, so subscripts are recycled here.
        val n = node.children.length
        for i <- 0 until n do 
            splitOnNode(node.children(i))
        
        // We no longer need the child list because it's only used for spill tracking.
        if node.children.length!=0 then
            node.children = new ArrayBuffer[Node]()
        // split current node.
        for n <- splitNode(node,DB.pageSize) do 
            if n.id > 0 then 
                tx.free(n.id)
                n.header.pgid = 0
            
            // allocate a new block and write node content to it.
            val nid = tx.allocate(n.size()) 
            if nid >= tx.maxPageId then 
                throw new Exception(s"pgid $nid above high water mark ${tx.maxPageId}")
            var bk = tx.makeBlock(nid,n.size())
            n.header.pgid = bk.id
            n.writeTo(bk)
            n.spilled = true
            // insert the new node info to its parent.
            n.parent match
                case None => None
                case Some(p) => 
                    var k = n.minKey
                    if k.length()==0 then k = n.elements(0).key 
                    p.put(k,n.elements(0).key,"",0,n.id)
                    n.minKey = n.elements(0).key
        
        // if the old root was splitd and created a new one, we need split it as well.
        node.parent match
            case None => None
            case Some(p) => 
                if p.id <= 0 then
                    node.children = new ArrayBuffer[Node]()
                    splitOnNode(p)
    /**
      * Divide the node into several nodes according to the size.
      */
    private def splitNode(node:Node,sz:Int):List[Node] = 
        // split current node to two nodes, then split the second recursively.
        cutNode(node,sz) match
            case (head,None) => List(head)
            case (head,Some(tail)) => List(head):::splitNode(tail,sz)
    /**
      * split node to two nodes.
      *
      * @param node
      * @param sz
      * @return
      */
    private def cutNode(node:Node,sz:Int):(Node,Option[Node]) =
        if node.size() <= sz || node.length <= Node.minKeysPerBlock*2 then 
            return (node,None)
        
        val threshold = (sz*DB.fillPercent).toInt
        var n = BlockHeader.size
        var idx = -1
        breakable(
            for i <- 0 until node.length do
                n += Node.indexSize+node.elements(i).keySize+node.elements(i).valueSize
                if n >= threshold then 
                    idx = i 
                    break()
        )
        if idx < 0 then
            return (node,None)
         
        var nodeB = new Node(new BlockHeader(-1L,node.header.flag,0,0,0))
        nodeB.elements = node.elements.slice(idx,node.elements.length)

        node.elements = node.elements.slice(0,idx)
        // if current node's parent is null, then create a new one.
        node.parent match
            case Some(p) => 
                nodeB.parent = Some(p)
                p.children+=nodeB
            case None =>
                var parent = new Node(new BlockHeader(-1L,branchType,0,0,0))
                parent.children+=node
                parent.children+=nodeB
                node.parent = Some(parent)
                nodeB.parent = Some(parent)
        (node,Some(nodeB))

    /** 
     * release pages of node. 
     * 
     */
    private def freeNode(node:Node):Unit = 
        if node.id > DB.meta1Page then
            tx.free(node.id)
            node.header.pgid = 0
            nodes.remove(node.id)

    /** 
     * release all pages of  node and its child nodes. 
     * 
     */
    private def freeFrom(id:Long):Unit = 
        if id <= 0 then return None 
        nodeOrBlock(id) match
            case (None,None) => None 
            case (Some(node),_) =>
                freeNode(node)
                if !node.isLeaf then 
                    for elem <- node.elements do
                        freeFrom(elem.child)
            case (None,Some(bk)) =>
                tx.free(bk.id)
                if bk.header.flag != leafType then 
                    nodeElements(Some(bk)) match 
                        case None => None
                        case Some(elems) =>
                            for elem <- elems do
                                freeFrom(elem.child)
        None 

    /** 
     * release all pages about current bucket. 
     *
     */
    private def freeAll():Unit =
        if bkv.root != 0 then
            freeFrom(bkv.root)
            bkv.root = 0
    /**
      * 
      *
      * @param name
      * @param dataType
      * @return
      */
    private def getBucket(name:String,dataType:Byte):Try[BTreeBucket] = 
        if tx.closed then
            return Failure(DB.exceptionTxClosed) 
        else if name.length==0 then 
            return Failure(DB.exceptionKeyIsNull)

        if buckets.contains(name) then 
            buckets.get(name) match
                case Some(bk) => return Success(bk)
                case None =>  return Failure(new Exception(s"collection cache failed,not found $name"))
        
        val dt = dataTypeName(dataType)
        var c = new BTreeBucketIter(this)
        c.search(name) match
            case (None,_,_) => return Failure(new Exception(s"not found $dt $name"))
            case (Some(k),v,f) => 
                if k!=name || f!=bucketType then 
                    return Failure(new Exception(s"not found $dt $name"))
                v match 
                    case None => Failure(new Exception(s"query $dt $name value failed"))
                    case Some(data) =>
                        val bytes = data.getBytes("ascii")
                        BTreeBucket(bytes) match
                            case None => Failure(new Exception(s"parse $dt $name value failed,expect data length is ${BTreeBucket.valueSize} but actual get ${bytes.length}")) 
                            case Some(value) =>
                                if value.dataType != dataType then
                                    return Failure(new Exception(s"already exists collection $name data type is ${dataTypeName(value.dataType)} not $dt"))
                                var bk = new BTreeBucket(name,tx)
                                bk.bkv = value 
                                buckets(name) = bk
                                Success(bk)
    /**
      * 
      *
      * @param name
      * @param dataType
      * @return
      */
    private def createBucket(name:String,dataType:Byte):Try[BTreeBucket] = 
        if tx.closed then
            return Failure(DB.exceptionTxClosed) 
        else if !tx.writable then 
            return Failure(DB.exceptionNotAllowOp) 
        else if name.length()<=0 then 
            return Failure(DB.exceptionKeyIsNull)
        else if name.length() >= DB.maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)
        else if buckets.contains(name) then 
            return Failure(new Exception(s"already exists collection $name"))
        
        val dt = dataTypeName(dataType)
        var c = new BTreeBucketIter(this)
        c.search(name) match
            //case (None,_,_) => return Failure(new Exception("bucket create failed: not found create node"))
            case (None,_,_) => None
            case (Some(k),v,f) => 
                if k == name && f != bucketType then
                    return Failure(new Exception(s"already exists key name $name")) 
                if k == name && f == bucketType then
                    v match 
                        case None => None
                        case Some(data) =>
                            BTreeBucket(data.getBytes("ascii")) match
                                case None => return Failure(new Exception(s"parse collection $name value failed"))  
                                case Some(value) => return Failure(new Exception(s"already exists ${dataTypeName(value.dataType)} collection name $name"))  
                    return Failure(new Exception(s"already exists collection name $name"))
        // create a new bucket
        var bk = new BTreeBucket(name,tx)
        bk.bkv = new BucketValue(-1,0,0,dataType) // null bkv
        bk.root = Some(new Node(new BlockHeader(-1L,leafType,0,0,0))) // null root node
        buckets(name) = bk
        c.node() match 
            case None => Failure(new Exception(s"$dt create failed: not found create node"))
            case Some(n) =>
                n.put(name,name,bk.value.toString(),bucketType,0)
                bkv.count+=1
                Success(bk)
    /**
      * 
      *
      * @param name
      * @param dataType
      * @return
      */
    private def createBucketIfNotExists(name:String,dataType:Byte):Try[BTreeBucket] =
        if tx.closed then
            return Failure(DB.exceptionTxClosed) 
        else if !tx.writable then 
            return Failure(DB.exceptionNotAllowOp)  
        else if name.length()<=0 then 
            return Failure(DB.exceptionKeyIsNull)
        else if name.length() >= DB.maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)

        getBucket(name,dataType) match
            case Success(bk) => Success(bk)
            case Failure(e) => 
                if DB.isNotExists(e) then
                    createBucket(name,dataType) 
                else 
                    Failure(e)
    /**
      * 
      *
      * @param name
      * @param dataType
      * @return
      */
    private def deleteBucket(name:String,dataType:Byte):Try[Unit] =
        if tx.closed then
            return Failure(DB.exceptionTxClosed)  
        else if !tx.writable then 
            return Failure(DB.exceptionNotAllowOp)
        else if name.length()<=0 then 
            return Failure(DB.exceptionKeyIsNull)
        else if name.length() >= DB.maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)
        
        val dt = dataTypeName(dataType)
        var c = new BTreeBucketIter(this)
        c.search(name) match 
            case (None,_,_) => return Failure(new Exception(s"not found key $name")) 
            case (Some(k),_,f) => 
                // key not exists or exists but not a bucket
                if k!=name then 
                    return Failure(new Exception(s"not exists key $name")) 
                if f!=bucketType then
                    return Failure(new Exception(s"$name is not a collection"))

                getBucket(name,dataType) match
                    case Failure(e) => return Failure(new Exception(s"query $dt $name failed:${e.getMessage()}")) 
                    case Success(bk) => 
                        // delete subbuckets recursively.
                        if dataType == bucketDataType then
                            try 
                                for (k,v) <- bk.iterator do
                                    k match
                                        case None => throw new Exception(s"query get null key in bucket ${bk.name}")
                                        case Some(key) =>
                                            v match
                                                case Some(_) => None // k is not a bucket,so do nothing for it
                                                case None => 
                                                    bk.deleteBucket(key) match
                                                        case Success(_) => None
                                                        case Failure(e) => throw e
                            catch
                                case e:Exception => return Failure(e)
                        // delete current bucket
                        buckets.remove(name) // clean cache
                        bk.nodes.clear()  // clean cache nodes
                        bk.root = None 
                        bk.freeAll() // release all pages about the bucket
                        c.node() match 
                            case None => return Failure(new Exception(s"not found $dt $name node")) 
                            case Some(node) => 
                                node.del(name) // delete bucket record from the parent node.
                                bkv.count-=1
                Success(None)

    /**
      * 
      *
      * @param name
      * @return
      */
    def getBSet(name:String):Try[BSet] = 
        getBucket(name,bsetDataType) match
            case Success(bk) => Success(new BTreeSet(bk))
            case Failure(e) => Failure(e)
    /**
      * 
      *
      * @param name
      * @return
      */
    def createBSet(name:String):Try[BSet] = 
        createBucket(name,bsetDataType) match
            case Success(bk) => Success(new BTreeSet(bk))
            case Failure(e) => Failure(e)
    /**
      * 
      *
      * @param name
      * @return
      */
    def createBSetIfNotExists(name:String):Try[BSet] = 
        createBucketIfNotExists(name,bsetDataType) match
            case Success(bk) => Success(new BTreeSet(bk))
            case Failure(e) => Failure(e)
    /**
      * 
      *
      * @param name
      * @return
      */
    def deleteBSet(name:String):Try[Unit] = deleteBucket(name,bsetDataType)

    /**
      * 
      *
      * @param name
      * @param readonly
      * @return
      */
    def getList(name:String,readonly:Boolean):Try[BList] = 
        getBucket(name,blistDataType) match
            case Failure(e) => Failure(e)
            case Success(bk) =>
                KList(bk,readonly) match
                    case Failure(e) => Failure(new Exception(s"get BList $name failed:${e.getMessage()}"))
                    case list => list      
    /**
      * 
      *
      * @param name
      * @return
      */
    def createList(name:String):Try[BList] = 
        createBucket(name,blistDataType) match
            case Failure(e) => Failure(e)
            case Success(bk) =>
                KList(bk,false) match
                    case Failure(e) => Failure(new Exception(s"create BList $name failed:${e.getMessage()}"))
                    case list => list
    /**
      * 
      *
      * @param name
      * @return
      */
    def createListIfNotExists(name:String):Try[BList] = 
        createBucketIfNotExists(name,blistDataType) match
            case Success(bk) =>
                KList(bk,false) match
                    case Failure(e) => Failure(new Exception("create BList $name failed:${e.getMessage()}"))
                    case list => list
            case Failure(e) => Failure(e)
    /**
      * 
      *
      * @param name
      * @return
      */
    def deleteList(name:String):Try[Unit] = deleteBucket(name,blistDataType)
    /**
      * 
      *
      * @return
      */
    def allCollection():Try[Seq[(String,String)]] = 
        var arr = ArrayBuffer[(String,String)]()
        try
            var iter = new BTreeBucketIter(this)
            for (k,v) <- iterator do
                k match
                    case None => None
                    case Some(key) => 
                        v match 
                            case Some(_) => None
                            case None => arr+=((key,""))
            for i <- 0 until arr.length do
                val (k,_) = arr(i)
                iter.search(k) match
                    case (None,_,_) => throw new Exception(s"not found collection info of $k")
                    case (Some(k),v,f) => 
                        if k!=name || f!=bucketType then 
                            throw new Exception(s"not found collection info of $k")
                        v match 
                            case None => throw new Exception(s"query $k value failed")
                            case Some(data) =>
                                val bytes = data.getBytes("ascii")
                                BTreeBucket(bytes) match
                                    case None => throw new Exception(s"parse $k value failed,expect data length is ${BTreeBucket.valueSize} but actual get ${bytes.length}")
                                    case Some(value) => arr(i) = (k,dataTypeName(value.dataType))
            Success(arr.toList)
        catch
            case e:Exception => Failure(e)
    //
    private var regions = Map[String,RTreeBucket]()
    /**
      * 
      *
      * @param name
      * @param readonly
      * @return
      */
    def getRegion(name:String):Try[Region] = 
        regions.get(name) match
            case None => None
            case Some(reg) => return Success(reg)
        getBucket(name,regionDataType) match
            case Failure(e) => Failure(e)
            case Success(bk) =>
                RTreeBucket(bk,tx) match
                    case Failure(e) => Failure(new Exception(s"get Region $name failed:${e.getMessage()}"))
                    case Success(reg) => 
                        regions(name) = reg
                        Success(reg)   
    /**
      * 
      *
      * @param name
      * @return
      */
    def createRegion(name:String,dimension:Int):Try[Region] = 
        createBucket(name,regionDataType) match
            case Failure(e) => Failure(e)
            case Success(bk) =>
                RTreeBucket(bk,dimension,tx) match
                    case Failure(e) => Failure(new Exception(s"create Region $name failed:${e.getMessage()}"))
                    case Success(reg) => 
                        regions(name) = reg
                        Success(reg)
    /**
      * 
      *
      * @param name
      * @return
      */
    def createRegionIfNotExists(name:String,dimension:Int):Try[Region] = 
        createBucketIfNotExists(name,regionDataType) match
            case Failure(e) => Failure(e)
            case Success(bk) =>
                RTreeBucket(bk,dimension,tx) match
                    case Failure(e) => Failure(new Exception(s"create Region $name failed:${e.getMessage()}"))
                    case Success(reg) => 
                        regions(name) = reg
                        Success(reg)
            
    /**
      * 
      *
      * @param name
      * @return
      */
    def deleteRegion(name:String):Try[Unit] =
        regions.get(name) match
            case None => None
            case Some(rbk) => 
                rbk.clear() match
                    case Failure(e) => return Failure(e)
                    case Success(_) => None
                deleteBucket(name,regionDataType) match
                    case Failure(e) => return Failure(e)
                    case Success(_) => None
                regions.remove(name)
                return Success(None)
        getBucket(name,regionDataType) match
            case Failure(e) => Failure(e)
            case Success(bk) =>
                RTreeBucket(bk,tx) match
                    case Failure(e) => Failure(new Exception(s"get Region $name failed:${e.getMessage()}"))
                    case Success(rbk) => 
                        rbk.clear() match
                            case Failure(e) => return Failure(e)
                            case Success(_) => None
                        deleteBucket(name,regionDataType) match
                            case Failure(e) => return Failure(e)
                            case Success(_) => None
                        regions.remove(name)
                        Success(None) 
        