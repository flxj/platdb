package platdb

import java.nio.ByteBuffer
import scala.collection.mutable.{Map,ArrayBuffer}
import scala.util.control.Breaks._
import scala.util.Try
import scala.util.Success
import scala.util.Failure

trait Bucket:
    def name:String
    def length:Int
    def closed:Boolean
    def iterator:BucketIterator
    def contains(key:String):Try[Boolean]
    def get(key:String):Try[String]
    def put(key:String,value:String):Try[Boolean]
    def delete(key:String):Try[Boolean]
    def getBucket(name:String):Try[Bucket]
    def createBucket(name:String):Try[Bucket]
    def createBucketIfNotExists(name:String):Try[Bucket] 
    def deleteBucket(name:String):Try[Boolean]

// count is the number of keys in current bucket
private[platdb] class bucketValue(var root:Int,var count:Int,var sequence:Long):
    def getBytes:Array[Byte] = BTreeBucket.marshal(this)
    override def toString(): String = new String(BTreeBucket.marshal(this))
    override def clone:bucketValue = new bucketValue(root,count,sequence)

//
private[platdb] object BTreeBucket:
    /** bucket value size when convert byte array.  */
    val valueSize:Int = 16
    //
    def read(data:Array[Byte]):Option[bucketValue] =
        if data.length!=valueSize then
            return None 
        var r = 0
        var c = 0
        var s:Long = 0
        for i <- 0 to 3 do
            r = r << 8
            r = r | (data(i) & 0xff)
            c = c << 8
            c = c | (data(4+i) & 0xff)
        for i <- 0 to 7 do
            s = s << 8
            s = s | (data(8+i) & 0xff)
        Some(new bucketValue(r,c,s))
    // convert bucket value to byte array
    def marshal(bkv:bucketValue):Array[Byte] = 
        var buf:ByteBuffer = ByteBuffer.allocate(valueSize)
        buf.putInt(bkv.root)
        buf.putInt(bkv.count)
        buf.putLong(bkv.sequence)
        buf.array()

//
private[platdb] class BTreeBucket(val bkname:String,var tx:Tx) extends Bucket:
    var bkv:bucketValue = null
    var root:Option[Node] = None
    /** cache nodes about writeable tx. */
    var nodes:Map[Int,Node] = null
    /** cache sub-buckets */
    var buckets:Map[String,BTreeBucket] = Map[String,BTreeBucket]()

    // keys number
    def name:String = bkname 
    def length:Int = bkv.count
    def value:bucketValue = bkv 
    def closed:Boolean = tx == null || tx.closed
    /**
      * 
      *
      * @return BucketIterator
      */
    def iterator:BucketIterator = new btreeBucketIter(this)
    def contains(key:String):Try[Boolean] = Failure(throw new Exception("Not implement now"))
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
      * put method insert or update(overwritten) the value for a key in the bucket.
      * Put operation will failed if the key is null or too large, or the value is too large.
      * If the bucket was managed by a readonly transaction, not allow put operation on it.
      * @param key
      * @param value
      * @return success flag
      */
    def put(key:String,value:String):Try[Boolean] =
        if tx.closed then
            return Failure(DB.exceptionTxClosed) 
        else if !tx.writable then 
            return Failure(DB.exceptionOpNotAllow) 
        else if key.length == 0 then
            return Failure(DB.exceptionKeyIsNull)
        else if  key.length>=DB.maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)
        else if value.length>=DB.maxValueSize then
            return Failure(DB.exceptionValueTooLarge)

        var c = new btreeBucketIter(this)
        c.search(key) match 
            case (None,_,_) => return Failure(DB.exceptionValueNotFound)
            case (Some(k),_,f) =>
                if k == key && f == bucketType then
                    return Failure(new Exception("the value is subbucket,not allow update it by put method"))
        c.node() match 
            case None => None
            case Some(node) =>
                if node.isLeaf then 
                    node.put(key,key,value,leafType,0)
                    bkv.count+=1
                    return Success(true)
        Failure(new Exception(s"not found insert node for key:$key")) 
    /**
      * try to remove a key from the bucket.
      * delete operation will be ignore if the key does not exist.
      * If the bucket was managed by a readonly transaction, not allow put operation on it.
      * @param key
      * @return success flag
      */
    def delete(key:String):Try[Boolean] = 
        if tx.closed then
            return Failure(DB.exceptionTxClosed) 
        else if !tx.writable then 
            return Failure(DB.exceptionOpNotAllow) 
        else if key.length <=0 then
            return Failure(DB.exceptionKeyIsNull) 
        
        var c = new btreeBucketIter(this)
        c.search(key) match 
            case (None,_,_) => return Failure(DB.exceptionValueNotFound)
            case (Some(k),_,f) =>
                if k == key && f == bucketType then
                    return Failure(new Exception("not allow delete subbucket value by delete method")) 
        c.node() match 
            case None => None
            case Some(node) =>
                if node.isLeaf then
                    node.del(key) 
                    bkv.count-=1
                    buckets.remove(key)
                    return Success(true)
        Failure(new Exception("not found delete object"))
    
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

        var c = new btreeBucketIter(this)
        c.search(name) match
            case (None,_,_) => return Failure(new Exception(s"not found bucket $name"))
            case (Some(k),v,f) => 
                if k!=name || f!=bucketType then 
                    return Failure(new Exception(s"not found bucket $name"))
                v match 
                    case None => return Failure(new Exception(s"query bucket $name value failed"))
                    case Some(data) =>
                        BTreeBucket.read(data.getBytes()) match
                            case None => return Failure(new Exception(s"parse bucket $name value failed")) 
                            case Some(value) =>
                                var bk = new BTreeBucket(name,tx)
                                bk.bkv = value 
                                buckets.addOne((name,bk))
                                return Success(bk)
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
            return Failure(DB.exceptionOpNotAllow) 
        else if name.length()<=0 then 
            return Failure(DB.exceptionKeyIsNull)
        else if name.length() >= DB.maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)
        else if buckets.contains(name) then 
            return Failure(new Exception(s"bucket $name is already exists"))
        
        var c = new btreeBucketIter(this)
        c.search(name) match
            case (None,_,_) => return Failure(new Exception("bucket create failed: not found create node"))
            case (Some(k),v,f) => 
                if k==name && f!=bucketType then
                    return Failure(new Exception(s"bucket create failed: key $name is already exists")) 
                if k == name && f == bucketType then
                    v match 
                        case None => None
                        case Some(data) =>
                            BTreeBucket.read(data.getBytes()) match
                                case None => return Failure(new Exception(s"parse bucket $name value failed"))  
                                case Some(value) =>
                                    var bk = new BTreeBucket(name,tx)
                                    bk.bkv = value 
                                    bk.root = node(value.root)
                                    buckets.addOne((name,bk))
                    return Failure(new Exception(s"bucket create failed: bucket $name is already exists"))
        // create a new bucket
        var bk = new BTreeBucket(name,tx)
        bk.bkv = new bucketValue(-1,0,0) // null bkv
        bk.root = Some(new Node(new BlockHeader(-1,leafType,0,0,0))) // null root node
        buckets.addOne((name,bk))
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
            return Failure(DB.exceptionOpNotAllow)  
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
    def deleteBucket(name:String):Try[Boolean] =
        if tx.closed then
            return Failure(DB.exceptionTxClosed)  
        else if !tx.writable then 
            return Failure(DB.exceptionOpNotAllow)
        else if name.length()<=0 then 
            return Failure(DB.exceptionKeyIsNull)
        else if name.length() >= DB.maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)
        
        var c = new btreeBucketIter(this)
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
                                node.del(name) // delete bucket record from the node
                                bkv.count-=1
                Success(true)
    /** try to get node or block by block id. */
    def nodeOrBlock(id:Int):(Option[Node],Option[Block]) = 
        if nodes.contains(id) then 
            (nodes.get(id),None)
        else 
            tx.block(id) match
                case Success(bk) => (None,Some(bk))
                case Failure(_) =>  (None,None)

    /** parse node elements info from block */
    def nodeElements(bk:Option[Block]):Option[ArrayBuffer[NodeElement]] = 
        bk match
            case None => return None 
            case Some(block) => 
                Node.read(block) match
                    case None => return None 
                    case Some(node) => 
                        nodes.addOne(node.id,node)
                        return Some(node.elements)
    /** try to get the idx node element from the block. */
    def getNodeElement(bk:Option[Block],idx:Int):Option[NodeElement] = 
        nodeElements(bk) match
            case None => None
            case Some(elems) =>
                if idx>=0 && elems.length>idx then 
                    return Some(elems(idx))
        None
    /** convert block to node. */
    def getNodeByBlock(bk:Try[Block]):Option[Node] = 
        bk match
            case Failure(_) => return None 
            case Success(block) => 
                if nodes.contains(block.id) then 
                    return nodes.get(block.id)
                Node.read(block) match
                    case None => return None 
                    case Some(node) => 
                        nodes.addOne((node.id,node))
                        return Some(node) 
    /** get node by id, search from cache -> disk. */
    def node(id:Int):Option[Node] = 
        if nodes.contains(id) then 
            return nodes.get(id)
        getNodeByBlock(tx.block(id))
    /** try to get a child node by index. */
    def getNodeChild(n:Node,idx:Int):Option[Node] = 
        if n.isLeaf || idx<0 || idx>= n.length then
            return None
        node(n.elements(idx).child)
    /** try to get right brother node. */
    private def getNodeRightSibling(node:Node):Option[Node] = 
        node.parent match
            case None => return None 
            case Some(p) =>
                val idx = p.childIndex(node)
                if idx >=0 && idx < p.length-1 then 
                    return getNodeChild(p,idx+1)
                return None 
    /** try to get node left brother node. */
    private def getNodeLeftSibling(node:Node):Option[Node] =
        node.parent match
            case None => return None 
            case Some(p) =>
                val idx = p.childIndex(node)
                if idx >=1 then 
                    return getNodeChild(p,idx-1)
                return None
    /**
     * rebalance the bucket,merge some small nodes.
     */
    def merge():Unit = 
        for (_,node) <- nodes do 
            mergeOnNode(node)
        for (_,bucket) <- buckets do 
            bucket.merge() 
    /**
     * try to merge the node.
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
                    getNodeChild(node,0) match
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
                                    node.children.append(child)
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
                                    mergeTo.children.append(child)
                                    child.parent = Some(mergeTo)
                            // remove all elements from current node to mergeTo.
                            mergeTo.elements  ++= node.elements
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
        // split all cache subbuckets.
        for (name,bucket) <- buckets do 
            bucket.split()
            // split operation maybe change the sub-bucket's root,so need update the newest root info in current bucket.
            val v = bucket.value
            bucket.root match 
                case None => None // Skip writing the bucket if there are no materialized nodes.
                case Some(n) =>
                    var c = new btreeBucketIter(bucket)
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
      */
    private def splitOnNode(node:Node):Unit =
        if node.spilled then return None
         // TODO: sort.Sort(n.children)
        // Recursively slice the children of the current node. 
        // Note that child node splitting may add more elements to the current node's children array, 
        // and these new elements do not need to be repartitioned, so subscripts are recycled here.
        val n = node.children.length
        for i <- 0 until n do 
            splitOnNode(node.children(i))
        
        // We no longer need the child list because it's only used for spill tracking.
        node.children = new ArrayBuffer[Node]()
        // split current node.
        for n <- splitNode(node,osPageSize) do 
            if n.id > 0 then 
                tx.free(n.id)
                n.header.pgid = 0
            // allocate a new block and write node content to it.
            val id = tx.allocate(n.size()) 
            if id >= tx.maxPageId then 
                throw new Exception(s"pgid $id above high water mark ${tx.maxPageId}")
            var bk = tx.makeBlock(id,n.size())  
            n.writeTo(bk)
            n.header = bk.header
            node.spilled = true
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
                if p.id == 0 then
                    node.children = new ArrayBuffer[Node]()
                    splitOnNode(p)
    /**
      * Divide the node into several nodes according to the size.
      */
    private def splitNode(node:Node,size:Int):List[Node] = 
        if node.size() <= size || node.length <= Node.minKeysPerBlock*2 then 
            return List[Node](node)
        // 将当前节点切成两个， 递归切分第二个节点
        var (headNode,tailNode) = cutNode(node,size)
        var listn = List(headNode)
        listn:::splitNode(tailNode,size)

    private def cutNode(node:Node,size:Int):(Node,Node) =
        /* 
        sz = pageHeaderSize

	// Loop until we only have the minimum number of keys required for the second page.
	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = uintptr(i)
		inode := n.inodes[i]
		elsize := n.pageElementSize() + uintptr(len(inode.key)) + uintptr(len(inode.value))

		// If we have at least the minimum number of keys and adding another
		// node would put us over the threshold then exit and return.
		if index >= minKeysPerPage && sz+elsize > uintptr(threshold) {
			break
		}

		// Add the element size to the total size.
		sz += elsize
	} 
        */
        
        var sz = Block.headerSize
        var idx = 0
        breakable(
            for i <- 0 until node.elements.length do
                sz = sz+Node.indexSize+node.elements(i).keySize+node.elements(i).valueSize
                if sz >= size then 
                    idx = i 
                    break()
        )
        var nodeB = new Node(new BlockHeader(0,0,0,0,0))
        nodeB.elements = node.elements.slice(idx+1,node.elements.length)
        nodeB.header.flag = node.header.flag

        node.elements = node.elements.slice(0,idx)

        // if current node's parent is null, then create a new one.
        node.parent match
            case Some(p) => p.children.addOne(nodeB)
            case None =>
                var parent = new Node(new BlockHeader(0,0,0,0,0))
                parent.children.addOne(node)
                parent.children.addOne(nodeB)
                node.parent = Some(parent)
        (node,nodeB)

    /** release pages of node. */
    private def freeNode(node:Node):Unit = 
        if node.id > 1 then
            tx.free(node.id)
            node.header.pgid = -1

    /** release all pages of  node and its child nodes. */
    private def freeFrom(id:Int):Unit = 
        if id <= 0 then return None 
        nodeOrBlock(id) match
            case (None,None) => return None 
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

    /** release all pages about current bucket. */
    private def freeAll():Unit =
        if bkv.root == 0 then return None 
        freeFrom(bkv.root)
        bkv.root = 0
