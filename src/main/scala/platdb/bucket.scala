package platdb

import java.nio.ByteBuffer
import scala.collection.mutable.{Map,ArrayBuffer}
import scala.util.control.Breaks._
import scala.util.Try
import scala.util.Success
import scala.util.Failure


// count is the number of keys in current bucket
private[platdb] class bucketValue(var root:Int,var count:Int,var sequence:Long):
    def getBytes:Array[Byte] = Bucket.marshal(this)
    override def toString(): String = new String(Bucket.marshal(this))
    override def clone:bucketValue = new bucketValue(root,count,sequence)

//
private[platdb] object Bucket:
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
class Bucket(val name:String,private[platdb] var tx:Tx):
    private[platdb] var bkv:bucketValue = null
    private[platdb] var root:Option[Node] = None
    /** cache nodes about writeable tx. */
    private[platdb] var nodes:Map[Int,Node] = null
    /** cache sub-buckets */
    private[platdb] var buckets:Map[String,Bucket] = Map[String,Bucket]()

    // keys number
    def length:Int = bkv.count
    private[platdb] def value:bucketValue = bkv 
    def closed:Boolean = tx == null || tx.closed
    /**
      * 
      *
      * @return BucketIterator
      */
    def iterator():BucketIterator = new bucketIter(this)
    //private[platdb] def size():Int = Bucket.valueSize
    //private[platdb] def writeTo(bk:Block):Int = 0
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
        
        val c = iterator()
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
        else if  key.length>=maxKeySize then
            return Failure(DB.exceptionKeyTooLarge)
        else if value.length>=maxValueSize then
            return Failure(DB.exceptionValueTooLager)

        var c = new bucketIter(this)
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
            return Failure(new Exception("transaction is closed")) 
        else if !tx.writable then 
            return Failure(new Exception("readonly transaction not allow current operation")) 
        else if key.length <=0 then
            return Failure(new Exception("delete key is null")) 
        
        var c = new bucketIter(this)
        c.search(key) match 
            case (None,_,_) => return Failure(new Exception("not found value"))
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
    
    // 获取子bucket
    def getBucket(name:String):Try[Bucket] = 
        if tx.closed then
            return Failure(new Exception("transaction is closed")) 
        else if name.length==0 then 
            return Failure(new Exception("bucket name is null"))
        if buckets.contains(name) then 
            buckets.get(name) match
                case Some(bk) => return Success(bk)
                case None =>  return Failure(new Exception(s"buckets cache failed,not found $name"))

        var c = new bucketIter(this)
        c.search(name) match
            case (None,_,_) => return Failure(new Exception(s"not found bucket $name"))
            case (Some(k),v,f) => 
                if k!=name || f!=bucketType then 
                    return Failure(new Exception(s"not found bucket $name"))
                v match 
                    case None => return Failure(new Exception(s"query bucket $name value failed"))
                    case Some(data) =>
                        Bucket.read(data.getBytes()) match
                            case None => return Failure(new Exception(s"parse bucket $name value failed")) 
                            case Some(value) =>
                                var bk = new Bucket(name,tx)
                                bk.bkv = value 
                                buckets.addOne((name,bk))
                                return Success(bk)
    // 创建bucket，如果已经存在，则返回None
    def createBucket(name:String):Try[Bucket] = 
        if tx.closed then
            return Failure(new Exception("transaction is closed")) 
        else if !tx.writable then 
            return Failure(new Exception("readonly transaction not allow current operation")) 
        else if name.length()==0 then 
            return Failure(new Exception("bucket name is null"))
        else if buckets.contains(name) then 
            return Failure(new Exception(s"bucket $name is already exists"))
        
        var c = new bucketIter(this)
        c.search(name) match
            case (None,_,_) => return Failure(new Exception("bucket create failed: not found create node"))
            case (Some(k),v,f) => 
                if k==name && f!=bucketType then  // key已经存在
                    return Failure(new Exception(s"bucket create failed: key $name is already exists")) 
                if k == name && f == bucketType then // bucket已经存在
                    v match 
                        case None => None // TODO check
                        case Some(data) =>
                            Bucket.read(data.getBytes()) match
                                case None => return Failure(new Exception(s"parse bucket $name value failed"))  
                                case Some(value) =>
                                    var bk = new Bucket(name,tx)
                                    bk.bkv = value 
                                    bk.root = node(value.root)
                                    buckets.addOne((name,bk))
                    return Failure(new Exception(s"bucket create failed: bucket $name is already exists"))
        // create a new bucket
        var bk = new Bucket(name,tx)
        bk.bkv = new bucketValue(-1,0,0) // 空的bkv值
        bk.root = Some(new Node(new BlockHeader(-1,leafType,0,0,0))) // TODO: 是否直接设为None更合适？
        buckets.addOne((name,bk))
        c.node() match 
            case None => Failure(new Exception("bucket create failed: not found create node"))
            case Some(n) =>
                n.put(name,name,bk.value.toString(),bucketType,0)
                bkv.count+=1
                Success(bk)

    // 创建bucket,如果已经存在则返回该bucket
    def createBucketIfNotExists(name:String):Try[Bucket] =
        if tx.closed then
            return Failure(new Exception("transaction is closed")) 
        else if !tx.writable then 
            return Failure(new Exception("readonly transaction not allow current operation"))  
        else if name.length()<=0 then 
            return Failure(new Exception("bucket name is null"))
        
        getBucket(name) match
            case Success(bk) => Success(bk)
            case Failure(e) => createBucket(name) // TODO check if the exception is not exists
    // 删除bucket
    def deleteBucket(name:String):Try[Boolean] =
        if tx.closed then
            return Failure(new Exception("transaction is closed"))  
        else if !tx.writable then 
            return Failure(new Exception("readonly transaction not allow delete operation"))
        else if name.length()<=0 then 
            return Failure(new Exception("bucket name is null"))
        
        var c = new bucketIter(this)
        c.search(name) match 
            case (None,_,_) => return Failure(new Exception(s"not found key $name")) 
            case (Some(k),_,f) => 
                // name不存在，或者存在但不是bucket
                if k!=name then 
                    return Failure(new Exception(s"$name not exists")) 
                if f!=bucketType then
                    return Failure(new Exception(s"$name is not a bucket"))

                // 递归删除子bucket
                getBucket(name) match
                    case Failure(e) => return Failure(new Exception(s"query bucket $name failed:${e.getMessage()}")) 
                    case Success(childBk) => 
                        try 
                            for (k,v) <- childBk.iterator() do
                                k match
                                    case None => throw new Exception(s"query get null key in bucket ${childBk.name}")
                                    case Some(key) =>
                                        v match
                                            case Some(value) => None // k是个普通元素
                                            case None => 
                                                childBk.deleteBucket(key) match
                                                    case Success(_) => None
                                                    case Failure(e) => throw e
                        catch
                            case e:Exception => return Failure(e)
                        
                        buckets.remove(name) //清理缓存
                        childBk.nodes.clear()  // 清理缓存节点
                        childBk.root = None 
                        childBk.freeAll() // 释放所有节点空间
                        c.node() match 
                            case None => return Failure(new Exception(s"not found bucket $name node")) 
                            case Some(node) => 
                                node.del(name) // 从当前bucket中删除子bucket的记录 
                                bkv.count-=1
                Success(true)
    // 根据blokid尝试获取节点或者block
    private[platdb] def nodeOrBlock(id:Int):(Option[Node],Option[Block]) = 
        if nodes.contains(id) then 
            (nodes.get(id),None)
        else 
            tx.block(id) match
                case Success(bk) => (None,Some(bk))
                case Failure(_) =>  (None,None)

    // 读取block中的节点元素
    private[platdb] def nodeElements(bk:Option[Block]):Option[ArrayBuffer[NodeElement]] = 
        bk match
            case None => return None 
            case Some(block) => 
                Node.read(block) match
                    case None => return None 
                    case Some(node) => 
                        nodes.addOne(node.id,node)
                        return Some(node.elements)
    // 获取节点元素
    private[platdb] def getNodeElement(bk:Option[Block],idx:Int):Option[NodeElement] = 
        nodeElements(bk) match
            case None => None
            case Some(elems) =>
                if idx>=0 && elems.length>idx then 
                    return Some(elems(idx))
        None
    // 
    private[platdb] def getNodeByBlock(bk:Try[Block]):Option[Node] = 
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
    // get方法都是按照 缓存->磁盘 顺序
    private[platdb] def node(id:Int):Option[Node] = 
        if nodes.contains(id) then 
            return nodes.get(id)
        getNodeByBlock(tx.block(id))

    // 尝试获取节点的孩子节点
    private[platdb] def getNodeChild(n:Node,idx:Int):Option[Node] = 
        if n.isLeaf || idx<0 || idx>= n.length then
            return None
        node(n.elements(idx).child)
    // 尝试获取节点的右兄弟节点
    private[platdb] def getNodeRightSibling(node:Node):Option[Node] = 
        node.parent match
            case None => return None 
            case Some(p) =>
                val idx = p.childIndex(node)
                if idx >=0 && idx < p.length-1 then 
                    return getNodeChild(p,idx+1)
                return None  
    // 尝试获取节点的左兄弟节点
    private[platdb] def getNodeLeftSibling(node:Node):Option[Node] =
        node.parent match
            case None => return None 
            case Some(p) =>
                val idx = p.childIndex(node)
                if idx >=1 then 
                    return getNodeChild(p,idx-1)
                return None
    // rebalance 
    // 错误处理：直接抛异常
    private[platdb] def merge():Unit = 
        for (id,node) <- nodes do 
            mergeOnNode(node)
        for (name,bucket) <- buckets do 
            bucket.merge()
    //  
    // 错误处理：直接抛异常      
    private def mergeOnNode(node:Node):Unit =
        if !node.unbalanced then return None
        node.unbalanced = false 
        // 检查节点是否满足阈值
        val threshold:Int = osPageSize / 4
        if node.size() > threshold && node.length > Node.lowerBound(node.ntype) then
            return None
        
        // 当前节点是否是根节点
        node.parent match
            case None =>
                // 根节点是个分支节点，并且根节点只有一个子节点，那么直接将该孩子节点提升为根节点 （显然根节点要是个叶子节点，即使就一个元素那也不需要处理）
                if !node.isLeaf && node.length == 1 then 
                    getNodeChild(node,0) match
                        case None => None
                        case Some(child) => 
                            child.parent = None // boltdb是把子节点的内容都复制到当前节点上，然后释放子节点
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
                // 合并节点
                if p.childIndex(node) == 0 then // 当前节点是其父节点的最左节点，因此将其右兄弟节点合并到当前节点中
                    getNodeRightSibling(node) match
                        case None => return None
                        case Some(mergeFrom) => 
                            for elem <- mergeFrom.elements do 
                                if nodes.contains(elem.child) then // 如果缓存中有mergeFrom的孩子节点，那么需要把这些节点的父亲节点重置为当前节点
                                    var child = nodes(elem.child)
                                    child.parent match
                                        case Some(cp) => 
                                            cp.removeChild(child)
                                            node.children.append(child)
                                            child.parent = Some(node)
                                        case None => None   
                            // 将mergeFrom的元素移动到当前节点中
                            node.elements = node.elements ++ mergeFrom.elements
                            p.del(mergeFrom.minKey)
                            p.removeChild(mergeFrom)
                            nodes.remove(mergeFrom.id)
                            freeNode(mergeFrom)
                else
                    getNodeLeftSibling(node) match // 将当前节点合并到其左兄弟节点中
                        case None => return
                        case Some(mergeTo) => 
                            for elem <- node.elements do 
                                if nodes.contains(elem.child) then 
                                    var child = nodes(elem.child)
                                    node.removeChild(child)
                                    mergeTo.children.append(child)
                                    child.parent = Some(mergeTo)
                            // 将当前节点元素移动到mergeTo中
                            mergeTo.elements = mergeTo.elements ++ node.elements
                            p.del(node.minKey)
                            p.removeChild(node)
                            nodes.remove(node.id)
                            freeNode(node)
                // 递归处理当前节点的父节点
                mergeOnNode(p)
    // 将当前bucket切分
    private[platdb] def split():Unit =
        // 先切分缓存的子bucket
        for (name,bucket) <- buckets do 
            bucket.split() 
            // bk的根节点可能发生了变化，因此需要将新的bk root信息插入当前bk
            val v = bucket.value
            /*   
// Skip writing the bucket if there are no materialized nodes.
		if child.rootNode == nil {
			continue
		}
            */
            bucket.root match 
                case None => None
                case Some(n) =>
                    var c = new bucketIter(bucket)
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
        // 切分当前bucket
        root match
            case None => return None // Ignore if there's not a materialized root node.
            case Some(node) => splitOnNode(node)
        
        // bucket切分可能导致根节点发生变化
        root match
            case None => None
            case Some(node) =>
                var r = node.root
                if r.id >= tx.maxPageId then
                    throw new Exception(s"pgid ${r.id} above high water mark ${tx.maxPageId}")
                bkv.root = r.id
                root = Some(r)

    // 切分当前节点
    private def splitOnNode(node:Node):Unit =
        if node.spilled then return None
         // TODO: sort.Sort(n.children)
        // 递归地切分当前节点的孩子节点，注意由于切分孩子可能会在当前节点的children数组中再添加元素，而这些元素是不需要再切分的，因此此处循环使用下标来迭代
        val n = node.children.length
        for i <- 0 until n do 
            splitOnNode(node.children(i))
 
        node.children = new ArrayBuffer[Node]()
        // 切分当前节点
        for n <- splitNode(node,osPageSize) do 
            if n.id > 0 then 
                tx.free(n.id)
                n.header.pgid = 0
            // 重新分配block并写入节点内容
            val id = tx.allocate(n.size()) 
            if id >= tx.maxPageId then 
                throw new Exception(s"pgid $id above high water mark ${tx.maxPageId}")
            var bk = tx.makeBlock(id,n.size())  
            n.writeTo(bk)
            n.header = bk.header
            node.spilled = true
            // 将新切出来的节点信息插入其父亲节点中
            n.parent match
                case None => None
                case Some(p) => 
                    var k = n.minKey
                    if k.length()==0 then k = n.elements(0).key 
                    p.put(k,n.elements(0).key,"",0,n.id)
                    n.minKey = n.elements(0).key
           
        // If the root node split and created a new root then we need to spill that
	    // as well. We'll clear out the children to make sure it doesn't try to respill.
        // 新创建了一个根节点
        node.parent match
            case None => None
            case Some(p) => 
                if p.id == 0 then
                    node.children = new ArrayBuffer[Node]()
                    splitOnNode(p)

    //  将node按照size大小切分成若干节点
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
        
        var sz = blockHeaderSize
        var idx = 0
        breakable(
            for i <- 0 until node.elements.length do
                sz = sz+nodeIndexSize+node.elements(i).keySize+node.elements(i).valueSize
                if sz >= size then 
                    idx = i 
                    break()
        )
        var nodeB = new Node(new BlockHeader(0,0,0,0,0))
        nodeB.elements = node.elements.slice(idx+1,node.elements.length)
        nodeB.header.flag = node.header.flag

        node.elements = node.elements.slice(0,idx)

        // 如果当前节点的父节点是个空的，那么创建一个
        node.parent match
            case Some(p) => p.children.addOne(nodeB)
            case None =>
                var parent = new Node(new BlockHeader(0,0,0,0,0))
                parent.children.addOne(node)
                parent.children.addOne(nodeB)
                node.parent = Some(parent)
        (node,nodeB)
    // 释放该节点
    private[platdb] def freeNode(node:Node):Unit = 
        if node.id != 0 then
            tx.free(node.id)
            node.header.pgid = 0 
    // 释放该节点及其所有子节点对象对应的page
    private[platdb] def freeFrom(id:Int):Unit = 
        if id <= 0 then return None 
        nodeOrBlock(id) match
            case (None,None) => return None 
            case (Some(node),_) =>
                freeNode(node)
                if !node.isLeaf then 
                    for elem <- node.elements do // 递归释放
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
    // 释放当前bucket的所有page
    private[platdb] def freeAll():Unit =
        if bkv.root == 0 then return None 
        freeFrom(bkv.root)
        bkv.root = 0
