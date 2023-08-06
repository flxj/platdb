package platdb

import scala.collection.mutable.{Map}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

// count表示当前bucket中key的个数
class bucketValue(var root:Int,var sequence:Long,var count:Int):
    // 作为bucket类型的blockElement的value内容: NodeIndex --> (key: bucketName, value:bucketValue)
    override def toString(): String = ???

object Bucket:
    def bkValue(data:Array[Byte]):Option[bucketValue]

class Bucket(val name:String,private[platdb] var tx:Tx) extends Persistence:
    private[platdb] var bkv:bucketValue = _
    private[platdb] var root:Option[Node] = _
    private[platdb] var nodes:Map[Int,Node] = _  // 缓存的都是写事务相关的node ?
    private[platdb] var buckets:Map[String,Bucket] = _ // sub-buckets

    def value:bucketValue = bkv 
    def closed:Boolean
    def iterator():BucketIterator = new bucketIter(this)
    // 查询元素对应的值
    def get(key:String):Option[String] = 
        if key.length == 0 then return None 
        var c = iterator()
        c.find(key) match 
            case (Some(k),v) => 
                if k == key then 
                    return v 
        None 
    // 插入元素
    def put(key:String,value:String):Boolean = 
        if key.length == 0 then
            return false 
        else if !tx.writable then 
            return false 
        else if key.length() <= 0 || key.length()>= maxKeySize then 
            return false 
        else if value.length()>= maxValueSize then
            return false 

        var c = iterator()
        c.search(key) match 
            case (None,_,_) => return false 
            case (Some(k),_,f) =>
                if k == key && f == bucketType then
                    return false 
        c.current() match 
            case Some(node) =>
                if node.isLeaf then 
                    node.put(key,key,value,leafType,0)
                    bkv.count+=1
                    return true
        false 
    // 从bucket中删除某个key值，返回操作是否成功，注意如果key对应一个子bucket则删除操作被忽略
    def delete(key:String):Boolean = 
        if key.length() <=0 then
            return false 
        else if !tx.writable then 
            return false 
        
        var c = iterator()
        c.search(key) match 
            case (None,_,_) => return false  
            case (Some(k),_,f) =>
                if k == key && f == bucketType then
                    return false 
        c.current() match 
            case None => return false 
            case Some(node) =>
                if !node.isLeaf then return false 
                node.del(key) // TODO: 返回key是否存在
                bkv.count-=1
                return true
    // 获取子bucket
    def getBucket(name:String):Option[Bucket] = 
        if name.length<=0 then return None 
        if buckets.contains(name) then 
            return buckets.get(name)
        var c = iterator()
        c.search(name) match
            case (None,_,_) => return None 
            case (Some(k),v,f) => 
                if k!=name || f!=bucketType then 
                    return None 
                v match 
                    case None => return None 
                    case Some(data) =>
                        Bucket.bkValue(data.getBytes()) match
                            case None => return None 
                            case Some(value) =>
                                var bk = new Bucket(name,tx)
                                bk.value = value 
                                buckets.addOne((name,bk))
                                return Some(bk)
    // 创建bucket，如果已经存在，则返回None
    def createBucket(name:String):Option[Bucket] = 
        if name.le<=0 then 
            return None 
        else if !tx.writable then 
            return None 
        var c = iterator()
        c.search(name) match
            case (None,_,_) => _
            case (Some(k),v,f) => 
                if k==name && f!=bucketType then  // key已经存在
                    return None 
                if k == name && f == bucketType then // bucket已经存在
                    v match 
                        case None => return None 
                        case Some(data) =>
                            Bucket.bkValue(data.getBytes()) match
                                case None => return None 
                                case Some(value) =>
                                    var bk = new Bucket(name,tx)
                                    bk.value = value 
                                    bk.rootNode = node(value.root)
                                    buckets.addOne((name,bk))
                                    return Some(bk)
        // create
        var bk = new Bucket(name,tx)
        bk.bkv = new bucketValue(-1,0,0)
        bk.rootNode = Some(new Node(new BlockHeader(-1,leafType,0,0,0)))
        buckets.addOne((name,bk))
        c.node() match 
            case None => return None 
            case Some(n) =>
                n.put(name,name,bkv.toString(),bucketType,0)
                bkv.count++
        Some(bk)
    // 创建bucket,如果已经存在则返回该bucket
    def createBucketIfNotExists(name:String):Option[Bucket] =
        if closed then 
            return None 
        else if name.length()<=0 then 
            return None 
        else if !tx.writable then 
            return None 
        getBucket(name) match
            case Some(bk) => return Some(bk)
            case None => return createBucket(name)
    // 删除bucket
    def deleteBucket(name:String):Boolean =
        if closed then 
            return false 
        else if name.length()<=0 then
            return false 
        else if !tx.writable then
            return false
        var c = iterator()
        c.search(name) match 
            case (None,_,_) => return false  
            case (Some(k),_,f) => 
                if k!=name || f!=bucketType then // name不存在，或者存在但不是bucket
                    return false 
                // 递归删除子bucket
                getBucket(name) match
                    case None => _ 
                    case Some(child) => 
                        for (Some(k),v) <- child.iterator() do
                            v match
                                case Some(value) => _ 
                                case None => 
                                    if !child.deleteBucket(k) then
                                        return false
                        buckets.remove(name) //清理缓存
                        child.nodes.clear()  // 清理缓存节点
                        child.rootNode = None 
                        child.freeAll() // 释放所有节点空间
                        c.node() match 
                            case None => _
                            case Some(node) => 
                                node.del(name) // 从当前bucket中删除子bucket的记录 
                                bkv.count--      
        true
    // 根据blokid尝试获取节点或者block
    private[platdb] def nodeOrBlock(id:Int):(Option[Node],Option[Block]) = 
        if nodes.contains(id) then 
            (nodes.get(id),None)
        else 
            (None,tx.block(id))

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
            case Some(elems) =>
                if idx>=0 && elems.length>idx then 
                    return Some(elems[idx])
        None
    // 
    private[platdb] def getNodeByBlock(bk:Option[Block]):Option[Node] = 
        bk match
            case None => return None 
            case Some(block) => 
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
    private[platdb] def getNodeChild(node:Node,idx:Int):Option[Node] = 
        if node.isLeaf || idx<0 || idx>= node.length then
            return None
        node(node.elements(idx).child)
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
    // 当前bucket中key的个数
    def length:Int = bkv.count
    private[platdb] def size():Int 
    private[platdb] def block():Block // 返回该bucket对应的Block结构
    private[platdb] def writeTo(bk:Block):Int 
    
    // rebalance 
    private[platdb] def merge():Unit =
        for (id,node) <- nodes do 
            mergeOnNode(node)
        for (name,bucket) <- buckets do 
            bucket.merge()
    //        
    private def mergeOnNode(node:Node):Unit =
        if !node.unbalanced then return None 
        node.unbalanced = false 
        // 检查节点是否满足阈值
        val threshold:Int = osPageSize / 4
        if node.size > threshold && node.length > Node.lowerBound(node.ntype) then
            return
        
        // 当前节点是否是父节点
        node.parent match
            case None =>
                // 根节点是个分支节点，并且根节点只有一个子节点，那么直接将该孩子节点提升为根节点 （显然根节点要是个叶子节点，即使就一个元素那也不需要处理）
                if !node.isLeaf && node.length == 1 then 
                    getNodeChild(node,0) match
                        case Some(child) => 
                            child.parent = None // boltdb是把子节点的内容都复制到当前节点上，然后释放子节点
                            bkv.root = child.id
                            root = Some(child)
                            node.removeChild(child)
                            nodes.remove(node.id)
                            free(node)
                        case None => return 
                    return 
            case Some(p) => 
                if node.length == 0 then // If node has no keys then just remove it.
                    p.del(node.minKey)
                    p.removeChild(node)
                    nodes.remove(node.id)
                    free(node)
                    mergeOnNode(p)
                    return
                // 合并节点
                if p.childIndex(node) == 0 then // 当前节点是其父节点的最左节点，因此将其右兄弟节点合并到当前节点中
                    getNodeRightSibling(node) match
                        case None => return
                        case Some(mergeFrom) => 
                            for elem <- mergeFrom.elements do 
                                if nodes.contains(elem.child) then // 如果缓存中有mergeFrom的孩子节点，那么需要把这些节点的父亲节点重置为当前节点
                                    var child = nodes(elem.child)
                                    child.parent match
                                        case Some(cp) => 
                                            cp.removeChild(child)
                                            node.children.append(child)
                                            child.parent = Some(node)
                                        case None => _   
                            // 将mergeFrom的元素移动到当前节点中
                            node.elements = node.elements ++ mergeFrom.elements
                            p.del(mergeFrom.minKey)
                            p.removeChild(mergeFrom)
                            nodes.remove(mergeFrom.id)
                            free(mergeFrom)
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
                            free(node)
                // 递归处理当前节点的父节点
                mergeOnNode(p)
    // 将节点
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
                case None => _ 
                case Some(n) => 
                    var c = bucket.iterator()
                    c.search(name) match 
                        case (None,_,_) => throw new Exception("misplaced bucket header:")
                        case (Some(k),_,flag) =>
                            if k!=key then 
                                throw new Exception("misplaced bucket header:")
                            if flag!=bucketType then 
                                throw new Exception(s"unexpected bucket header flag:$flag")
                            c.node() match 
                                case Some(node) => node.put(k,key,v.toString(),bucketType,0)
                                case None => throw new Exception("")
        // 切分当前节点
        root match
            case None => return  // Ignore if there's not a materialized root node.
            case Some(node) => splitOnNode(node)
        
        root = Some(root.root)
        /* 
        // Update the root node for this bucket.
        if b.rootNode.pgid >= b.tx.meta.pgid {
            panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", b.rootNode.pgid, b.tx.meta.pgid))
        }
        */
        bkv.root = root.root.id
    // 切分当前节点
    private def splitOnNode(node:Node):Unit =
        if node.spilled then return None 
         // TODO: sort.Sort(n.children)
        // 递归地切分当前节点的孩子节点，注意由于切分孩子可能会在当前节点的children数组中再添加元素，而这些元素是不需要再切分的，因此此处循环使用下标来迭代
        val n = node.children.length
        for i <- 0 until n do 
            splitOnNode(node.children[i]) // TODO: 如果出错了应该返回，或者抛出异常
        
        node.children = Array[Node]()
        // 切分当前节点
        for n <- splitNode(node,osPageSize) do 
            if n.id > 0 then 
                tx.free(n.id)
                n.header.id = 0
            // 重新分配block并写入节点内容
            tx.allocate(n.size) match
                case None => return 
                case Some(id) => 
                    if id >= tx.blockId then throw new Exception(s"pgid ($id) above high water mark (${tx.blockId})")
                    tx.makeBlock(id,n.size) match 
                        case None => throw new Exception(s"allocate block failed (${id})")
                        case Some(bk) =>
                            n.writeTo(bk)
                            n.header = bk.header
                            node.spilled = true
            // 将新切出来的节点信息插入其父亲节点中
            n.parent match
                case Some(p) => 
                    var k = n.minKey
                    if k.length()==0 then k = n.elements[0].key 
                    p.put(key,n.elements[0].key,"",0,n.id)
                    n.minKey = n.elements[0].key
        /*   
        // If the root node split and created a new root then we need to spill that
	// as well. We'll clear out the children to make sure it doesn't try to respill.
    */  
        // 新创建了一个根节点
        node.parent match
            case Some(p) if p.id == 0 => 
                node.children = Array[Node]()
                splitOnNode(p)

    //  将node按照size大小切分成若干节点    
    private def splitNode(node:Node,size:Int):List[Node] = 
        if node.size <= size || node.length <= Node.minKeysPerBlock*2 then 
            return List[Node](node)
        // 将当前节点切成两个， 递归切分第二个节点
        (headNode,tailNode) := cutNode(node,size)
        headNode:::splitNode(tailNode,size)

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
            for (i,elem) <- node.elements do
                sz = sz+nodeIndexSize+elem.keySize+elem.valSize
                if sz >= size then 
                    idx = i 
                    break()
        )
        var nodeB = new Node(new BlockHeader)
        nodeB.elements = node.elements.slice(idx+1,node.elements.length)
        nodeB.header.flag = node.header.flag

        node.elements = node.elements.slice(0,idx)

        // 如果当前节点的父节点是个空的，那么创建一个
        node.parent match
            case Some(p) => 
                p.children.addOne(nodeB)
            case None =>
                var parent = new Node(new BlockHeader)
                parent.addOne(node)
                parent.addOne(nodeB)
                node.parent = Some(parent)
       (node,nodeB)

    private[platdb] def free(node:Node):Unit
    private[platdb] def freeAll():Unit // 释放该bucket对象的所有page



/*
-----------------------------------------------------
boltdb bucket插入操作：先创建一个cursor，然后调用seek方法定位到叶子节点， 在调用cursor.node().put()方法将元素插入该叶子节点
seek方法会先到bucket的nodes缓存中寻找，如果没有再到事务中寻找bucket.tx.pages(pgid)： 先找tx缓存的dirty pages, 如果没有直接从mmap中加载(tx.db.page(pgid)）
cursor().node()方法首先检查栈顶元素，如果node字段不为空并且是个叶子节点则直接返回该*node
否则尝试从栈底元素开始，自顶向下检索b+树的路径： 如果栈底底元素node字段为空，则先调用c.bucket.node(c.stack[0].page.id, nil)方法，使用pgid创建一个node对象，并将该node缓存到bucket.nodes中
```golang
// Start from root and traverse down the hierarchy.
var n = c.stack[0].node
if n == nil {
  n = c.bucket.node(c.stack[0].page.id, nil) // 返回创建的node指针
}
for _, ref := range c.stack[:len(c.stack)-1] { // 自顶向下检索的过程
  _assert(!n.isLeaf, "expected branch node")
  n = n.childAt(ref.index) // 调用n.bucket.node(n.inodes[index].pgid, n)
}
_assert(n.isLeaf, "expected leaf node")
return n
```
经过cursor.node()方法，其搜索路径上涉及的所有page都转化成了node,并缓存到bucket中 （cursor.node方法在创建节点时候会调整节点的chiledren字段）
调用node.put()方法将元素插入当前节点
bucket插入操作结束
---------------------------
bucket relabalce()操作： 先对缓存的所有node执行rebalance, 然后对所有subbucket执行rebalabce
```golang
for _, n := range b.nodes {
  n.rebalance()
}
for _, child := range b.buckets {
child.rebalance()
}
```
node的rebalance操作主要逻辑是：对标记的unbalance=true的节点，检查节点大小和元素个数，如果低于阈值，则需要和它的左右兄弟合并，因为合并操作会导致当前节点的父节点上有元素删除，因此需要递归的向上继续rebalance,直到root节点.   (搜索其左右兄弟的操作涉及从mmap加载新的page,创建新node并缓存到bucket.nodes)  两个节点合并后，对其中不再需要的一个节点调用node.free()方法释放其pgid: n.bucket.tx.db.freelist.free(n.bucket.tx.meta.txid, n.bucket.tx.page(n.pgid))
rebalance操作会调整node的chileren字段
------------------------------
bucket spill() 操作： 先spill它的所有缓存的子buckets. 然后调用b.rootNode.spill() 方法从根结点开始，将当前bucket的节点进行溢出
node.spill()会先针对children字段的节点进行递归spill, 然后再spill当前节点
spill当前节点操作会尝试将该node切分成若干nodes, 然后调用tx.freelist.free() 释放原来的pgid, 接着调用tx.allocate()为这些nodes分配新pgid，将node信息写入新分配的page, 并把对应page缓存到tx.pages中， 节点切分会向父节点elements中插入元素
另外如果在split节点时候，发现当前节点的parent为空，那末也会创建一个新节点作为parent,当前节点以及切出来的另一个节点会加入这个parent的children字段
*/

// 优化: 延迟rebalance，允许出现叶子节点高度不一致？