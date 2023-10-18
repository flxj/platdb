package platdb

import java.nio.ByteBuffer
import scala.collection.mutable.{ArrayBuffer}
import scala.util.control.Breaks._

/* node storage construct on block:
+------------------+-----------------------------------------+
|     header       |                data                     |
+------------------+-----------------+------------+----------+
| block header     |    node indexs  |      node elements    |
+------------------+-----------------+------------+----------+
*
*/

/** 
  * for branch node, the valSize field will be set as pgid.
  *
  * @param flag
  * @param offset
  * @param keySize
  * @param valSize
  */
private[platdb] case class NodeIndex(offset:Int,keySize:Int,valSize:Long,flag:Byte)

/**
  * for branch node element, its value filed is null,its falg field is 0;
  * for leaf node element, its child field is -1;
  * if the element is a bucketValue then its flag field is branchType otherwise is 0.
  *
  * @param flag
  * @param child
  * @param key
  * @param value
  */
private[platdb] class NodeElement(var flag:Byte,var child:Long,var key:String,var value:String): // TODO: use Array[Byte] as key,value type
    def keySize:Int = key.getBytes.length
    def valueSize:Int = if flag!=bucketType then value.getBytes.length else value.getBytes("ascii").length
    def getValueBytes:Array[Byte] = if flag!=bucketType then value.getBytes() else value.getBytes("ascii")

// binary search
extension (arr:ArrayBuffer[NodeElement])
    def indexFunc(func:(NodeElement)=>Boolean):Int = 
        var idx = -1
        var low = 0
        var high = arr.length
        while low < high do
            val mid = (low+high)/2
            if func(arr(mid)) then
                idx = mid
                high = mid
            else
                low = mid+1
        idx

private[platdb] object Node:
    val indexSize = 17
    // parse node elements form block.
    def elements(bk:Block):Option[ArrayBuffer[NodeElement]] = 
        bk.getBytes() match
            case None => None
            case Some(data) =>
                if data.length < bk.header.count*indexSize then
                    return None
                var elems = new ArrayBuffer[NodeElement]()
                var err:Boolean = false 
                breakable( 
                    for i <- 0 until bk.header.count do
                        val idx = data.slice(indexSize*i,(i+1)*indexSize) 
                        unmashalIndex(idx) match
                            case None => 
                                err = true
                                break() 
                            case Some(ni) =>
                                val off = ni.offset - BlockHeader.size
                                val key = new String(data.slice(off,off+ni.keySize))
                                var child = ni.valSize 
                                var value:String = ""
                                if bk.header.flag == leafType then
                                    child = 0 
                                    value = new String(data.slice(off+ni.keySize,(off+ni.keySize+ni.valSize).toInt))
                                elems+=new NodeElement(ni.flag,child,key,value)
                )
                if !err then Some(elems) else None

    // convert block to node.
    def apply(bk:Block):Option[Node] = 
        var node:Option[Node] = None
        elements(bk) match
            case None => None
            case Some(elems) =>
                var n = new Node(bk.header)
                n.elements = elems 
                if n.length > 0 then
                    n.minKey = n.elements(0).key
                Some(n)

    // min elements number for node type.
    def lowerBound(ntype:Int):Int = if ntype == leafType then 1 else 2
    def isLeaf(node:Node):Boolean = node.header.flag == leafType
    def isBranch(node:Node):Boolean = node.header.flag == branchType
    def minKeysPerBlock:Int = 2
    // convert node index to bytes.
    def marshalIndex(e:NodeIndex):Array[Byte] = 
        var o = e.offset
        var k = e.keySize
        var v1 = (e.valSize >> 32).toInt
        var v2 = e.valSize
        var arr = new Array[Byte](indexSize)
        for i <- 0 to 3 do
            arr(3-i) = (o & 0xff).toByte
            arr(7-i) = (k & 0xff).toByte
            arr(11-i) = (v1 & 0xff).toByte
            arr(15-i) = (v2 & 0xff).toByte
            o = o >> 8
            k = k >> 8
            v1 = v1 >> 8
            v2 = v2 >> 8
        arr(indexSize-1) = e.flag
        arr
        
    // parse bytes to node index.
    def unmashalIndex(bs:Array[Byte]):Option[NodeIndex] =
        if bs.length != indexSize then 
            None 
        else
            val arr = for i <- 0 to 3 yield
                (bs(4*i) & 0xff) << 24 | (bs(4*i+1) & 0xff) << 16 | (bs(4*i+2) & 0xff) << 8 | (bs(4*i+3) & 0xff)
            val v = (arr(2) & 0x00000000ffffffffL) << 32 | (arr(3) & 0x00000000ffffffffL)
            Some(NodeIndex(arr(0),arr(1),v,bs(indexSize-1)))


/* Node is the representation of B+ tree nodes in memory.

Every branch node internal maintain an ordered list of elements,such as:
[(K0,P0), (K1,P1), (K2,P2), ... , (Kn,Pn)], Key satisfies K0 < K1 < K2 < ... < Kn

The element E in the Pi points subtree satisfies Ki<=E.Key<K(i+1), which means Ki is the smallest element of the Pi points subtree.

The process of retrieving the specified key on a branch node is:
Retrieve the list of elements and find the first element that satisfies Ki>=Key
i) if there is an element that meets the condition and Ki==Key, recursively enter the Pi subtree to search.
ii) if there is an element that satisfies the condition and Ki!=Key, if i>0 then recursively searches into the P(i-1) subtree; If i==0 goes into the P0 subtree to search.
iii) if there are no elements that meet the condition, place the cursor at the last element and search in the Pn subtree.

The leaf node internally maintains an ordered list of elements with key and value information, such as:
[(K0,V0,F0),(K1,V1,F1),(K2,V2,F2),...,(Kn,Vn,Fn)], where K0 < K1 < K2 < ... < Kn

Vi represents the value corresponding to the Ki element, and Fi is the flag information used to indicate the type of Vi (ordinary value or subbucket).
*/
private[platdb] class Node(var header:BlockHeader) extends Persistence:
    var unbalanced:Boolean = false
    var spilled:Boolean = false
    var minKey:String = ""
    var parent:Option[Node] = None
    var children:ArrayBuffer[Node] = new ArrayBuffer[Node]()
    var elements:ArrayBuffer[NodeElement] = new ArrayBuffer[NodeElement]()

    def id:Long = header.pgid
    def length:Int = elements.length 
    def ntype:Byte = header.flag
    def isLeaf:Boolean = header.flag == leafType
    def isBranch:Boolean = header.flag == branchType
    def isRoot:Boolean = parent match
        case Some(n) => false
        case None => true
    def root:Node = parent match {
            case Some(node:Node) => node.root
            case None => this
        }
    /**
      *  insert a element into node.
      *
      * @param oldKey: old key will be overwrited by newKey
      * @param newKey: new key
      * @param newVal: new value
      * @param flag: node element type 
      * @param child: child node id
      */ 
    def put(oldKey:String,newKey:String, newVal:String,flag:Byte,child:Long):Unit=
        if oldKey.length<=0 || newKey.length<=0 then return None 
        // 1. find insert location
        val elem:NodeElement = new NodeElement(flag,child,newKey,newVal)
        if elements.length == 0 then 
            elements+=elem 
        else 
            val idx:Int = elements.indexFunc((e:NodeElement) => e.key>=oldKey)
            if idx >= 0 then
                if elements(idx).key == oldKey then
                    elements(idx) = elem
                else 
                    elements.insert(idx,elem)
            else 
                elements+=elem
    
    /**
      * delete element from current node.
      *
      * @param key
      */
    def del(key:String):Unit =
        if key.length<=0 then return None
        val idx = elements.indexFunc((e:NodeElement) => e.key>=key)
        if idx>=0 && elements(idx).key == key then
            elements.remove(idx)
            unbalanced = true

    /**
      * delete child node from children array.
      *
      * @param node
      */
    def removeChild(node:Node):Unit =
        var idx:Int = -1 
        breakable(
            for i <- 0 until children.length do 
                if children(i).id == node.id then 
                    idx = i 
                    break()
        )
        if idx >=0 then 
            children.remove(idx)

    /**
      * The method returns the index position of the child's node in children array.
      *
      * @param node
      * @return
      */
    def childIndex(node:Node):Int =
        var idx:Int = -1
        breakable(
            for i <- 0 until elements.length do 
                if elements(i).child == node.id then 
                    idx = i
                    break()
        )
        idx
    def size():Int = 
        var dataSize:Int = BlockHeader.size+(elements.length*Node.indexSize)
        for e <- elements do
            dataSize += e.keySize +e.valueSize
        dataSize
    def writeTo(bk:Block):Int =
        if isLeaf then
            bk.header.flag = leafType
        else 
            bk.header.flag = branchType
        bk.header.count = elements.length
        bk.header.size = size()
        bk.header.overflow = (size()+DB.pageSize)/DB.pageSize - 1
        // update data.
        bk.append(bk.header.getBytes())
        var idx = bk.size
        var offset = BlockHeader.size+(elements.length*Node.indexSize)
        for e <- elements do 
            val ni = isLeaf match
                case true => NodeIndex(offset,e.keySize,e.valueSize,e.flag)
                case false => NodeIndex(offset,e.keySize,e.child,e.flag)

            bk.write(idx,Node.marshalIndex(ni))
            bk.write(offset,e.key.getBytes)
            bk.append(e.getValueBytes)
           
            idx+=Node.indexSize
            offset = bk.size
        bk.size
