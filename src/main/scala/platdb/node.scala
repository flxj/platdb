package platdb

import java.nio.ByteBuffer
import scala.collection.mutable.{ArrayBuffer}
import scala.util.control.Breaks._

/* node block construct:
+------------------+-----------------------------------------+
|     header       |                data                     |
+------------------+-----------------+------------+----------+
| block header     |    node indexs  |      node elements    |
+------------------+-----------------+------------+----------+
*
* 一个节点元素将被转换为一个NodeIndex和一个Ayrray[Byte],其中NodeIndex存放到block的indexs区,array存放到elements区
*/

// for branch node, the valSize field will be set as pgid.
private[platdb] case class NodeIndex(flag:Int,offset:Int,keySize:Int,valSize:Int)

// for branch node element, its value filed is null,its falg field is 0;
// for leaf node element, its child field is -1;
// if the element is a bucketValue then its flag field is branchType otherwise is 0.
private[platdb] class NodeElement(var flag:Int,var child:Int,var key:String,var value:String):
    def keySize:Int = key.getBytes.length
    def valueSize:Int = value.getBytes.length // TODO: 使用Array[Byte] 作为key,value字段类型

private[platdb] object Node:
    val indexSize = 16
    def read(bk:Block):Option[Node] = 
        var node = new Node(bk.header)
        node.elements = new ArrayBuffer[NodeElement]()

        bk.getBytes() match
            case None => return None
            case Some(data) =>
                if data.length < bk.header.count*Node.indexSize then
                    return None
                var offset = 0
                breakable( 
                    for i <- 0 until bk.header.count do 
                        val idx = data.slice(offset,offset+indexSize) 
                        unmashalIndex(idx) match
                            case None => 
                                node = null
                                break() 
                            case Some(ni) =>
                                var child = ni.valSize 
                                val key = data.slice(ni.offset,ni.offset+ni.keySize).toString()
                                var value:String = ""
                                if isLeaf(node) then
                                    child = 0 
                                    value = data.slice(ni.offset+ni.keySize,ni.offset+ni.keySize+ni.valSize).toString()
                                node.elements.addOne(new NodeElement(ni.flag,child,key,value))
                )
        if node!=null then
            if node.length >0 then 
                node.minKey = node.elements(0).key
            Some(node)
        else
            None
    // min elements number for node type.
    def lowerBound(ntype:Int):Int = if ntype == leafType then 1 else 2
    def isLeaf(node:Node):Boolean = node.header.flag == leafType
    def isBranch(node:Node):Boolean = node.header.flag == branchType
    def minKeysPerBlock:Int = 2
    // convert node index to bytes.
    def marshalIndex(e:NodeIndex):Array[Byte] = 
        var buf:ByteBuffer = ByteBuffer.allocate(indexSize)
        buf.putInt(e.flag)
        buf.putInt(e.offset)
        buf.putInt(e.keySize)
        buf.putInt(e.valSize)
        buf.array()
    // parse bytes to node index.
    def unmashalIndex(bs:Array[Byte]):Option[NodeIndex] =
        if bs.length != indexSize then 
            None 
        else
            var arr = new Array[Int](indexSize/4)
            var i = 0
            while i<indexSize/4 do 
                var n = 0
                for j <- 0 to 3 do
                    n = n << 8
                    n = n | (bs(4*i+j) & 0xff)
                arr(i) = n
                i = i+1
            Some(new NodeIndex(arr(0),arr(1),arr(2),arr(3)))

// node is the representation of B+ tree nodes in memory.
private[platdb] class Node(var header:BlockHeader) extends Persistence:
    var unbalanced:Boolean = false
    var spilled:Boolean = false
    var minKey:String = ""
    var parent:Option[Node] = None
    var children:ArrayBuffer[Node] = new ArrayBuffer[Node]()
    var elements:ArrayBuffer[NodeElement] = new ArrayBuffer[NodeElement]()

    def id:Int = header.pgid
    def length:Int = elements.length 
    def ntype:Int = header.flag
    def isLeaf:Boolean = header.flag == leafType
    def isBranch:Boolean = header.flag == branchType
    def isRoot:Boolean = parent match
        case Some(n) => false
        case None => true
    def root:Node = parent match {
            case Some(node:Node) => node.root
            case None => this
        }
    def size():Int = 
        var dataSize:Int = Block.headerSize+(elements.length*Node.indexSize)
        for e <- elements do
            dataSize= dataSize+ e.keySize +e.valueSize
        dataSize 
    
    /**
      *  insert a element into node.
      *
      * @param oldKey: old key will be overwrited by newKey
      * @param newKey: new key
      * @param newVal: new value
      * @param flag: node element type 
      * @param child: child node id
      */ 
    def put(oldKey:String,newKey:String, newVal:String,flag:Int,child:Int):Unit=
        if oldKey.length<=0 || newKey.length<=0 then return None 
        // 1. find insert location
        val elem:NodeElement = new NodeElement(flag,child,newKey,newVal)
        if elements.length == 0 then 
            elements+=elem 
        else 
            val idx:Int = elements.indexWhere((e:NodeElement) => e.key>=oldKey) // TODO ensure indexWhere method
            if idx <= elements.length-1 &&  elements(idx).key == oldKey then
                elements(idx) = elem 
            else 
                elements.insert(idx,elem)
    
    /** delete element from current node.*/
    def del(key:String):Unit =
        if key.length<=0 then return None
        val idx = elements.indexWhere((e:NodeElement) => e.key>=key)
        if idx>=0 && idx<elements.length && elements(idx).key == key then
            elements = elements.slice(0,idx)++elements.slice(idx+1,elements.length)
            unbalanced = true

    /** delete child node from children array. */
    def removeChild(node:Node):Unit =
        var idx:Int = -1 
        breakable(
            for i <- 0 until children.length do 
                if children(i).id == node.id then 
                    idx = i 
                    break()
        )
        if idx >=0 then 
            children = children.slice(0,idx)++children.slice(idx+1,children.length)

    /** The method returns the index position of the child's node in children array. */
    def childIndex(node:Node):Int =
        var idx:Int = -1
        breakable(
            for i <- 0 until elements.length do 
                if elements(i).child == node.id then 
                    idx = i
                    break()
        )
        idx
    def writeTo(bk:Block):Int =
        // update header.
        if isLeaf then 
            bk.header.flag = leafType
        else 
            bk.header.flag = branchType
        bk.header.count = elements.length
        bk.header.size = size()
        bk.header.overflow = (size()+DB.pageSize)/DB.pageSize - 1
        // update data.
        bk.append(Block.marshalHeader(bk.header))
        var idx = bk.size
        var offset = Block.headerSize+(elements.length*Node.indexSize)
        for elem <- elements do 
            val ni = isLeaf match
                case true => new NodeIndex(elem.flag,offset,elem.keySize,elem.valueSize)
                case false => new NodeIndex(elem.flag,offset,elem.keySize,elem.child)

            bk.write(idx,Node.marshalIndex(ni))
            bk.write(offset,elem.key.getBytes)
            bk.append(elem.value.getBytes)
           
            idx+=Node.indexSize
            offset = bk.size
        bk.size



        
        