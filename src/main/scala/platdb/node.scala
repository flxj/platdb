package platdb

import scala.collection.mutable.{ArrayBuffer}

val nodeIndexSize = 20 

// if isLeaf, valSize== pgid
case class NodeIndex(flag:Int,offset:Int,keySize:Int,valSize:Int)

// 对于分支节点元素，其value为空； 对于叶子节点元素其child字段为-1
class NodeElement(flag:Int,child:Int,key:String,value:String) // 一个节点元素将转换为---> 一个NodeIndex+Ayrray[Byte] ---> 存放到block的data字段中
    def keySize:Int = key.getBytes.length
    def valueSize:Int = value.getBytes.length

object Node:
    def read(bk:Block):Option[Node] = 
        var node = new Node(bk.header)
        node.elements = ArrayBuffer[NodeElement]()

        val data = bk.data
        var offset = blockHeaderSize
        for i <- 0 until bk.header.count do 
            val idx = data.slice(offset,offset+nodeIndexSize) 
            unmashalIndex(idx) match
                case None => return None 
                case Some(ni) =>
                    var child = ni.valSize 
                    val key = data.slice(ni.offset,ni.offset+ni.keySize).toString()
                    var value:String = ""
                    if isLeaf(node) then
                        child = 0 
                        value = data.slice(ni.offset+ni.keySize,ni.offset+ni.keySize+ni.valSize).toString()
                    node.elements.addOne(new NodeElement(ni.flag,child,key,value))
        if node.length >0 then 
            node.minKey = node.elements[0].key
        Some(node)
    // 
    def lowerBound(ntype:Int):Int = 0
    def isLeaf(node:Node):Boolean = node.header.flag == leafType
    def isBranch(node:Node):Boolean = node.header.flag == branchType
    def minKeysPerBlock:Int 
    // 
    def marshalIndex(e:NodeIndex):Array[Byte] = 
        var buf:ByteBuffer = ByteBuffer.allocate(nodeIndexSize)
        buf.putInt(e.flag)
        buf.putInt(e.offset)
        buf.putInt(e.keySize)
        buf.putInt(e.valSize)
        buf.array()
    // 
    def unmashalIndex(bs:Array[Byte]):Option[NodeIndex] =
        if bs.length != nodeIndexSize then 
            None 
        else
            var arr = new Array[Int](nodeIndexSize/4)
            var i = 0
            while i<nodeIndexSize/4 do 
                var n = 0
                for j <- 0 to 3 do
                    n = n << 8
                    n = n | (bs(4*i+j) & 0xff)
                arr(i) = n
                i = i+1
            Some(new NodeIndex(arr(0),arr(1),arr(2),arr(3)))

// node作为b+树节点的内存表示
private[platdb] class Node(var header:BlockHeader) extends Persistence:
    var unbalanced:Boolean = _
    var spilled:Boolean = _
    var minKey:String = _
    var parent:Option[Node] = _ 
    var children:ArrayBuffer[Node] = _
    var elements:ArrayBuffer[NodeElement] = _

    def id:Int = header.id
    def length:Int = elements.length 
    // 
    def ntype:Int = header.flag
    // 
    def isLeaf:Boolean = header.flag == leafType
    //
    def root:Node = parent match {
            case Some(node:Node) => node.root()
            case None => this
        }

    // 包括header在内的节点大小
    def size:Int = 
        var dataSize:Int = blockHeaderSize+(elements.length*nodeIndexSize)
        for e <- elements do
            dataSize= dataSize+ e.keySize +e.valueSzie
        dataSize 
    
    //  insert a element into node. 
    def put(oldKey:String,newKey:String, newVal:String,flag:Int,child:Int):Unit=
        if oldKey.length<=0 || newKey.length<=0 then None 
        // 1. find insert location
        val elem:NodeElement = new NodeElement(flag,child,newKey,newVal)
        if elements.length == 0 then 
            elements+= elem 
        else 
            val idx:Int = elements.indexWhere((e:NodeElement) => e.key>=oldKey) // TODO ensure indexWhere method
            if idx <= elements.length-1 &&  elements[idx].key == oldKey then
                elements(idx) = elem 
            else 
                elements.insert(idx,elem)
    //  TODO
    def del(key:String):Unit =
        if key.length<=0 then return
        idx:= elements.indexWhere((e:NodeElement) => e.key>=oldKey)
        if idx>=0 && idx<elements.length && elements[idx].key == key then 
            // remove the element
            elements = elements.slice(0,idx)++elements.slice(idx+1,elements.length)
            unbalanced = true
    
    def removeChild(node:Node):Unit =
        var idx:Int = -1 
        for (i,child) <- children do 
            if child.id == node.id then 
                idx = i 
        if idx >=0 then 
            children = children.slice(0,idx)++children.slice(idx+1,children.length)

    // 参数为当前节点的孩子节点，该方法返回该孩子节点的索引位置
    def childIndex(node:Node):Int =
        for (i,elem) <- elements do 
            if elem.child == node.id then 
                return i
        return -1 
    // 
    def block():Block // 返回该节点对应的Block对象 
    // 将当前节点内容写入参数block
    def writeTo(bk:Block):Int =
        // 更新header
        if isLeaf then 
            bk.header.flag = leafType
        else 
            bk.header.flag = branchType
        bk.count = elements.length
        bk.header.size = size
        bk.header.overflow = (size+osPageSize)/osPageSize - 1
        // 更新data字段
        bk.append(Block.marshalHeader(bk.header))
        var idx = bk.size
        var offset = blockHeaderSize+(elements.length*nodeIndexSize)
        for (i,elem) <- elements do 
            val ni = isLeaf match
                case true => new NodeIndex(elem.flag,offset,elem.keySize,elem.valueSize)
                case false => new NodeIndex(elem.flag,offset,elem.keySize,elem.child)

            bk.write(idx,Node.marshalIndex(ni))
            bk.write(offset,elem.key.getBytes)
            bk.append(elem.value.getBytes)
           
            idx = idx+nodeIndexSize
            offset = bk.size
        bk.size



        
        