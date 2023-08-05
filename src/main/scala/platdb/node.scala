package platdb

import scala.collection.mutable.{ArrayBuffer}

val nodeIndexSize = 20 

// if isLeaf, valSize== pgid
@SerialVersionUID(100L)
case class NodeIndex(flag:Int,offset:Int,keySize:Int,valSize:Int) extends Serializable

// 对于分支节点元素，其value为空； 对于叶子节点元素其child字段为-1
case class NodeElement(flag:Int,child:Int,key:String,value:String) // 一个节点元素将转换为---> 一个NodeIndex+Ayrray[Byte] ---> 存放到block的data字段中
    def keySize:Int 
    def valueSize:Int

object NodeFactory:
    def read(bk:Block):Option[Node] = None 
    def lowerBound(ntype:Int):Int = 0
    def isLeaf(node:Node):Boolean 
    def isBranch(node:Node):Boolean
    def minKeysPerBlock:Int 
   
// node
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
    def isLeaf:Boolean = NodeFactory.isLeaf(this) 
    //
    def root:Node = parent match {
            case Some(node:Node) => node.root()
            case None => this
        }
    def size:Int = 
        var dataSize:Int = blockHeaderSize+(elements.length*blockIndexSize)
        for e <- elements do
            dataSize= dataSize+ e.key.length +e.value.length // TODO: bytes length
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
    
    def removeChild(node:Node):Unit

    // 参数为当前节点的孩子节点，该方法返回该孩子节点的索引位置
    def childIndex(node:Node):Int 

    def split():List[Node] = None // 节点的结构的都在bucket层次维护

    def block():Block // 返回该节点对应的Block对象 
    def writeTo(bk:Block):Int    