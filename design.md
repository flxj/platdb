

The data file is considered as a linear array composed of several fixed length (such as 4kb) data segments (called pages), each page is identified by a unique pgid, and pgid * pageSize represents the offset of the block of data relative to the starting position of the file


Pages with pgids 0 and 1 are used to store meta information of the database, while other pages are used to store user data and free page information (freelist)


The pages of user data are logically organized as a nested B+tree structure, where the root tree is used to store the meta information of the data structure object created by the user (key is the data structure name, value is the root node pgid of the structure). The data structure object is also organized as B+tree (such as bucket/bset/list) or Rtree (region), and the nodes of the tree (Node/RNode) are stored as one or more consecutive pages, and the node ID is the pgid of the first page


Freelist is organized into a list structure that records which pages are currently idle and can be used to allocate node storage space for read/write transactions


All nodes involved in read/write transactions will be assigned new pages
