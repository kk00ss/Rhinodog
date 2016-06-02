package rhinodog.Core.Definitions

import java.util.concurrent._

object Caching {

    case class BlockCache(key: BlockKey,
                          block: BlockDataSerialized,
                          metadata: BlockMetadataSerialized)

    case class DocsChangesCache
    (addedDocuments: ConcurrentSkipListMap[Long, DocumentSerialized],
     deletedDocuments: ConcurrentSkipListSet[Long]) {
        def this() = this(new ConcurrentSkipListMap(), new ConcurrentSkipListSet())
    }
    case class WriteCache
    (addedBlocks: ConcurrentSkipListMap[BlockKey, BlockDataSerialized],
     addedMeta: ConcurrentSkipListMap[BlockKey, BlockMetadataSerialized],
     newTerms: ConcurrentHashMap[String, Int],
     deletedBlocks: ConcurrentSkipListSet[BlockKey],
     docs:DocsChangesCache,
     partialFlushInfosToAdd: ConcurrentSkipListMap[Array[Byte], Array[Byte]],
     partialFlushInfosToDELETE: ConcurrentLinkedQueue[Array[Byte]]) {
        def this() = this(
            new ConcurrentSkipListMap[BlockKey, BlockDataSerialized](),
            new ConcurrentSkipListMap[BlockKey, BlockMetadataSerialized](),
            new ConcurrentHashMap[String, Int](),
            new ConcurrentSkipListSet[BlockKey](),
            new DocsChangesCache(),
            new ConcurrentSkipListMap[Array[Byte], Array[Byte]],
            new ConcurrentLinkedQueue[Array[Byte]]())
    }
}
