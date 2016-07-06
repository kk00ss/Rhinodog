// Copyright 2016 Konstantin Voznesenskiy

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rhinodog.Core.Definitions

import java.util.concurrent._

object Caching {

    case class BlockCache(key: BlockKey,
                          block: BlockDataSerialized,
                          metadata: BlockMetadataSerialized) extends Ordered[BlockCache] {
        def compare(that: BlockCache) = this.key.compare(that.key)
    }

    case class DocsChangesCache
    (addedDocuments: ConcurrentSkipListMap[Long, DocumentSerialized],
     deletedDocuments: ConcurrentSkipListSet[Long]) {
        def this() = this(new ConcurrentSkipListMap(), new ConcurrentSkipListSet())
    }
    case class WriteCache
    (addedBlocks: ConcurrentSkipListMap[BlockKey, (BlockDataSerialized, BlockMetadataSerialized)],
     newTerms: ConcurrentHashMap[String, Int],
     newTermsByID: ConcurrentHashMap[Int, String],
     deletedBlocks: ConcurrentSkipListSet[BlockKey],
     docs: DocsChangesCache) {
        def this() = this(
            new ConcurrentSkipListMap[BlockKey,
                        (BlockDataSerialized, BlockMetadataSerialized)](),
            new ConcurrentHashMap[String, Int](),
            new ConcurrentHashMap[Int, String],
            new ConcurrentSkipListSet[BlockKey](),
            new DocsChangesCache())
    }
}
