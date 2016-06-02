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

import rhinodog.Core.Definitions.BaseTraits._

object Configuration {
    // Constants
    val KB = 1024
    val MB = 1024 * KB
    val GB = 1024 * MB

    object storageModeEnum
        extends Enumeration {
        type storageMode = this.Value
        val READONLY, WRITEnREAD, CREATE = this.Value
    }

    case class StorageConfig
    (globalConfig: GlobalConfig,
     mainComponents: MainComponents,
     updateFreeSpaceRatio: Function[Float,Unit] = null)

    case class GlobalConfig
    (bitSetSegmentRange: Int = Short.MaxValue,
     //interval in milliseconds
     smallFlushInterval: Long = 1000,
     //number of small flushes per one large flush
     largeFlushInterval: Int = 5,
     // 16 bytes are for LMDB overflow pages info
     pageSize: Int = 4 * KB,
     //LMDB folder
     path: String = "storageFolder",
     maxConcurrentCompactions: Short = 4,
     //storage space is acquired in chunks of this size
     mapSizeIncreaseStep: Int = 1 * GB,
     //blockSize: Int = 256 * KB - 16, //don't need it because of partial reads
     storageMode: storageModeEnum.storageMode = storageModeEnum.CREATE,
     //delay in ms - we need it to decouple compactions from analysis threads
     merges_QueueCheckInterval: Long = 100,
     //async compactions will not start if the system CPU load is more than this
     merges_CpuLoadThreshold: Float = 0.95f,
     // compactions are only performed to reclaim space used by deleted documents
     // there is no need for further consolidation of data if it is already stored
     // in blocks of size (page size)
     // TODO: variable compaction factor based on the abount of free space
     merges_CompactionFactor: Short =  2,
     // min size of merge, larger value means merge will happen later,
     // but will affect performance of other tasks
     // value should be large enough to benefit sequential read/write performance
     // merges are expected to be quite small
     // all merges are performed on separate thread
     // NOT IN USE: for large merges there is multi-step implementation, every step is of this size
     merges_MinSize: Int = 256 * KB,
     // max size of merge
     // max size of merge doesn't mean it will be max size of a single transaction commit
     // because the later may contain many merges, currently this number is not restricted
     // same size for both means that merge will be triggered after reaching the threshold,
     // but only part of the data will go into compactionLevel1
     merges_MaxSize: Int = 256 * KB) {
        def targetBlockSize: Int = pageSize - 16 // should always be pageSize - 16
        //should be larger than max size of DocPosting in bytes
        def maxNodeUnderflow: Int = {
            val tmp = pageSize / 20
            if(tmp < 10) 10
            else tmp
        }
    }

    case class MainComponents
    (measureSerializer: MeasureSerializerBase,
     repository: RepositoryBase,
     metadataManager: MetadataManagerBase)

    case class TermWriterConfig
    (termID: Int,
     mainComponents: MainComponents,
     targetSize: Int,
     maxNodeUnderflow: Int)

}
