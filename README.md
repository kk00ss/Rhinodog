# InvertedIndex
Implementation of inverted index on top of LMDB with a some interesting optimizations. Written for fun, work in progress.

##WHY? 
Lucene is huge and old, code quality is great, but it might be hard to extend it in ways that noone thought of before, it's code contains some assumptions made a long time ago which are not that important now,and it's huge and... did I tell you that it's huge? I'm not telling Lucene is bad - I'm just testing new ideas (at least they are new for me :-)).

##Current state
* readiness - guts outside, but components seem ready
* tests - work in progress, main.scala contains correctness and performance tests for in-memory search which is on par
          or faster than Lucene (faster when it can skip full segments without decoding based on their metadata) 


##Main features:
*  Use LMDB transaction to maintain consistant state of an index.
*  Compactions are performed on per term basis, so each of them will be smaller -
   and if for some term there are no changes it's data will not be copied. 
*  Minimize compactions - as far as I understand Lucene performs compactions in order to avoid random reads. Which are slow on HDDs.
   But today we have cheap SSDs so random IO is cheap, and it makes sense to  merge blocks until SSD page size is reached.
   Current implementation uses 4KB as max size for a block - as to benefit from OS virtual memory.
   So only 2 cases when compaction will be performed A) Merge small blocks of uneven sizes into 4KB blocks in a single multi-MB compaction B) Merge 2 4KB blocks both of which are at least half empty
*  Use block's and segment's metadata to avoid decoding blocks and segments that cannot give TOP K matches.
*  Flexible storage format and pluggable analysis
   Analysis and Measure format will be closely coupled in order to avoid explosion of abstractions used to decouple them.

