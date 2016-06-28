# InvertedIndex
Implementation of inverted index on top of LMDB with some interesting optimizations. 
Written for fun, work in progress. For understanding why this project was created see Main features section

##Current state
* readiness - Limited features, not 100% tests coverage and issue with LMDB, after creating new environment it cannot save new document and fails, but after restart same document is perfectly fine for it. LMDB's write speed is also potential bottleneck for write performance, but I would bet it will show good query performance. 
* tests - work in progress. Performance tests were only run on generated datasets, not real tests. But in-memory tests show that it is comparable with Lucene, and for some cases even faster. Educated guess would be - because Red-Black-Tree is faster than skip-lists. But it's hard to say for sure.

##Plans
* Multifield documents support
* Faceting


 
##Small example

```scala
        val invertedIndex = new InvertedIndex(new OkapiBM25MeasureSerializer(),
                                              new EnglishAnalyzer(),
                                              storageModeEnum.CREATE)
        val document = scala.io.Source.fromFile("testArticle.txt").mkString
        val ID = invertedIndex.addDocument(Document(document))
        invertedIndex.flush()
        // 1 - is a termID, automatic conversion of word -> stem -> termID, will be added soon
        val topLevelIterator = invertedIndex
            .getQueryEngine()
            .buildTopLevelIterator(ElementaryClause("mississippi")) //English analyzer lowercases every word

        val ret = invertedIndex.getQueryEngine().executeQuery(topLevelIterator, 10)
```



##Main features:
*  Use LMDB transaction to maintain consistant state of an index. So it will be hard to break it, or lose data.
*  Compactions are performed on per term basis, so each of those will be much smaller (a few MBs not GBs) -
   and if for some term there are no changes it's data will not be copied. 
*  Minimize compactions - as far as I understand Lucene performs compactions in order to avoid random reads. Which are slow on HDDs.
   But today we have cheap SSDs so random IO is cheap, and it makes sense to  merge blocks until SSD page size is reached.
   Current implementation uses 4KB as max size for a block - as to benefit from OS virtual memory (but it might be better to increase it to 64KB for better random IO performance).
   There are only 2 cases when compaction will be performed A) Merge small blocks of uneven sizes into 4KB blocks in a single multi-MB compaction B) Merge a lot of 4KB blocks which are at least half empty, if their combined size is larger than minCompactionSize
*  Use block's and segment's metadata to avoid decoding blocks and segments that cannot give TOP K matches.
*  Flexible storage format and pluggable analysis. Current version contains examples of using Lucene's analyzers and Okapi BM25 scoring.
   Analysis and Measure format will be closely coupled in order to avoid explosion of abstractions used to decouple them.
*  It's relatively easy to implement scoring at indexing time (BM25 without IDF can be computed and stored at indexing time, with assumption that we know avarage document size in advance)


##List of opensource software used:
*   [Lightning Memory mapped Data Base -LMDB](https://github.com/LMDB/lmdb)  
*   [Deephaks LMDB driver](https://github.com/deephacks/lmdbjni)
*   [Roaring bitmaps](https://github.com/RoaringBitmap/RoaringBitmap)
*   [JavaFastPFor](https://github.com/lemire/JavaFastPFOR)
*   [Netflix archaius](https://github.com/Netflix/archaius)
*   [Metrics from Dropwizard](http://metrics.dropwizard.io/)
*   [SLF4J](http://www.slf4j.org/)
*   [Logback](http://logback.qos.ch/)	
*   [Netflix archaius](https://github.com/Netflix/archaius)
*   [Junit](https://github.com/junit-team)
*   [Lucene 6.0.0 analyzers and for comparison](https://github.com/apache/lucene-solr)

