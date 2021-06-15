package com.jayden.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Index instance, same as elasticsearch shard.
 *
 * @author jayden
 * @date 6/15/21
 */
public class IndexInstance {

    private static final Logger logger = LoggerFactory.getLogger(IndexInstance.class);

    private String indexName;
    private String path;
    private IndexWriter indexWriter;
    private SearcherManager searcherManager;

    public IndexInstance(String indexName,
                         String path) {
        this.indexName = indexName;
        this.path = path;
        File f = new File(path);
        if (!f.exists()) {
            logger.info("create folder {}.", path);
            if (f.mkdir()) {
                logger.info("create folder {} succeed.", path);
            } else {
                throw new IllegalArgumentException("can not create folder");
            }
        }
    }

    /**
     * Open a lucene index writer
     * @throws IOException something error
     */
    public void openIndex() throws IOException {
        if (isOpen()) {
            logger.info("index {} already been opened.", indexName);
            return;
        }
        Path indexFolder = Paths.get(path);
        FSDirectory luceneDir = MMapDirectory.open(indexFolder);
        IndexWriterConfig config = new IndexWriterConfig();
        config.setCommitOnClose(true);
        config.setRAMBufferSizeMB(512.0f);

        //TODO config
        ConcurrentMergeScheduler mergeScheduler = (ConcurrentMergeScheduler) config.getMergeScheduler();
        mergeScheduler.setMaxMergesAndThreads(5, 5);

        mergeScheduler.disableAutoIOThrottle();
        config.setMergeScheduler(mergeScheduler);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        indexWriter = new IndexWriter(luceneDir, config);
        searcherManager = new SearcherManager(indexWriter, null);
    }

    public void close() throws IOException {
        if (isOpen()) {
            logger.info("close index {}.", indexName);
            this.indexWriter.close();
            logger.info("close index {} succeed.", indexName);
        }
    }

    public void delete() throws IOException {
        //TODO close and delete directory
    }

    public void flush() throws IOException {
        if (!isOpen()) {
            logger.info("index: {} closed, not need to flush.", indexName);
            return;
        }
        indexWriter.commit();
    }

    public int index(Document document) throws IOException {
        if (indexWriter == null) {
            return -1;
        }
        indexWriter.addDocument(document);
        logger.debug("index succeed.");
        return 0;
    }

    public Map<String, Object> search(Query query, int size, ScoreDoc after) throws IOException {
        IndexSearcher searcher = null;
        logger.info("query is: {}.", query.toString());
        Map<String, Object> rtn = new HashMap<>();
        try {
            searcher = searcherManager.acquire();
            TopScoreDocCollector collector = TopScoreDocCollector.create(size, after);
            searcher.search(query, collector);
            TopDocs topDocs = collector.topDocs();
            int totalHits = topDocs.totalHits;
            rtn.put("total", totalHits);
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            List<Document> data = new ArrayList<>();
            for (ScoreDoc scoreDoc : scoreDocs) {
                Document doc = searcher.doc( scoreDoc.doc);
                data.add(doc);
            }
            rtn.put("data", data);
        } finally {
            if (searcher != null) {
                searcherManager.release(searcher);
            }
        }
        return rtn;
    }


    /**
     * IndexWriter has been opened or not
     *
     * @return result
     */
    private boolean isOpen() {
        return indexWriter != null && indexWriter.isOpen();
    }
}
