package com.jayden.search;

import org.apache.lucene.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Container for multi indices
 *
 * @author jayden
 * @date 6/16/21
 */
public class IndexContainer {

    private static final Logger logger = LoggerFactory.getLogger(IndexContainer.class);
    private static final byte[] lock = new byte[0];

    private final String basePath;
    private final int concurrency;
    private Map<String, IndexInstance> indices;

    private ScheduledExecutorService refreshIndexExecutor;
    private ScheduledExecutorService flushExecutor;

    public IndexContainer(final String basePath, final int concurrency) {
        this.basePath = basePath;
        this.concurrency = concurrency;
    }

    private void initIndices() {
        logger.info("start init indices from disk path: {}.", basePath);
        File files = new File(basePath);
        if (!files.exists()) {
            boolean result = files.mkdir();
            logger.info("path: {} is not exist, attempt to mkdir, result is: {}.", basePath, result);
        }
        for (File file : Objects.requireNonNull(files.listFiles())) {
            try {
                String indexName = file.getName();
                String path = file.getPath();
                IndexInstance indexInstance = new IndexInstance(indexName, path);
                this.indices.put(indexName, indexInstance);
            } catch (Exception e) {
                logger.error("init index from disk error.", e);
                throw new RuntimeException("Failed to open index:" + file);
            }
        }
        logger.info("init indices from disk succeed.");
    }

    private void scheduleFlush() {
        this.flushExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread();
                t.setName("flush-thread");
                return t;
            }
        });
    }

    public void write(String index, Document document) throws IOException {
        IndexInstance indexInstance = getIndexOrCreate(index);
        int write = indexInstance.index(document);
        if (write == -1) {
            indexInstance.openIndex();
            indexInstance.index(document);
        }
    }


    private IndexInstance getIndexOrCreate(String index) throws IOException {
        IndexInstance indexInstance = this.getIndex(index);
        if (indexInstance != null) {
            return indexInstance;
        }
        synchronized (lock) {
            indexInstance = this.getIndex(index);
            if (indexInstance == null) {
                indexInstance = new IndexInstance(index, getIndexPath(index));
                indexInstance.openIndex();
                indices.put(index, indexInstance);
            }
            return indexInstance;
        }
    }

    private String getIndexPath(String index) {
        return this.basePath + "/" + index;
    }

    private IndexInstance getIndex(String index) {
        return indices.get(index);
    }

}
