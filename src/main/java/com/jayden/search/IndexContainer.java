package com.jayden.search;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Objects;

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


}
