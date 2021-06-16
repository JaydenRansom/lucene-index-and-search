package com.jayden.search.test;

import com.jayden.search.IndexContainer;
import org.apache.lucene.document.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author jayden
 * @date 6/16/21
 */
public class IndexTest {

    private static final Logger logger = LoggerFactory.getLogger(IndexTest.class);

    private IndexContainer indexContainer;

    @Before
    public void before() {
        indexContainer = new IndexContainer("/export/search/test", 5);
    }

    @Test
    public void testIndex() throws IOException {
        Document document = new Document();
        document.add(new LongPoint("time", 10000000L));
        document.add(new StoredField("path", "/export/log/test/test.log"));
        document.add(new StringField("jayden", "test", Field.Store.YES));

        indexContainer.write("test-index", document);
    }
}
