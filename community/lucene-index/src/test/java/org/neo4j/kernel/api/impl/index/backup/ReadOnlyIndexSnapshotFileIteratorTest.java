/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index.backup;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.TestIndexWriterModes;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class ReadOnlyIndexSnapshotFileIteratorTest {
    @Inject
    private TestDirectory testDir;

    Path indexDir;
    protected Directory dir;

    @BeforeEach
    void setUp() throws IOException {
        indexDir = testDir.homePath();
        dir = DirectoryFactory.PERSISTENT.open(indexDir);
    }

    @AfterEach
    public void tearDown() throws IOException {
        IOUtils.closeAll(dir);
    }

    @Test
    void shouldReturnRealSnapshotIfIndexAllowsIt() throws IOException {
        prepareIndex();

        Set<String> files = listDir(dir);
        assertFalse(files.isEmpty());

        try (ResourceIterator<Path> snapshot = makeSnapshot()) {
            Set<String> snapshotFiles =
                    snapshot.stream().map(Path::getFileName).map(Path::toString).collect(toSet());
            assertEquals(files, snapshotFiles);
        }
    }

    @Test
    void shouldReturnEmptyIteratorWhenNoCommitsHaveBeenMade() throws IOException {
        try (ResourceIterator<Path> snapshot = makeSnapshot()) {
            assertFalse(snapshot.hasNext());
        }
    }

    private void prepareIndex() throws IOException {
        Config config = Config.defaults();
        IndexWriterConfig writerConfig = new IndexWriterConfigBuilder(TestIndexWriterModes.STANDARD, config).build();
        try (IndexWriter writer = new IndexWriter(dir, writerConfig)) {
            insertRandomDocuments(writer);
        }
    }

    protected ResourceIterator<Path> makeSnapshot() throws IOException {
        return LuceneIndexSnapshots.forIndex(indexDir, dir);
    }

    private static void insertRandomDocuments(IndexWriter writer) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("a", "b", Field.Store.YES));
        doc.add(new StringField("c", "d", Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
    }

    private static Set<String> listDir(Directory dir) throws IOException {
        String[] files = dir.listAll();
        return Stream.of(files)
                .filter(file -> !IndexWriter.WRITE_LOCK_NAME.equals(file))
                .collect(toSet());
    }
}
