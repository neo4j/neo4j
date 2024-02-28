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

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.junit.jupiter.api.AfterEach;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigBuilder;
import org.neo4j.kernel.api.impl.index.TestIndexWriterModes;

public class WritableIndexSnapshotFileIteratorTest extends ReadOnlyIndexSnapshotFileIteratorTest {
    private IndexWriter indexWriter;

    @Override
    @AfterEach
    public void tearDown() throws IOException {
        if (indexWriter != null) {
            indexWriter.close();
        }
        super.tearDown();
    }

    @Override
    protected ResourceIterator<Path> makeSnapshot() throws IOException {
        Config config = Config.defaults();
        IndexWriterConfig writerConfig = new IndexWriterConfigBuilder(TestIndexWriterModes.STANDARD, config).build();
        indexWriter = new IndexWriter(dir, writerConfig);
        return LuceneIndexSnapshots.forIndex(indexDir, indexWriter);
    }
}
