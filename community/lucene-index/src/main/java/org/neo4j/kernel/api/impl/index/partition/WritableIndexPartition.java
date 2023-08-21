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
package org.neo4j.kernel.api.impl.index.partition;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.backup.LuceneIndexSnapshots;

/**
 * Represents a single writable partition of a partitioned lucene index.
 * @see AbstractIndexPartition
 */
public class WritableIndexPartition extends AbstractIndexPartition {
    private final IndexWriter indexWriter;
    private final SearcherManager searcherManager;

    public WritableIndexPartition(Path partitionFolder, Directory directory, IndexWriterConfig writerConfig)
            throws IOException {
        super(partitionFolder, directory);
        this.indexWriter = new IndexWriter(directory, writerConfig);
        this.searcherManager = new SearcherManager(indexWriter, new Neo4jSearcherFactory());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionSearcher acquireSearcher() throws IOException {
        return new PartitionSearcher(searcherManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void maybeRefreshBlocking() throws IOException {
        searcherManager.maybeRefreshBlocking();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeAll(searcherManager, indexWriter, getDirectory());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceIterator<Path> snapshot() throws IOException {
        return LuceneIndexSnapshots.forIndex(partitionFolder, indexWriter);
    }

    @Override
    public void accessClosedDirectory(ThrowingBiConsumer<Integer, Directory, IOException> visitor) throws IOException {
        indexWriter.close();
        var searcher = searcherManager.acquire();
        int numDocs;
        try {
            numDocs = searcher.getIndexReader().numDocs();
        } finally {
            searcherManager.close();
        }
        visitor.accept(numDocs, directory);
    }
}
