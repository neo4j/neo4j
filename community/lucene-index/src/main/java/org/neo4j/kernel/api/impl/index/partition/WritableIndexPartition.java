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
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectoryReader;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexSearcher;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriter;
import org.neo4j.kernel.api.impl.index.lucene.LuceneIndexWriterConfig;
import org.neo4j.kernel.api.impl.index.lucene.LuceneSearcherManager;

/**
 * Represents a single writable partition of a partitioned lucene index.
 * @see AbstractIndexPartition
 */
public class WritableIndexPartition extends AbstractIndexPartition {
    private final LuceneIndexWriter indexWriter;
    private final LuceneSearcherManager searcherManager;
    private final LuceneDirectoryReader directoryReader;

    public WritableIndexPartition(Path partitionFolder, LuceneDirectory directory, LuceneIndexWriterConfig writerConfig)
            throws IOException {
        super(partitionFolder, directory);
        this.indexWriter = directory.newWriter(writerConfig);
        this.directoryReader = indexWriter.directoryReader();
        this.searcherManager = directoryReader.newSearcherManager();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LuceneIndexWriter getIndexWriter() {
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
        IOUtils.closeAll(searcherManager, directoryReader, indexWriter, getDirectory());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceIterator<Path> snapshot() throws IOException {
        return indexWriter.snapshot(partitionFolder);
    }

    @Override
    public void accessClosedDirectory(ThrowingBiConsumer<Integer, LuceneDirectory, IOException> visitor)
            throws IOException {
        indexWriter.close();
        LuceneIndexSearcher searcher = searcherManager.acquire();
        int numDocs;
        try {
            numDocs = searcher.numDocs();
        } finally {
            searcherManager.close();
        }
        visitor.accept(numDocs, directory);
    }
}
