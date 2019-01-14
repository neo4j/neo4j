/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index.partition;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.backup.LuceneIndexSnapshots;

/**
 * Represents a single writable partition of a partitioned lucene index.
 * @see AbstractIndexPartition
 */
public class WritableIndexPartition extends AbstractIndexPartition
{
    private final IndexWriter indexWriter;
    private final SearcherManager searcherManager;

    public WritableIndexPartition( File partitionFolder, Directory directory, IndexWriterConfig writerConfig )
            throws IOException
    {
        super( partitionFolder, directory );
        this.indexWriter = new IndexWriter( directory, writerConfig );
        this.searcherManager = new SearcherManager( indexWriter, new SearcherFactory() );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexWriter getIndexWriter()
    {
        return indexWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionSearcher acquireSearcher() throws IOException
    {
        return new PartitionSearcher( searcherManager );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void maybeRefreshBlocking() throws IOException
    {
        searcherManager.maybeRefreshBlocking();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( searcherManager, indexWriter, getDirectory() );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceIterator<File> snapshot() throws IOException
    {
        return LuceneIndexSnapshots.forIndex( partitionFolder, indexWriter );
    }
}
