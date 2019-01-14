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
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.backup.LuceneIndexSnapshots;

/**
 * Represents a single read only partition of a partitioned lucene index.
 * Read only partition do not support write to index and performs all read operations based on index opened in read
 * only mode.
 */
public class ReadOnlyIndexPartition extends AbstractIndexPartition
{
    private final SearcherManager searcherManager;

    ReadOnlyIndexPartition( File partitionFolder, Directory directory ) throws IOException
    {
        super( partitionFolder, directory );
        this.searcherManager = new SearcherManager( directory, new SearcherFactory() );
    }

    @Override
    public IndexWriter getIndexWriter()
    {
        throw new UnsupportedOperationException( "Retrieving index writer from read only index partition is " +
                                                 "unsupported." );
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
     *  Refresh partition. No-op in read only partition.
     *
     * @throws IOException if refreshing fails.
     */
    @Override
    public void maybeRefreshBlocking()
    {
        // nothing to refresh in read only partition
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( searcherManager, directory );
    }

    /**
     * Retrieve list of consistent Lucene index files for read only partition.
     *
     * @return the iterator over index files.
     * @throws IOException if any IO operation fails.
     */
    @Override
    public ResourceIterator<File> snapshot() throws IOException
    {
        return LuceneIndexSnapshots.forIndex( partitionFolder, directory );
    }
}

