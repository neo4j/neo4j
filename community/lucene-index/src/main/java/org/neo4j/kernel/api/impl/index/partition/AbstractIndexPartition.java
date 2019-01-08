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
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ResourceIterator;

/**
 * Represents a single partition of a partitioned lucene index. Each partition is a separate Lucene index.
 * Contains and manages lifecycle of the corresponding {@link Directory}, {@link IndexWriter writer} and
 * {@link SearcherManager}.
 */
public abstract class AbstractIndexPartition implements Closeable
{
    protected final Directory directory;
    protected final File partitionFolder;

    public AbstractIndexPartition( File partitionFolder, Directory directory )
    {
        this.partitionFolder = partitionFolder;
        this.directory = directory;
    }

    /**
     * Retrieve index partition directory
     * @return partition directory
     */
    public Directory getDirectory()
    {
        return directory;
    }

    /**
     * Retrieve index partition writer
     * @return partition writer
     */
    public abstract IndexWriter getIndexWriter();

    /**
     * Return searcher for requested partition.
     * There is no tracking of acquired searchers, so the expectation is that callers will call close on acquired
     * searchers to release resources.
     *
     * @return partition searcher
     * @throws IOException if exception happened during searcher acquisition
     */
    public abstract PartitionSearcher acquireSearcher() throws IOException;

    /**
     * Refresh partition to make newly inserted data visible for readers.
     *
     * @throws IOException if refreshing fails.
     */
    public abstract void maybeRefreshBlocking() throws IOException;

    /**
     * Retrieve list of consistent Lucene index files for this partition.
     *
     * @return the iterator over index files.
     * @throws IOException if any IO operation fails.
     */
    public abstract ResourceIterator<File> snapshot() throws IOException;

}
