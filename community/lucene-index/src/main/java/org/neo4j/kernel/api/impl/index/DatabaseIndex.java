/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.impl.index.backup.WritableIndexSnapshotFileIterator;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;

/**
 * Lucene index that may consist of one or multiple separate lucene indexes that are represented as independent
 * {@link AbstractIndexPartition partitions}.
 */
public interface DatabaseIndex<READER extends IndexReader> extends Closeable
{
    /**
     * Creates new index.
     * As part of creation process index will allocate all required folders, index failure storage
     * and will create its first partition.
     * <p>
     * <b>Index creation do not automatically open it. To be able to use index please open it first.</b>
     *
     * @throws IOException
     */
    void create() throws IOException;

    /**
     * Open index with all allocated partitions.
     *
     * @throws IOException
     */
    void open() throws IOException;

    /**
     * Check if index is open or not
     * @return true if index is open
     */
    boolean isOpen();

    /**
     * Check if index is opened in read only mode
     * @return true if index open in rad only mode
     */
    boolean isReadOnly();

    /**
     * Check lucene index existence within all allocated partitions.
     *
     * @return true if index exist in all partitions, false when index is empty or does not exist
     * @throws IOException
     */
    boolean exists() throws IOException;

    /**
     * Verify state of the index.
     * If index is already open and in use method assume that index is valid since lucene already operating with it,
     * otherwise necessary checks perform.
     *
     * @return true if lucene confirm that index is in valid clean state or index is already open.
     */
    boolean isValid();

    /**
     * Close index and deletes all it's partitions.
     */
    void drop();

    /**
     * Commits all index partitions.
     *
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Creates an iterable over all {@link org.apache.lucene.document.Document document}s in all partitions.
     *
     * @return LuceneAllDocumentsReader over all documents
     */
    LuceneAllDocumentsReader allDocumentsReader();

    /**
     * Snapshot of all file in all index partitions.
     *
     * @return iterator over all index files.
     * @throws IOException
     * @see WritableIndexSnapshotFileIterator
     */
    ResourceIterator<File> snapshot() throws IOException;

    /**
     * Refresh all partitions to make newly inserted data visible for readers.
     *
     * @throws IOException
     */
    void maybeRefreshBlocking() throws IOException;

    /**
     * Get index partitions
     * @return list of index partition
     */
    List<AbstractIndexPartition> getPartitions();

    LuceneIndexWriter getIndexWriter();

    READER getIndexReader() throws IOException;

    IndexDescriptor getDescriptor();

    /**
     * Check if this index is marked as online.
     *
     * @return <code>true</code> if index is online, <code>false</code> otherwise
     * @throws IOException
     */
    boolean isOnline() throws IOException;

    /**
     * Marks index as online by including "status" -> "online" map into commit metadata of the first partition.
     *
     * @throws IOException
     */
    void markAsOnline() throws IOException;

    /**
     * Writes the given failure message to the failure storage.
     *
     * @param failure the failure message.
     * @throws IOException
     */
    void markAsFailed( String failure ) throws IOException;
}
