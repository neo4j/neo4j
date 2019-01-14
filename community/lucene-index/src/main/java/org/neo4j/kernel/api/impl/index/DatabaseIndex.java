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
package org.neo4j.kernel.api.impl.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.impl.index.backup.WritableIndexSnapshotFileIterator;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;

/**
 * Lucene index that may consist of one or multiple separate lucene indexes that are represented as independent
 * {@link AbstractIndexPartition partitions}.
 */
public interface DatabaseIndex extends Closeable
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
     *
     * @throws IOException
     */
    void drop() throws IOException;

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
}
