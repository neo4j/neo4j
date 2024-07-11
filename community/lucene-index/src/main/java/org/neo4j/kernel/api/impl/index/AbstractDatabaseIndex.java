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
package org.neo4j.kernel.api.impl.index;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.store.Directory;
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;

/**
 * This class collects the common features of {@link MinimalDatabaseIndex} and {@link WritableDatabaseIndex}.
 */
abstract class AbstractDatabaseIndex<INDEX extends AbstractLuceneIndex<READER>, READER extends ValueIndexReader>
        implements DatabaseIndex<READER> {
    protected final INDEX luceneIndex;

    AbstractDatabaseIndex(INDEX luceneIndex) {
        this.luceneIndex = luceneIndex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open() throws IOException {
        luceneIndex.open();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return luceneIndex.isOpen();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists() throws IOException {
        return luceneIndex.exists();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        return luceneIndex.isValid();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LuceneAllDocumentsReader allDocumentsReader() {
        return luceneIndex.allDocumentsReader();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AbstractIndexPartition> getPartitions() {
        return luceneIndex.getPartitions();
    }

    @Override
    public void accessClosedDirectories(ThrowingBiConsumer<Integer, Directory, IOException> visitor)
            throws IOException {
        luceneIndex.accessClosedDirectories(visitor);
    }

    @Override
    public READER getIndexReader(IndexUsageTracking usageTracker) throws IOException {
        return luceneIndex.getIndexReader(usageTracker);
    }

    @Override
    public IndexDescriptor getDescriptor() {
        return luceneIndex.getDescriptor();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOnline() throws IOException {
        return luceneIndex.isOnline();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void markAsFailed(String failure) throws IOException {
        luceneIndex.markAsFailed(failure);
    }
}
