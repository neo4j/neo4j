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
import java.nio.file.Path;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.ValueIndexReader;

/**
 * A minimal index does not support reading or writing. It only supports dropping and checking online / failure status.
 */
public class MinimalDatabaseIndex<READER extends ValueIndexReader>
        extends AbstractDatabaseIndex<MinimalLuceneIndex<READER>, READER> {
    public MinimalDatabaseIndex(PartitionedIndexStorage indexStorage, IndexDescriptor descriptor, Config config) {
        super(new MinimalLuceneIndex<>(indexStorage, new ReadOnlyIndexPartitionFactory(), descriptor, config));
    }

    @Override
    public void drop() {
        luceneIndex.drop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create() {
        throw new UnsupportedOperationException("Index creation in read only mode is not supported.");
    }

    @Override
    public boolean isPermanentlyOnly() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadOnly() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() {
        // nothing to flush in read only mode
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        luceneIndex.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceIterator<Path> snapshotFiles() throws IOException {
        return luceneIndex.snapshotFiles();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void maybeRefreshBlocking() {
        // nothing to refresh in read only mode
    }

    @Override
    public LuceneIndexWriter getIndexWriter() {
        throw new UnsupportedOperationException("Can't get index writer for read only lucene index.");
    }

    /**
     * Unsupported operation in read only index.
     */
    @Override
    public void markAsOnline() {
        throw new UnsupportedOperationException("Can't mark read only index.");
    }
}
