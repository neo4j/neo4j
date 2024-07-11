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
package org.neo4j.kernel.api.impl.schema.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.sampler.AggregatingIndexSampler;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexReader;
import org.neo4j.kernel.api.index.BridgingIndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.values.storable.Value;

/**
 * Index reader that is able to read/sample multiple partitions of a partitioned Lucene index.
 * Internally uses multiple {@link TextIndexReader} or {@link TrigramIndexReader}s for individual partitions.
 */
public class PartitionedValueIndexReader implements ValueIndexReader {
    private final IndexDescriptor descriptor;
    private final List<ValueIndexReader> indexReaders;
    private final IndexUsageTracking usageTracker;

    public PartitionedValueIndexReader(
            IndexDescriptor descriptor, List<ValueIndexReader> readers, IndexUsageTracking usageTracker) {
        this.descriptor = descriptor;
        this.indexReaders = readers;
        this.usageTracker = usageTracker;
    }

    @Override
    public void query(
            IndexProgressor.EntityValueClient client,
            QueryContext context,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query)
            throws IndexNotApplicableKernelException {
        try {
            BridgingIndexProgressor bridgingIndexProgressor =
                    new BridgingIndexProgressor(client, descriptor.schema().getPropertyIds());
            indexReaders.parallelStream().forEach(reader -> {
                try {
                    reader.query(bridgingIndexProgressor, context, constraints, query);
                } catch (IndexNotApplicableKernelException e) {
                    throw new InnerException(e);
                }
            });
            usageTracker.queried();
            boolean needStoreFilter = bridgingIndexProgressor.needStoreFilter();
            client.initializeQuery(descriptor, bridgingIndexProgressor, false, needStoreFilter, constraints, query);
        } catch (InnerException e) {
            throw e.getCause();
        }
    }

    @Override
    public PartitionedValueSeek valueSeek(
            int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    private static final class InnerException extends RuntimeException {
        private InnerException(IndexNotApplicableKernelException e) {
            super(e);
        }

        @Override
        public synchronized IndexNotApplicableKernelException getCause() {
            return (IndexNotApplicableKernelException) super.getCause();
        }
    }

    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        return indexReaders.parallelStream()
                .mapToLong(
                        reader -> reader.countIndexedEntities(entityId, cursorContext, propertyKeyIds, propertyValues))
                .sum();
    }

    @Override
    public IndexSampler createSampler() {
        List<IndexSampler> indexSamplers = indexReaders.parallelStream()
                .map(ValueIndexReader::createSampler)
                .toList();
        return new AggregatingIndexSampler(indexSamplers);
    }

    @Override
    public void close() {
        try {
            List<AutoCloseable> resources = new ArrayList<>(indexReaders);
            IOUtils.closeAll(resources);
        } catch (IOException e) {
            throw new IndexReaderCloseException(e);
        }
    }
}
