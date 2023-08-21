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
package org.neo4j.kernel.impl.index.schema.tracking;

import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.values.storable.Value;

public class TrackingIndexReader implements ValueIndexReader {
    private final ValueIndexReader delegate;
    private final AtomicLong closeReadersCounter;

    TrackingIndexReader(ValueIndexReader delegate, AtomicLong closeReadersCounter) {
        this.delegate = delegate;
        this.closeReadersCounter = closeReadersCounter;
    }

    @Override
    public long countIndexedEntities(
            long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues) {
        return delegate.countIndexedEntities(entityId, cursorContext, propertyKeyIds, propertyValues);
    }

    @Override
    public IndexSampler createSampler() {
        return delegate.createSampler();
    }

    @Override
    public void query(
            IndexProgressor.EntityValueClient client,
            QueryContext context,
            AccessMode accessMode,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query)
            throws IndexNotApplicableKernelException {
        delegate.query(client, context, accessMode, constraints, query);
    }

    @Override
    public PartitionedValueSeek valueSeek(
            int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query) {
        return delegate.valueSeek(desiredNumberOfPartitions, context, query);
    }

    @Override
    public void close() {
        delegate.close();
        closeReadersCounter.incrementAndGet();
    }
}
