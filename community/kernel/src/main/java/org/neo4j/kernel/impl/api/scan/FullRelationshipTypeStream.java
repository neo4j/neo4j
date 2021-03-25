/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.api.scan;

import org.apache.commons.lang3.ArrayUtils;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.index.schema.FullStoreChangeStream;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;

/**
 * {@link FullStoreChangeStream} that scan the relationship store using a {@link IndexStoreView} to get its data.
 */
public class FullRelationshipTypeStream extends FullTokenStream
{
    public FullRelationshipTypeStream( IndexStoreView indexStoreView )
    {
        super( indexStoreView );
    }

    @Override
    StoreScan getStoreScan( IndexStoreView indexStoreView, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        return indexStoreView.visitRelationships( ArrayUtils.EMPTY_INT_ARRAY, ALWAYS_TRUE_INT, null, this, true, false, cacheTracer,
                memoryTracker );
    }
}
