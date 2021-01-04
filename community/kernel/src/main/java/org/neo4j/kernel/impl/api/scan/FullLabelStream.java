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
package org.neo4j.kernel.impl.api.scan;

import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;

import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.index.schema.FullStoreChangeStream;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityTokenUpdate;

import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;

/**
 * {@link FullStoreChangeStream} that scan the node store using a {@link IndexStoreView} to get its data.
 */
public class FullLabelStream extends FullTokenStream
{
    public FullLabelStream( IndexStoreView indexStoreView )
    {
        super( indexStoreView );
    }

    @Override
    StoreScan<IOException> getStoreScan( IndexStoreView indexStoreView, Visitor<EntityTokenUpdate,IOException> tokenUpdateVisitor,
            PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return indexStoreView.visitNodes( ArrayUtils.EMPTY_INT_ARRAY, ALWAYS_TRUE_INT, null, tokenUpdateVisitor, true,
                cursorTracer, memoryTracker );
    }
}
