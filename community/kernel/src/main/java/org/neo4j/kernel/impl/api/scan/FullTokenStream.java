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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.index.schema.FullStoreChangeStream;
import org.neo4j.kernel.impl.index.schema.TokenScanWriter;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityTokenUpdate;

/**
 * {@link FullStoreChangeStream} using a {@link IndexStoreView} to get its data.
 *
 * Connects the provided {@link TokenScanWriter writer} to the receiving end of a full store scan,
 * feeding {@link EntityTokenUpdate entity tokens} into the writer.
 */
public abstract class FullTokenStream implements FullStoreChangeStream, TokenScanConsumer
{
    private final IndexStoreView indexStoreView;
    private TokenScanWriter writer;
    private long count;

    FullTokenStream( IndexStoreView indexStoreView )
    {
        this.indexStoreView = indexStoreView;
    }

    abstract StoreScan getStoreScan( IndexStoreView indexStoreView, PageCacheTracer cacheTracer, MemoryTracker memoryTracker );

    @Override
    public long applyTo( TokenScanWriter writer, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        // Keep the writer for using it in "visit"
        this.writer = writer;
        StoreScan scan = getStoreScan( indexStoreView, cacheTracer, memoryTracker );
        scan.run( StoreScan.NO_EXTERNAL_UPDATES );
        return count;
    }

    @Override
    public Batch newBatch()
    {
        return new Batch()
        {
            final List<EntityTokenUpdate> updates = new ArrayList<>();

            @Override
            public void addRecord( long entityId, long[] tokens )
            {
                updates.add( EntityTokenUpdate.tokenChanges( entityId, new long[0], tokens ) );
            }

            @Override
            public void process()
            {
                for ( EntityTokenUpdate update : updates )
                {
                    writer.write( update );
                    count++;
                }
            }
        };
    }
}
