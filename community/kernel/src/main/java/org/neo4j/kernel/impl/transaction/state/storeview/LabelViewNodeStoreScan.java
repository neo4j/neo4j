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
package org.neo4j.kernel.impl.transaction.state.storeview;

import java.util.function.IntPredicate;
import javax.annotation.Nullable;

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.lock.LockService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageReader;

/**
 * Store scan view that will try to minimize the amount of scanned nodes by using label scan store {@link LabelScanStore}
 * as a source of known labeled node ids.
 */
public class LabelViewNodeStoreScan extends NodeStoreScan
{
    private final LabelScanStore labelScanStore;

    public LabelViewNodeStoreScan( Config config, StorageReader storageReader, LockService locks,
            LabelScanStore labelScanStore,
            @Nullable TokenScanConsumer labelScanConsumer,
            @Nullable PropertyScanConsumer propertyScanConsumer,
            int[] labelIds, IntPredicate propertyKeyIdFilter, boolean parallelWrite,
            JobScheduler scheduler, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        super( config, storageReader, locks, labelScanConsumer, propertyScanConsumer, labelIds,
                propertyKeyIdFilter, parallelWrite, scheduler, cacheTracer, memoryTracker );
        this.labelScanStore = labelScanStore;
    }

    @Override
    public EntityIdIterator getEntityIdIterator( PageCursorTracer cursorTracer )
    {
        return new TokenScanViewIdIterator( labelScanStore.newReader(), entityTokenIdFilter, cursorTracer );
    }
}
