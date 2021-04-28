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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.staging.PullingProducerStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.CursorContext;
import org.neo4j.kernel.impl.api.index.StoreScan;

import static org.neo4j.lock.LockWaitStrategies.INCREMENTAL_BACKOFF;

public class ReadEntityIdsStep extends PullingProducerStep
{
    private static final String CURSOR_TRACER_TAG = "indexPopulationReadEntityIds";

    private final StoreScan.ExternalUpdatesCheck externalUpdatesCheck;
    private final AtomicBoolean continueScanning;
    private final Function<CursorContext,EntityIdIterator> entityIdIteratorSupplier;
    private final PageCacheTracer pageCacheTracer;
    private volatile long position;
    private CursorContext cursorContext;
    private EntityIdIterator entityIdIterator;
    private long lastEntityId;

    public ReadEntityIdsStep( StageControl control, Configuration configuration, Function<CursorContext,EntityIdIterator> entityIdIteratorSupplier,
            PageCacheTracer cacheTracer, StoreScan.ExternalUpdatesCheck externalUpdatesCheck, AtomicBoolean continueScanning )
    {
        super( control, configuration );
        this.entityIdIteratorSupplier = entityIdIteratorSupplier;
        this.pageCacheTracer = cacheTracer;
        this.externalUpdatesCheck = externalUpdatesCheck;
        this.continueScanning = continueScanning;
    }

    @Override
    protected void process()
    {
        cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( CURSOR_TRACER_TAG ) );
        entityIdIterator = entityIdIteratorSupplier.apply( cursorContext );
        super.process();
    }

    @Override
    protected Object nextBatchOrNull( long ticket, int batchSize )
    {
        if ( !continueScanning.get() || !entityIdIterator.hasNext() )
        {
            return null;
        }

        checkAndApplyExternalUpdates();

        long[] entityIds = new long[batchSize];
        int cursor = 0;
        while ( cursor < batchSize && entityIdIterator.hasNext() )
        {
            entityIds[cursor++] = entityIdIterator.next();
        }
        position += cursor;
        lastEntityId = entityIds[cursor - 1];
        return cursor == entityIds.length ? entityIds : Arrays.copyOf( entityIds, cursor );
    }

    private void checkAndApplyExternalUpdates()
    {
        if ( externalUpdatesCheck.needToApplyExternalUpdates() )
        {
            // Block here until all batches that have been sent already have been fully processed by the downstream steps
            // control.isIdle returns true when all steps in this processing stage have processed all batches they have received
            for ( long i = 0; !control.isIdle(); i++ )
            {
                INCREMENTAL_BACKOFF.apply( i );
            }
            externalUpdatesCheck.applyExternalUpdates( lastEntityId );
            entityIdIterator.invalidateCache();
        }
    }

    @Override
    protected void done()
    {
        super.done();
        IOUtils.closeAllUnchecked( entityIdIterator, cursorContext );
    }

    @Override
    protected long position()
    {
        return position;
    }
}
