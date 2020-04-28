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
package org.neo4j.kernel.impl.api.index;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.index.updater.UpdateCountingIndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;

public class OnlineIndexProxy implements IndexProxy
{
    private final long indexId;
    private final IndexDescriptor descriptor;
    final IndexAccessor accessor;
    private final IndexStatisticsStore indexStatisticsStore;
    private boolean started;

    // About this flag: there are two online "modes", you might say...
    // - One is the pure starting of an already online index which was cleanly shut down and all that.
    //   This scenario is simple and doesn't need this idempotency mode.
    // - The other is the creation or starting from an uncompleted population, where there will be a point
    //   in the future where this index will flip from a populating index proxy to an online index proxy.
    //   This is the problematic part. You see... we have been accidentally relying on the short-lived node
    //   entity locks for this to work. The scenario where they have saved indexes from getting duplicate
    //   nodes in them (one from populator and the other from a "normal" update is where a populator is nearing
    //   its completion and wants to flip. Another thread is in the middle of applying a transaction which
    //   in the end will feed an update to this index. Index updates are applied after store updates, so
    //   the populator may see the created node and add it, index flips and then the updates comes in to the normal
    //   online index and gets added again. The read lock here will have the populator wait for the transaction
    //   to fully apply, e.g. also wait for the index update to reach the population job before adding that node
    //   and flipping (the update mechanism in a populator is idempotent).
    //     This strategy has changed slightly in 3.0 where transactions can be applied in whole batches
    //   and index updates for the whole batch will be applied in the end. This is fine for everything except
    //   the above scenario because the short-lived entity locks are per transaction, not per batch, and must
    //   be so to not interfere with transactions creating constraints inside this batch. We do need to apply
    //   index updates in batches because nowadays slave update pulling and application isn't special in any
    //   way, it's simply applying transactions in batches and this needs to be very fast to not have instances
    //   fall behind in a cluster.
    //     So the sum of this is that during the session (until the next restart of the db) an index gets created
    //   it will be in this forced idempotency mode where it applies additions idempotently, which may be
    //   slightly more costly, but shouldn't make that big of a difference hopefully.
    private final boolean forcedIdempotentMode;

    OnlineIndexProxy( IndexDescriptor descriptor, IndexAccessor accessor, IndexStatisticsStore indexStatisticsStore,
            boolean forcedIdempotentMode )
    {
        assert accessor != null;
        this.indexId = descriptor.getId();
        this.descriptor = descriptor;
        this.accessor = accessor;
        this.indexStatisticsStore = indexStatisticsStore;
        this.forcedIdempotentMode = forcedIdempotentMode;
    }

    @Override
    public void start()
    {
        started = true;
    }

    @Override
    public IndexUpdater newUpdater( final IndexUpdateMode mode, PageCursorTracer cursorTracer )
    {
        IndexUpdater actual = accessor.newUpdater( escalateModeIfNecessary( mode ), cursorTracer );
        return started ? updateCountingUpdater( actual ) : actual;
    }

    private IndexUpdateMode escalateModeIfNecessary( IndexUpdateMode mode )
    {
        if ( forcedIdempotentMode )
        {
            // If this proxy is flagged with taking extra care about idempotency then escalate ONLINE to ONLINE_IDEMPOTENT.
            if ( mode != IndexUpdateMode.ONLINE )
            {
                throw new IllegalArgumentException( "Unexpected mode " + mode + " given that " + this +
                        " has been marked with forced idempotent mode. Expected mode " + IndexUpdateMode.ONLINE );
            }
            return IndexUpdateMode.ONLINE_IDEMPOTENT;
        }
        return mode;
    }

    private IndexUpdater updateCountingUpdater( final IndexUpdater indexUpdater )
    {
        return new UpdateCountingIndexUpdater( indexStatisticsStore, indexId, indexUpdater );
    }

    @Override
    public void drop()
    {
        indexStatisticsStore.removeIndex( getDescriptor().getId() );
        accessor.drop();
    }

    @Override
    public IndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.ONLINE;
    }

    @Override
    public void force( IOLimiter ioLimiter, PageCursorTracer cursorTracer )
    {
        accessor.force( ioLimiter, cursorTracer );
    }

    @Override
    public void refresh()
    {
        accessor.refresh();
    }

    @Override
    public void close( PageCursorTracer cursorTracer ) throws IOException
    {
        accessor.close();
    }

    @Override
    public IndexReader newReader()
    {
        return accessor.newReader();
    }

    @Override
    public boolean awaitStoreScanCompleted( long time, TimeUnit unit )
    {
        return false; // the store scan is already completed
    }

    @Override
    public void activate()
    {
        // ok, already active
    }

    @Override
    public void validate()
    {
        // ok, it's online so it's valid
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        accessor.validateBeforeCommit( tuple );
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException
    {
        throw new IllegalStateException( this + " is ONLINE" );
    }

    @Override
    public PopulationProgress getIndexPopulationProgress()
    {
        return PopulationProgress.DONE;
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        return accessor.snapshotFiles();
    }

    @Override
    public Map<String,Value> indexConfig()
    {
        return accessor.indexConfig();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[accessor:" + accessor + ", descriptor:" + descriptor + ']';
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {
        accessor.verifyDeferredConstraints( nodePropertyAccessor );
    }

    @VisibleForTesting
    public IndexAccessor accessor()
    {
        return accessor;
    }
}
