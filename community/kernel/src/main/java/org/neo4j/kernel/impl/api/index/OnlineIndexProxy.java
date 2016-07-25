/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.util.concurrent.Future;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.kernel.impl.api.index.updater.UpdateCountingIndexUpdater;

import static org.neo4j.helpers.FutureAdapter.VOID;

public class OnlineIndexProxy implements IndexProxy
{
    private final IndexDescriptor descriptor;
    final IndexAccessor accessor;
    private final IndexStoreView storeView;
    private final SchemaIndexProvider.Descriptor providerDescriptor;
    private final IndexConfiguration configuration;
    private final IndexCountsRemover indexCountsRemover;

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

    public OnlineIndexProxy( IndexDescriptor descriptor, IndexConfiguration configuration, IndexAccessor accessor,
                             IndexStoreView storeView, SchemaIndexProvider.Descriptor providerDescriptor,
                             boolean forcedIdempotentMode )
    {
        this.descriptor = descriptor;
        this.storeView = storeView;
        this.providerDescriptor = providerDescriptor;
        this.accessor = accessor;
        this.configuration = configuration;
        this.forcedIdempotentMode = forcedIdempotentMode;
        this.indexCountsRemover = new IndexCountsRemover( storeView, descriptor );
    }

    @Override
    public void start()
    {
    }

    @Override
    public IndexUpdater newUpdater( final IndexUpdateMode mode )
    {
        return updateCountingUpdater( accessor.newUpdater( forcedIdempotentMode ? IndexUpdateMode.RECOVERY : mode ) );
    }

    private IndexUpdater updateCountingUpdater( final IndexUpdater indexUpdater )
    {
        return new UpdateCountingIndexUpdater( storeView, descriptor, indexUpdater );
    }

    @Override
    public Future<Void> drop() throws IOException
    {
        indexCountsRemover.remove();
        accessor.drop();
        return VOID;
    }

    @Override
    public IndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public SchemaIndexProvider.Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.ONLINE;
    }

    @Override
    public void force() throws IOException
    {
        accessor.force();
    }

    @Override
    public void flush() throws IOException
    {
        accessor.flush();
    }

    @Override
    public Future<Void> close() throws IOException
    {
        accessor.close();
        return VOID;
    }

    @Override
    public IndexReader newReader()
    {
        return accessor.newReader();
    }

    @Override
    public boolean awaitStoreScanCompleted() throws IndexPopulationFailedKernelException, InterruptedException
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
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return accessor.snapshotFiles();
    }

    @Override
    public IndexConfiguration config()
    {
        return configuration;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[accessor:" + accessor + ", descriptor:" + descriptor + "]";
    }

}
