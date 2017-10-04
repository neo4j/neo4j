/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreIdIterator;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.schema.PopulationProgress;

import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

/**
 * Node scanner that will perform some sort of process over set of nodes
 * from nodeStore {@link NodeStore} based on node ids supplied by underlying store aware id iterator.
 * @param <FAILURE> type of exception thrown on failure
 */
public abstract class NodeStoreScan<FAILURE extends Exception> implements StoreScan<FAILURE>
{
    private volatile boolean continueScanning;
    private final NodeRecord record;

    protected final NodeStore nodeStore;
    protected final LockService locks;
    private final long totalCount;

    private long count;

    public abstract void process( NodeRecord loaded ) throws FAILURE;

    public NodeStoreScan( NodeStore nodeStore, LockService locks, long totalCount )
    {
        this.nodeStore = nodeStore;
        this.record = nodeStore.newRecord();
        this.locks = locks;
        this.totalCount = totalCount;
    }

    @Override
    public void run() throws FAILURE
    {
        try ( PrimitiveLongResourceIterator nodeIds = getNodeIdIterator() )
        {
            continueScanning = true;
            while ( continueScanning && nodeIds.hasNext() )
            {
                long id = nodeIds.next();
                try ( Lock ignored = locks.acquireNodeLock( id, LockService.LockType.READ_LOCK ) )
                {
                    count++;
                    if ( nodeStore.getRecord( id, record, FORCE ).inUse() )
                    {
                        process( record );
                    }
                }
            }
        }
    }

    protected PrimitiveLongResourceIterator getNodeIdIterator()
    {
        return PrimitiveLongCollections.resourceIterator( new StoreIdIterator( nodeStore ), null );
    }

    @Override
    public void stop()
    {
        continueScanning = false;
    }

    @Override
    public PopulationProgress getProgress()
    {
        if ( totalCount > 0 )
        {
            return new PopulationProgress( count, totalCount );
        }

        // nothing to do 100% completed
        return PopulationProgress.DONE;
    }
}
