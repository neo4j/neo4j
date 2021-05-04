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

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.api.LeaseService.NoLeaseClient;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;

import static org.neo4j.kernel.impl.api.KernelTransactions.SYSTEM_TRANSACTION_ID;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public class IndexedStoreScan implements StoreScan
{
    private final Locks locks;
    private final Config config;
    private final StoreScan delegate;
    private final IndexDescriptor index;

    public IndexedStoreScan( Locks locks, IndexDescriptor index, Config config, StoreScan delegate )
    {
        this.locks = locks;
        this.config = config;
        this.delegate = delegate;
        this.index = index;
    }

    @Override
    public void run( ExternalUpdatesCheck externalUpdatesCheck )
    {
        try ( Locks.Client client = locks.newClient() )
        {
            client.initialize( NoLeaseClient.INSTANCE, SYSTEM_TRANSACTION_ID, INSTANCE, config );
            client.acquireShared( LockTracer.NONE, index.schema().keyType(), index.schema().lockingKeys() );
            delegate.run( externalUpdatesCheck );
        }
    }

    @Override
    public void stop()
    {
        delegate.stop();
    }

    @Override
    public PopulationProgress getProgress()
    {
        return delegate.getProgress();
    }
}
