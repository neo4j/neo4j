/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.replication.id;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.raft.state.id_allocation.IdAllocationState;
import org.neo4j.coreedge.raft.state.id_allocation.UnallocatedIds;
import org.neo4j.coreedge.server.core.locks.PendingIdAllocationRequest;
import org.neo4j.kernel.impl.store.id.IdType;

public class PendingIdAllocationRequests
{
    private final Map<ReplicatedIdAllocationRequest, PendingIdAllocationRequest> outstanding =
            new ConcurrentHashMap<>();
    private UnallocatedIds idAllocationState = new IdAllocationState();

    public synchronized long firstUnallocated( IdType idType )
    {
        return idAllocationState.firstUnallocated( idType );
    }

    public void setUnallocatedIds( UnallocatedIds state ) {

        this.idAllocationState = state;
    }

    public PendingIdAllocationRequest register( ReplicatedIdAllocationRequest request )
    {
        PendingIdAllocationRequest future = new PendingIdAllocationFuture( request );
        outstanding.put( request, future );
        return future;
    }

    public PendingIdAllocationRequest retrieve( ReplicatedIdAllocationRequest request )
    {
        return outstanding.remove( request );
    }

    public class PendingIdAllocationFuture implements PendingIdAllocationRequest
    {
        private final CompletableFuture<Boolean> future = new CompletableFuture<>();
        private final ReplicatedIdAllocationRequest request;

        public PendingIdAllocationFuture( ReplicatedIdAllocationRequest request )
        {
            this.request = request;
        }

        @Override
        public boolean waitUntilAcquired( long timeout, TimeUnit unit ) throws InterruptedException, TimeoutException
        {
            try
            {
                return future.get( timeout, unit );
            }
            catch ( ExecutionException e )
            {
                return false;
            }
        }

        @Override
        public void notifyAcquired()
        {
            future.complete( true );
        }

        @Override
        public void notifyLost()
        {
            future.complete(false );
        }

        @Override
        public void close()
        {
            outstanding.remove( request );
        }
    }
}
