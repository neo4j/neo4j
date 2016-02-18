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
package org.neo4j.coreedge.server.core.locks;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PendingLockTokensRequests<MEMBER>
{
    private final Map<ReplicatedLockTokenRequest<MEMBER>, PendingLockTokenRequest> outstanding =
            new ConcurrentHashMap<>();
    private LockToken currentToken = ReplicatedLockTokenRequest.INVALID_REPLICATED_LOCK_TOKEN_REQUEST;

    public LockToken currentToken()
    {
        return currentToken;
    }

    public void setCurrentToken( LockToken currentToken )
    {
        this.currentToken = currentToken;
    }

    public PendingLockTokenRequest register( ReplicatedLockTokenRequest<MEMBER> request )
    {
        PendingLockTokenFuture future = new PendingLockTokenFuture( request );
        outstanding.put( request, future );
        return future;
    }

    public PendingLockTokenRequest retrieve( ReplicatedLockTokenRequest<MEMBER> request )
    {
        return outstanding.remove( request );
    }

    public class PendingLockTokenFuture implements PendingLockTokenRequest
    {
        private final CompletableFuture<Boolean> future = new CompletableFuture<>();
        private final ReplicatedLockTokenRequest<MEMBER> request;

        public PendingLockTokenFuture( ReplicatedLockTokenRequest<MEMBER> request )
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
            future.complete( false );
        }

        @Override
        public void close()
        {
            outstanding.remove( request );
        }
    }
}
