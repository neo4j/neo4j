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

import org.neo4j.coreedge.raft.LeaderLocator;
import org.neo4j.coreedge.raft.NoLeaderTimeoutException;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionStateMachine;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.storageengine.api.lock.ResourceType;

/**
 * Each member of the cluster uses its own {@link LeaderOnlyLockManager} which wraps a local {@link Locks} manager.
 * The validity of local lock managers is synchronized by using a token which gets requested by each server as necessary
 * and if the request is granted then the associated id can be used to identify a unique lock session in the cluster.
 *
 * The fundamental strategy is to only allow locks on the leader. This has the benefit of minimizing the synchronization
 * to only concern the single token but it also means that non-leaders should not even attempt to request the token or
 * significant churn of this single resource will lead to a high level of aborted transactions.
 *
 * The token requests carry a candidate id and they get ordered with respect to the transactions in the consensus machinery.
 * The latest request which gets accepted (see {@link ReplicatedTransactionStateMachine}) defines the currently valid
 * lock session id in this ordering. Each transaction that uses locking gets marked with a lock session id that was valid
 * at the time of acquiring it, but by the time a transaction commits it might no longer be valid, which in such case
 * would lead to the transaction being rejected and failed.
 *
 * The {@link ReplicatedLockTokenStateMachine} handles the token requests and considers only one to be valid at a time.
 * Meanwhile, {@link ReplicatedTransactionStateMachine} rejects any transactions that get committed under an
 * invalid token.
 */
// TODO: Fix lock exception hierarchy.
public class LeaderOnlyLockManager<MEMBER> implements Locks
{
    private final MEMBER myself;

    private final Replicator replicator;
    private final LeaderLocator<MEMBER> leaderLocator;
    private final Locks localLocks;
    private final LockTokenManager lockTokenManager;
    private final long leaderLockTokenTimeout;

    public LeaderOnlyLockManager( MEMBER myself, Replicator replicator, LeaderLocator<MEMBER> leaderLocator,
            Locks localLocks, LockTokenManager lockTokenManager, long leaderLockTokenTimeout )
    {
        this.myself = myself;
        this.replicator = replicator;
        this.leaderLocator = leaderLocator;
        this.localLocks = localLocks;
        this.lockTokenManager = lockTokenManager;
        this.leaderLockTokenTimeout = leaderLockTokenTimeout;
    }

    @Override
    public Locks.Client newClient()
    {
        return new LeaderOnlyLockClient( localLocks.newClient() );
    }

    /**
     * Acquires a valid token id owned by us or throws.
     */
    private synchronized int acquireTokenOrThrow()
    {
        LockToken currentToken = lockTokenManager.currentToken();
        if( myself.equals( currentToken.owner() ) )
        {
            return currentToken.id();
        }

        /* If we are not the leader then we will not even attempt to get the token,
           since only the leader should take locks. */
        ensureLeader();

        ReplicatedLockTokenRequest<MEMBER> lockTokenRequest = new ReplicatedLockTokenRequest<>(
                myself, lockTokenManager.nextCandidateId() );

        try
        {
            replicator.replicate( lockTokenRequest );
        }
        catch ( Replicator.ReplicationFailedException e )
        {
            throw new AcquireLockTimeoutException( e, "Could not acquire lock token." );
        }

        try
        {
            lockTokenManager.waitForTokenId( lockTokenRequest.id(), leaderLockTokenTimeout );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new AcquireLockTimeoutException( e, "Interrupted" );
        }

        currentToken = lockTokenManager.currentToken();

        if ( currentToken.id() != lockTokenRequest.id() || !myself.equals( currentToken.owner() ) )
        {
            throw new AcquireLockTimeoutException( "Failed to acquire requested lock token." );
        }

        return currentToken.id();
    }

    private void ensureLeader()
    {
        MEMBER leader;

        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderTimeoutException e )
        {
            throw new AcquireLockTimeoutException( e, "Could not acquire lock token." );
        }

        if( !leader.equals( myself ) )
        {
            throw new AcquireLockTimeoutException( "Should only attempt to take locks when leader." );
        }
    }

    @Override
    public void accept( Visitor visitor )
    {
        localLocks.accept( visitor );
    }

    @Override
    public void close()
    {
        localLocks.close();
    }

    /**
     * The LeaderOnlyLockClient delegates to a local lock client for taking locks, but makes
     * sure that it holds the cluster locking token before actually taking locks. If the token
     * is lost during a locking session then a transaction will either fail on a subsequent
     * local locking operation or during commit time.
     */
    private class LeaderOnlyLockClient implements Locks.Client
    {
        private final Client localClient;
        private int lockTokenId = LockToken.INVALID_LOCK_TOKEN_ID;

        public LeaderOnlyLockClient( Client localClient )
        {
            this.localClient = localClient;
        }

        /**
         * This ensures that a valid token was held at some point in time. It throws an
         * exception if it was held but was later lost or never could be taken to
         * begin with.
         */
        private void ensureHoldingToken()
        {
            if ( lockTokenId == LockToken.INVALID_LOCK_TOKEN_ID )
            {
                lockTokenId = acquireTokenOrThrow();
            }
            else if( lockTokenId != lockTokenManager.currentToken().id() )
            {
                throw new AcquireLockTimeoutException( "Local instance lost lock token." );
            }
        }

        @Override
        public void acquireShared( ResourceType resourceType, long resourceId ) throws AcquireLockTimeoutException
        {
            localClient.acquireShared( resourceType, resourceId );
        }

        @Override
        public void acquireExclusive( ResourceType resourceType, long resourceId ) throws AcquireLockTimeoutException
        {
            ensureHoldingToken();
            localClient.acquireExclusive( resourceType, resourceId );
        }

        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
        {
            ensureHoldingToken();
            return localClient.tryExclusiveLock( resourceType, resourceId );
        }

        @Override
        public boolean trySharedLock( ResourceType resourceType, long resourceId )
        {
            return localClient.trySharedLock( resourceType, resourceId );
        }

        @Override
        public void releaseShared( ResourceType resourceType, long resourceId )
        {
            localClient.releaseShared( resourceType, resourceId );
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long resourceId )
        {
            localClient.releaseExclusive( resourceType, resourceId );
        }

        @Override
        public void releaseAll()
        {
            localClient.releaseAll();
        }

        @Override
        public void stop()
        {
            localClient.stop();
        }

        @Override
        public void close()
        {
            localClient.close();
        }

        @Override
        public int getLockSessionId()
        {
            return lockTokenId;
        }
    }
}
