/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.locks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.replication.ReplicationFailureException;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionStateMachine;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.storageengine.api.lock.ResourceType;

import static org.neo4j.kernel.api.exceptions.Status.Cluster.NoLeaderAvailable;
import static org.neo4j.kernel.api.exceptions.Status.Cluster.NotALeader;
import static org.neo4j.kernel.api.exceptions.Status.Cluster.ReplicationFailure;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Interrupted;

/**
 * Each member of the cluster uses its own {@link LeaderOnlyLockManager} which wraps a local {@link Locks} manager.
 * The validity of local lock managers is synchronized by using a token which gets requested by each server as necessary
 * and if the request is granted then the associated id can be used to identify a unique lock session in the cluster.
 * <p/>
 * The fundamental strategy is to only allow locks on the leader. This has the benefit of minimizing the synchronization
 * to only concern the single token but it also means that non-leaders should not even attempt to request the token or
 * significant churn of this single resource will lead to a high level of aborted transactions.
 * <p/>
 * The token requests carry a candidate id and they get ordered with respect to the transactions in the consensus
 * machinery.
 * The latest request which gets accepted (see {@link ReplicatedTransactionStateMachine}) defines the currently valid
 * lock session id in this ordering. Each transaction that uses locking gets marked with a lock session id that was
 * valid
 * at the time of acquiring it, but by the time a transaction commits it might no longer be valid, which in such case
 * would lead to the transaction being rejected and failed.
 * <p/>
 * The {@link ReplicatedLockTokenStateMachine} handles the token requests and considers only one to be valid at a time.
 * Meanwhile, {@link ReplicatedTransactionStateMachine} rejects any transactions that get committed under an
 * invalid token.
 */

// TODO: Fix lock exception usage when lock exception hierarchy has been fixed.
public class LeaderOnlyLockManager implements Locks
{
    public static final String LOCK_NOT_ON_LEADER_ERROR_MESSAGE = "Should only attempt to take locks when leader.";

    private final MemberId myself;

    private final Replicator replicator;
    private final LeaderLocator leaderLocator;
    private final Locks localLocks;
    private final ReplicatedLockTokenStateMachine lockTokenStateMachine;

    public LeaderOnlyLockManager( MemberId myself, Replicator replicator, LeaderLocator leaderLocator, Locks localLocks,
            ReplicatedLockTokenStateMachine lockTokenStateMachine )
    {
        this.myself = myself;
        this.replicator = replicator;
        this.leaderLocator = leaderLocator;
        this.localLocks = localLocks;
        this.lockTokenStateMachine = lockTokenStateMachine;
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
        LockToken currentToken = lockTokenStateMachine.currentToken();
        if ( myself.equals( currentToken.owner() ) )
        {
            return currentToken.id();
        }

        /* If we are not the leader then we will not even attempt to get the token,
           since only the leader should take locks. */
        ensureLeader();

        ReplicatedLockTokenRequest lockTokenRequest =
                new ReplicatedLockTokenRequest( myself, LockToken.nextCandidateId( currentToken.id() ) );

        Future<Object> future;
        try
        {
            future = replicator.replicate( lockTokenRequest, true );
        }
        catch ( ReplicationFailureException e )
        {
            throw new AcquireLockTimeoutException( e, "Replication failure acquiring lock token.", ReplicationFailure );
        }

        try
        {
            boolean success = (boolean) future.get();
            if ( success )
            {
                return lockTokenRequest.id();
            }
            else
            {
                throw new AcquireLockTimeoutException( "Failed to acquire lock token. Was taken by another candidate.",
                        NotALeader );
            }
        }
        catch ( ExecutionException e )
        {
            throw new AcquireLockTimeoutException( e, "Failed to acquire lock token.", NotALeader );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new AcquireLockTimeoutException( e, "Failed to acquire lock token.", Interrupted );
        }
    }

    private void ensureLeader()
    {
        MemberId leader;

        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            throw new AcquireLockTimeoutException( e, "Could not acquire lock token.", NoLeaderAvailable );
        }

        if ( !leader.equals( myself ) )
        {
            throw new AcquireLockTimeoutException( LOCK_NOT_ON_LEADER_ERROR_MESSAGE, NotALeader );
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

        LeaderOnlyLockClient( Client localClient )
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
            else if ( lockTokenId != lockTokenStateMachine.currentToken().id() )
            {
                throw new AcquireLockTimeoutException( "Local instance lost lock token.", NotALeader );
            }
        }

        @Override
        public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceId ) throws AcquireLockTimeoutException
        {
            localClient.acquireShared( tracer, resourceType, resourceId );
        }

        @Override
        public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceId ) throws AcquireLockTimeoutException
        {
            ensureHoldingToken();
            localClient.acquireExclusive( tracer, resourceType, resourceId );
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
        public boolean reEnterShared( ResourceType resourceType, long resourceId )
        {
            return localClient.reEnterShared( resourceType, resourceId );
        }

        @Override
        public boolean reEnterExclusive( ResourceType resourceType, long resourceId )
        {
            ensureHoldingToken();
            return localClient.reEnterExclusive( resourceType, resourceId );
        }

        @Override
        public void releaseShared( ResourceType resourceType, long... resourceIds )
        {
            localClient.releaseShared( resourceType, resourceIds );
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long... resourceIds )
        {
            localClient.releaseExclusive( resourceType, resourceIds );
        }

        @Override
        public void prepare()
        {
            localClient.prepare();
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

        @Override
        public Stream<? extends ActiveLock> activeLocks()
        {
            return localClient.activeLocks();
        }

        @Override
        public long activeLockCount()
        {
            return localClient.activeLockCount();
        }
    }
}
