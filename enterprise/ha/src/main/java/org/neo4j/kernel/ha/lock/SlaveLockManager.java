/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha.lock;

import org.neo4j.com.Response;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.transaction.LockType.READ;
import static org.neo4j.kernel.impl.transaction.LockType.WRITE;

public class SlaveLockManager extends LifecycleAdapter implements Locks
{
    private final RequestContextFactory requestContextFactory;
    private final Locks local;
    private final Master master;
    private final HaXaDataSourceManager xaDsm;
    private final AbstractTransactionManager txManager;
    private final RemoteTxHook txHook;
    private final AvailabilityGuard availabilityGuard;
    private final Configuration config;

    public static interface Configuration
    {
        long getAvailabilityTimeout();
    }

    public SlaveLockManager( Locks localLocks, RequestContextFactory requestContextFactory, Master master,
            HaXaDataSourceManager xaDsm, AbstractTransactionManager txManager, RemoteTxHook txHook,
            AvailabilityGuard availabilityGuard, Configuration config )
    {
        this.requestContextFactory = requestContextFactory;
        this.xaDsm = xaDsm;
        this.txManager = txManager;
        this.txHook = txHook;
        this.availabilityGuard = availabilityGuard;
        this.config = config;
        this.local = localLocks;
        this.master = master;
    }

    @Override
    public Client newClient()
    {
        return new SlaveLocksClient(master, local.newClient(), local, requestContextFactory, xaDsm, txManager, txHook,
                availabilityGuard, config );
    }

    @Override
    public void dumpLocks( StringLogger out )
    {
        local.dumpLocks( out );
    }

    @Override
    public String implementationId()
    {
        return "slave-locks";
    }

    private static class SlaveLocksClient implements Client
    {
        private final Master master;
        private final Client client;
        private final Locks localLockManager;
        private final RequestContextFactory requestContextFactory;
        private final HaXaDataSourceManager xaDsm;
        private final AbstractTransactionManager txManager;
        private final RemoteTxHook txHook;
        private final AvailabilityGuard availabilityGuard;
        private final Configuration config;

        public SlaveLocksClient( Master master, Client local, Locks localLockManager, RequestContextFactory requestContextFactory,
                                 HaXaDataSourceManager xaDsm, AbstractTransactionManager txManager, RemoteTxHook txHook,
                                 AvailabilityGuard availabilityGuard, Configuration config )
        {
            this.master = master;
            this.client = local;
            this.localLockManager = localLockManager;
            this.requestContextFactory = requestContextFactory;
            this.xaDsm = xaDsm;
            this.txManager = txManager;
            this.txHook = txHook;
            this.availabilityGuard = availabilityGuard;
            this.config = config;
        }

        @Override
        public void acquireShared( ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
        {
            if ( getReadLockOnMaster( resourceType, resourceIds ) )
            {
                if ( !client.trySharedLock( resourceType, resourceIds ) )
                {
                    throw new LocalDeadlockDetectedException( client, localLockManager, resourceType, resourceIds, READ );
                }
            }
        }

        @Override
        public void acquireExclusive( ResourceType resourceType, long... resourceIds ) throws
                AcquireLockTimeoutException
        {
            if ( acquireExclusiveOnMaster( resourceType, resourceIds ) )
            {
                if ( !client.tryExclusiveLock( resourceType, resourceIds ) )
                {
                    throw new LocalDeadlockDetectedException( client, localLockManager, resourceType, resourceIds, WRITE );
                }
            }
        }

        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long... resourceIds )
        {
            throw newUnsupportedDirectTryLockUsageException();
        }

        @Override
        public boolean trySharedLock( ResourceType resourceType, long... resourceIds )
        {
            throw newUnsupportedDirectTryLockUsageException();
        }

        @Override
        public void releaseShared( ResourceType resourceType, long... resourceIds )
        {
            client.releaseShared( resourceType, resourceIds );
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long... resourceIds )
        {
            client.releaseExclusive( resourceType, resourceIds );
        }

        @Override
        public void releaseAllShared()
        {
            client.releaseAllShared();
        }

        @Override
        public void releaseAllExclusive()
        {
            client.releaseAllExclusive();
        }

        @Override
        public void releaseAll()
        {
            client.releaseAll();
        }

        @Override
        public void close()
        {
            client.close();
        }

        private boolean getReadLockOnMaster( ResourceType resourceType, long ... resourceId )
        {
            if ( resourceType == ResourceTypes.NODE
                || resourceType == ResourceTypes.RELATIONSHIP
                || resourceType == ResourceTypes.GRAPH_PROPS
                || resourceType == ResourceTypes.LEGACY_INDEX )
            {
                makeSureTxHasBeenInitialized();
                return receiveLockResponse(
                    master.acquireSharedLock( requestContextFactory.newRequestContext(), resourceType, resourceId ));
            }
            else
            {
                return true;
            }
        }

        private boolean acquireExclusiveOnMaster( ResourceType resourceType, long ... resourceId )
        {
            makeSureTxHasBeenInitialized();
            return receiveLockResponse(
                    master.acquireExclusiveLock( requestContextFactory.newRequestContext(), resourceType, resourceId ));
        }

        private boolean receiveLockResponse( Response<LockResult> response )
        {
            LockResult result = xaDsm.applyTransactions( response );
            switch ( result.getStatus() )
            {
                case DEAD_LOCKED:
                    throw new DeadlockDetectedException( result.getDeadlockMessage() );
                case NOT_LOCKED:
                    throw new UnsupportedOperationException();
                case OK_LOCKED:
                    break;
                default:
                    throw new UnsupportedOperationException( result.toString() );
            }

            return true;
        }


        private void makeSureTxHasBeenInitialized()
        {
            if ( !availabilityGuard.isAvailable( config.getAvailabilityTimeout() ) )
            {
                // TODO Specific exception instead?
                throw new RuntimeException( "Timed out waiting for database to allow operations to proceed. "
                        + availabilityGuard.describeWhoIsBlocking() );
            }

            txHook.remotelyInitializeTransaction( txManager.getEventIdentifier(), txManager.getTransactionState() );
        }

        private UnsupportedOperationException newUnsupportedDirectTryLockUsageException()
        {
            return new UnsupportedOperationException( "At the time of adding \"try lock\" semantics there was no usage of " +
                    getClass().getSimpleName() + " calling it directly. It was designed to be called on a local " +
                    LockManager.class.getSimpleName() + " delegated to from within the waiting version" );
        }
    }
}
