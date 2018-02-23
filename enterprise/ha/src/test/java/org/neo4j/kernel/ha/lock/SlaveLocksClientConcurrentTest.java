/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLockManager;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.lock.ResourceType;

import static java.time.Duration.ofMillis;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.com.RequestContext.anonymous;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.ha.lock.LockStatus.OK_LOCKED;
import static org.neo4j.kernel.impl.locking.LockTracer.NONE;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.time.Clocks.systemClock;

class SlaveLocksClientConcurrentTest
{
    private static ExecutorService executor;

    private Master master;
    private ForsetiLockManager lockManager;
    private RequestContextFactory requestContextFactory;
    private AvailabilityGuard availabilityGuard;

    @BeforeAll
    static void initExecutor()
    {
        executor = newCachedThreadPool();
    }

    @AfterAll
    static void closeExecutor()
    {
        executor.shutdownNow();
    }

    @BeforeEach
    void setUp()
    {
        master = mock( Master.class, new LockedOnMasterAnswer() );
        lockManager = new ForsetiLockManager( defaults(), systemClock(), ResourceTypes.values() );
        requestContextFactory = mock( RequestContextFactory.class );
        availabilityGuard = new AvailabilityGuard( systemClock(), mock( Log.class ) );

        when( requestContextFactory.newRequestContext( anyInt() ) ).thenReturn( anonymous( 1 ) );
    }

    @Test
    void readersCanAcquireLockAsSoonAsItReleasedOnMaster() throws InterruptedException
    {
        assertTimeout( ofMillis( 1000 ), () -> {
            SlaveLocksClient reader = createClient();
            SlaveLocksClient writer = createClient();

            CountDownLatch readerCompletedLatch = new CountDownLatch( 1 );
            CountDownLatch resourceLatch = new CountDownLatch( 1 );

            when( master.endLockSession( any( RequestContext.class ), anyBoolean() ) )
                    .then( new WaitLatchAnswer( resourceLatch, readerCompletedLatch ) );

            long nodeId = 10L;
            ResourceReader resourceReader =
                    new ResourceReader( reader, NODE, nodeId, resourceLatch, readerCompletedLatch );
            ResourceWriter resourceWriter = new ResourceWriter( writer, NODE, nodeId );

            executor.submit( resourceReader );
            executor.submit( resourceWriter );

            assertTrue( readerCompletedLatch.await( 1000, MILLISECONDS ),
                    "Reader should wait for writer to release locks before acquire" );
        } );
    }

    private SlaveLocksClient createClient()
    {
        return new SlaveLocksClient( master, lockManager.newClient(), lockManager, requestContextFactory,
                availabilityGuard, getInstance() );
    }

    private static class LockedOnMasterAnswer implements Answer
    {
        private final Response lockResult;

        LockedOnMasterAnswer()
        {
            lockResult = mock( Response.class );
            when( lockResult.response() ).thenReturn( new LockResult( OK_LOCKED ) );
        }

        @Override
        public Object answer( InvocationOnMock invocation )
        {
            return lockResult;
        }
    }

    private static class WaitLatchAnswer implements Answer<Void>
    {
        private final CountDownLatch resourceLatch;
        private final CountDownLatch resourceReleaseLatch;

        WaitLatchAnswer( CountDownLatch resourceLatch, CountDownLatch resourceReleaseLatch )
        {
            this.resourceLatch = resourceLatch;
            this.resourceReleaseLatch = resourceReleaseLatch;
        }

        @Override
        public Void answer( InvocationOnMock invocation ) throws Throwable
        {
            // releasing reader after local lock released
            resourceLatch.countDown();
            // waiting here for reader to finish read lock acquisition.
            // by this we check that local exclusive lock where released before releasing it on
            // master otherwise reader will be blocked forever
            resourceReleaseLatch.await();
            return null;
        }
    }

    private class ResourceWriter extends ResourceWorker
    {
        ResourceWriter( SlaveLocksClient locksClient, ResourceType resourceType, long id )
        {
            super( locksClient, resourceType, id );
        }

        @Override
        public void run()
        {
            locksClient.acquireExclusive( NONE, resourceType, id );
            locksClient.close();
        }
    }

    private class ResourceReader extends ResourceWorker
    {
        private final CountDownLatch resourceLatch;
        private final CountDownLatch resourceReleaseLatch;

        ResourceReader( SlaveLocksClient locksClient, ResourceType resourceType, long id, CountDownLatch resourceLatch,
                CountDownLatch resourceReleaseLatch )
        {
            super( locksClient, resourceType, id );
            this.resourceLatch = resourceLatch;
            this.resourceReleaseLatch = resourceReleaseLatch;
        }

        @Override
        public void run()
        {
            try
            {
                resourceLatch.await();
                locksClient.acquireShared( NONE, resourceType, id );
                resourceReleaseLatch.countDown();
                locksClient.close();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private abstract class ResourceWorker implements Runnable
    {
        final SlaveLocksClient locksClient;
        final ResourceType resourceType;
        final long id;

        ResourceWorker( SlaveLocksClient locksClient, ResourceType resourceType, long id )
        {
            this.locksClient = locksClient;
            this.resourceType = resourceType;
            this.id = id;
        }
    }
}
