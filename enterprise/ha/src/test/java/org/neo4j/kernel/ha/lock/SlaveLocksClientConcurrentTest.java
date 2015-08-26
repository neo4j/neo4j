/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLockManager;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlaveLocksClientConcurrentTest
{

    private static ExecutorService executor;
    private SlaveLocksClient reader;
    private SlaveLocksClient writer;

    private Master master;
    private ForsetiLockManager lockManager;
    private RequestContextFactory requestContextFactory;
    private AvailabilityGuard availabilityGuard;

    @BeforeClass
    public static void initExecutor()
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void closeExecutor()
    {
        executor.shutdownNow();
    }

    @Before
    public void setUp()
    {
        master = mock( Master.class, new LockedOnMasterAnswer() );
        lockManager = new ForsetiLockManager( ResourceTypes.values() );
        requestContextFactory = mock( RequestContextFactory.class );
        availabilityGuard = new AvailabilityGuard( Clock.SYSTEM_CLOCK );

        when( requestContextFactory.newRequestContext( Mockito.anyInt() ) )
                .thenReturn( RequestContext.anonymous( 1 ) );
    }

    @Test(timeout = 1000)
    public void readersCanAcquireLockAsSoonAsItReleasedOnMaster() throws InterruptedException
    {
        reader = createClient();
        writer = createClient();

        CountDownLatch readerCompletedLatch = new CountDownLatch( 1 );
        CountDownLatch resourceLatch = new CountDownLatch( 1 );

        when( master.endLockSession( any(RequestContext.class), anyBoolean() ) ).then(
                new WaitLatchAnswer( resourceLatch, readerCompletedLatch ) );

        long nodeId = 10l;
        ResourceReader resourceReader = new ResourceReader( reader, ResourceTypes.NODE, nodeId, resourceLatch, readerCompletedLatch);
        ResourceWriter resourceWriter = new ResourceWriter( writer, ResourceTypes.NODE, nodeId, resourceLatch );

        executor.submit( resourceReader );
        executor.submit( resourceWriter );

        assertTrue( "Reader should wait for writer to release local and master lock before acquire",
                readerCompletedLatch.await( 1000, TimeUnit.MILLISECONDS ) );
    }

    private SlaveLocksClient createClient()
    {
        return new SlaveLocksClient( master, lockManager.newClient(), lockManager,
                requestContextFactory, availabilityGuard, new TestConfiguration() );
    }

    private static class LockedOnMasterAnswer implements Answer
    {
        private Response lockResult;

        public LockedOnMasterAnswer()
        {
            lockResult = Mockito.mock( Response.class );
            when( lockResult.response() ).thenReturn( new LockResult( LockStatus.OK_LOCKED ) );
        }

        @Override
        public Object answer( InvocationOnMock invocation ) throws Throwable
        {
            return lockResult;
        }
    }

    private static class WaitLatchAnswer implements Answer<Void>
    {
        private final CountDownLatch resourceLatch;
        private final CountDownLatch resourceReleaseLatch;

        public WaitLatchAnswer( CountDownLatch resourceLatch, CountDownLatch resourceReleaseLatch )
        {
            this.resourceLatch = resourceLatch;
            this.resourceReleaseLatch = resourceReleaseLatch;
        }

        @Override
        public Void answer( InvocationOnMock invocation ) throws Throwable
        {
            resourceLatch.countDown();
            // waiting here for reader to finish read lock acquisition.
            // by this we check that local lock where released before releasing it on
            // master
            resourceReleaseLatch.await();
            return null;
        }
    }

    private class ResourceWriter extends ResourceWorker
    {
        public ResourceWriter(SlaveLocksClient locksClient, Locks.ResourceType resourceType, long id, CountDownLatch
                readerStartLatch )
        {
            super(locksClient, resourceType, id, readerStartLatch);
        }

        @Override
        public void run()
        {
            locksClient.acquireExclusive( resourceType, id );
            resourceLatch.countDown();
            locksClient.close();
        }
    }

    private class ResourceReader extends ResourceWorker
    {
        private CountDownLatch resourceReleaseLatch;

        public ResourceReader( SlaveLocksClient locksClient, Locks.ResourceType resourceType, long id, CountDownLatch
                resourceLatch, CountDownLatch resourceReleaseLatch )
        {
            super( locksClient, resourceType, id, resourceLatch );
            this.resourceReleaseLatch = resourceReleaseLatch;
        }

        @Override
        public void run()
        {
            try
            {
                resourceLatch.await();
                locksClient.acquireShared( resourceType, id );
                resourceReleaseLatch.countDown();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private abstract class ResourceWorker implements Runnable
    {
        protected SlaveLocksClient locksClient;
        protected Locks.ResourceType resourceType;
        protected long id;
        protected CountDownLatch resourceLatch;

        public ResourceWorker( SlaveLocksClient locksClient, Locks.ResourceType resourceType, long id, CountDownLatch resourceLatch )
        {
            this.locksClient = locksClient;
            this.resourceType = resourceType;
            this.id = id;
            this.resourceLatch = resourceLatch;
        }
    }

    private static class TestConfiguration implements SlaveLockManager.Configuration
    {
        @Override
        public long getAvailabilityTimeout()
        {
            return 100;
        }
    }
}