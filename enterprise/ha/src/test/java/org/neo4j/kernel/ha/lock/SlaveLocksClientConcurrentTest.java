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
package org.neo4j.kernel.ha.lock;


import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLockManager;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlaveLocksClientConcurrentTest
{
    private static ExecutorService executor;

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
        lockManager = new ForsetiLockManager( Config.defaults(), Clocks.systemClock(), ResourceTypes.values() );
        requestContextFactory = mock( RequestContextFactory.class );
        availabilityGuard = new AvailabilityGuard( Clocks.systemClock(), mock( Log.class ) );

        when( requestContextFactory.newRequestContext( Mockito.anyInt() ) )
                .thenReturn( RequestContext.anonymous( 1 ) );
    }

    @Test( timeout = 1000 )
    public void readersCanAcquireLockAsSoonAsItReleasedOnMaster() throws InterruptedException
    {
        SlaveLocksClient reader = createClient();
        SlaveLocksClient writer = createClient();

        CountDownLatch readerCompletedLatch = new CountDownLatch( 1 );
        CountDownLatch resourceLatch = new CountDownLatch( 1 );

        when( master.endLockSession( any( RequestContext.class ), anyBoolean() ) ).then(
                new WaitLatchAnswer( resourceLatch, readerCompletedLatch ) );

        long nodeId = 10L;
        ResourceReader resourceReader =
                new ResourceReader( reader, ResourceTypes.NODE, nodeId, resourceLatch, readerCompletedLatch );
        ResourceWriter resourceWriter = new ResourceWriter( writer, ResourceTypes.NODE, nodeId );

        executor.submit( resourceReader );
        executor.submit( resourceWriter );

        assertTrue( "Reader should wait for writer to release locks before acquire",
                readerCompletedLatch.await( 1000, TimeUnit.MILLISECONDS ) );
    }

    private SlaveLocksClient createClient()
    {
        return new SlaveLocksClient( master, lockManager.newClient(), lockManager,
                requestContextFactory, availabilityGuard, NullLogProvider.getInstance() );
    }

    private static class LockedOnMasterAnswer implements Answer
    {
        private final Response lockResult;

        LockedOnMasterAnswer()
        {
            lockResult = Mockito.mock( Response.class );
            when( lockResult.response() ).thenReturn( new LockResult( LockStatus.OK_LOCKED ) );
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
            locksClient.acquireExclusive( LockTracer.NONE, resourceType, id );
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
                locksClient.acquireShared( LockTracer.NONE, resourceType, id );
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
        protected final SlaveLocksClient locksClient;
        protected final ResourceType resourceType;
        protected final long id;

        ResourceWorker( SlaveLocksClient locksClient, ResourceType resourceType, long id )
        {
            this.locksClient = locksClient;
            this.resourceType = resourceType;
            this.id = id;
        }
    }
}
