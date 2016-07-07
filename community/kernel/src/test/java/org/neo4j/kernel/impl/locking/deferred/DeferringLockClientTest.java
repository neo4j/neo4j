/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking.deferred;

import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.Locks.ResourceType;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.test.RandomRule;

import static java.lang.Math.abs;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DeferringLockClientTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void releaseOfNotHeldSharedLockThrows() throws Exception
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        try
        {
            // WHEN
            client.releaseShared( ResourceTypes.NODE, 42 );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            // THEN
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void releaseOfNotHeldExclusiveLockThrows() throws Exception
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        try
        {
            // WHEN
            client.releaseExclusive( ResourceTypes.NODE, 42 );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            // THEN
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void shouldDeferAllLocks() throws Exception
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        // WHEN
        Set<LockUnit> expected = new HashSet<>();
        ResourceType[] types = ResourceTypes.values();
        for ( int i = 0; i < 10_000; i++ )
        {
            boolean exclusive = random.nextBoolean();
            LockUnit lockUnit = new LockUnit( random.among( types ), abs( random.nextLong() ), exclusive );

            if ( exclusive )
            {
                client.acquireExclusive( lockUnit.resourceType(), lockUnit.resourceId() );
            }
            else
            {
                client.acquireShared( lockUnit.resourceType(), lockUnit.resourceId() );
            }
            expected.add( lockUnit );
        }
        actualClient.assertRegisteredLocks( Collections.<LockUnit>emptySet() );
        client.acquireDeferredLocks();

        // THEN
        actualClient.assertRegisteredLocks( expected );
    }

    @Test
    public void shouldStopUnderlyingClient() throws Exception
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        // WHEN
        client.stop();

        // THEN
        verify( actualClient ).stop();
    }

    @Test
    public void shouldCloseUnderlyingClient() throws Exception
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        // WHEN
        client.close();

        // THEN
        verify( actualClient ).close();
    }

    @Test
    public void exclusiveLockAcquiredMultipleTimesCanNotBeReleasedAtOnce() throws Exception
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireExclusive( ResourceTypes.NODE, 1 );
        client.acquireExclusive( ResourceTypes.NODE, 1 );
        client.releaseExclusive( ResourceTypes.NODE, 1 );

        // WHEN
        client.acquireDeferredLocks();

        // THEN
        actualClient.assertRegisteredLocks( Collections.singleton( new LockUnit( ResourceTypes.NODE, 1, true ) ) );
    }

    @Test
    public void sharedLockAcquiredMultipleTimesCanNotBeReleasedAtOnce() throws Exception
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireShared( ResourceTypes.NODE, 1 );
        client.acquireShared( ResourceTypes.NODE, 1 );
        client.releaseShared( ResourceTypes.NODE, 1 );

        // WHEN
        client.acquireDeferredLocks();

        // THEN
        actualClient.assertRegisteredLocks( Collections.singleton( new LockUnit( ResourceTypes.NODE, 1, false ) ) );
    }

    @Test
    public void acquireBothSharedAndExclusiveLockThenReleaseShared()
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireShared( ResourceTypes.NODE, 1 );
        client.acquireExclusive( ResourceTypes.NODE, 1 );
        client.releaseShared( ResourceTypes.NODE, 1 );

        // WHEN
        client.acquireDeferredLocks();

        // THEN
        actualClient.assertRegisteredLocks( Collections.singleton( new LockUnit( ResourceTypes.NODE, 1, true ) ) );
    }

    @Test
    public void acquireBothSharedAndExclusiveLockThenReleaseExclusive()
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireShared( ResourceTypes.NODE, 1 );
        client.acquireExclusive( ResourceTypes.NODE, 1 );
        client.releaseExclusive( ResourceTypes.NODE, 1 );

        // WHEN
        client.acquireDeferredLocks();

        // THEN
        actualClient.assertRegisteredLocks( Collections.singleton( new LockUnit( ResourceTypes.NODE, 1, false ) ) );
    }

    private static class TestLocks extends LifecycleAdapter implements Locks
    {
        @Override
        public TestLocksClient newClient()
        {
            return new TestLocksClient();
        }

        @Override
        public void accept( Visitor visitor )
        {
        }
    }

    private static class TestLocksClient implements Locks.Client
    {
        private final Set<LockUnit> actualLockUnits = new HashSet<>();

        @Override
        public void acquireShared( ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
        {
            register( resourceType, false, resourceIds );
        }

        void assertRegisteredLocks( Set<LockUnit> expectedLocks )
        {
            assertEquals( expectedLocks, actualLockUnits );
        }

        private boolean register( ResourceType resourceType, boolean exclusive, long... resourceIds )
        {
            for ( long resourceId : resourceIds )
            {
                actualLockUnits.add( new LockUnit( resourceType, resourceId, exclusive ) );
            }
            return true;
        }

        @Override
        public void acquireExclusive( ResourceType resourceType, long... resourceIds )
                throws AcquireLockTimeoutException
        {
            register( resourceType, true, resourceIds );
        }

        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
        {
            return register( resourceType, true, resourceId );
        }

        @Override
        public boolean trySharedLock( ResourceType resourceType, long resourceId )
        {
            return register( resourceType, false, resourceId );
        }

        @Override
        public void releaseShared( ResourceType resourceType, long resourceId )
        {
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long resourceId )
        {
        }

        @Override
        public void stop()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public int getLockSessionId()
        {
            return 0;
        }
    }
}
