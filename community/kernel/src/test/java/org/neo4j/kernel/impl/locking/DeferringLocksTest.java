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
package org.neo4j.kernel.impl.locking;

import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.impl.locking.Locks.ResourceType;
import org.neo4j.kernel.impl.locking.deferred.DeferringLockClient;
import org.neo4j.kernel.impl.locking.deferred.LockUnit;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.test.RandomRule;

import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;

public class DeferringLocksTest
{
    @Rule
    public final RandomRule random = new RandomRule();

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
            LockUnit lockUnit = new LockUnit( random.among( types ), abs( random.nextLong() ), true );
            client.acquireExclusive( lockUnit.resourceType(), lockUnit.resourceId() );
            expected.add( lockUnit );
        }
        actualClient.assertRegisteredLocks( new HashSet<LockUnit>() );
        client.grabDeferredLocks();

        // THEN
        actualClient.assertRegisteredLocks( expected );
    }

    private static class TestLocks extends LifecycleAdapter implements Locks
    {
        private TestLocksClient client;

        @Override
        public TestLocksClient newClient()
        {
            return client = new TestLocksClient();
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
        public void releaseAll()
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
