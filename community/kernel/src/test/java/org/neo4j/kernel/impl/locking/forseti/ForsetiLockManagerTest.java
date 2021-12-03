/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.locking.forseti;

import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.time.Clocks;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.Race.throwing;

@ExtendWith( RandomExtension.class )
class ForsetiLockManagerTest
{
    @Inject
    RandomSupport random;

    Config config;
    ForsetiLockManager manager;

    @BeforeEach
    void setUp()
    {
        config = Config.defaults( GraphDatabaseInternalSettings.lock_manager_verbose_deadlocks, true );
        manager = new ForsetiLockManager( config, Clocks.nanoClock(), ResourceTypes.values() );
    }

    @AfterEach
    void tearDown()
    {
        manager.close();
    }

    @Test
    void testMultipleClientsSameTxId() throws Throwable
    {
        //This tests an issue where using the same transaction id for two concurrently used clients would livelock
        //Having non-unique transaction ids should not happen and be addressed on its own but the LockManager should still not hang
        final int TX_ID = 0;

        Race race = new Race();
        race.addContestants( 3, throwing( () -> {
            try ( Locks.Client client = manager.newClient() )
            {
                client.initialize( LeaseService.NoLeaseClient.INSTANCE, TX_ID, EmptyMemoryTracker.INSTANCE, config ); // Note same TX_ID

                if ( random.nextBoolean() )
                {
                    client.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 0 );
                }
                else
                {
                    client.acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 0 );
                }
            }
        } ) );

        race.go( 3, TimeUnit.MINUTES ); //Should not timeout (livelock)
    }

    @Test
    void testSameThreadMultipleClientCommitDirectDeadlock()
    {
        config = Config.defaults( GraphDatabaseSettings.lock_acquisition_timeout, Duration.ofMinutes( 1 ) );

        //Given
        try ( Locks.Client client1 = manager.newClient();
              Locks.Client client2 = manager.newClient(); )
        {
            client1.initialize( LeaseService.NoLeaseClient.INSTANCE, 1, EmptyMemoryTracker.INSTANCE, config );
            client2.initialize( LeaseService.NoLeaseClient.INSTANCE, 2, EmptyMemoryTracker.INSTANCE, config );

            //When
            client2.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 0 );
            client2.prepareForCommit();

            //Then (1 waiting for 2), 2 committing
            assertThatThrownBy( () -> client1.acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 0 ) )
                    .isInstanceOf( DeadlockDetectedException.class ).hasMessageContaining( "committing on the same thread" );

        }
    }

    @Test
    void testSameThreadMultipleClientCommitIndirectDeadlock() throws TimeoutException
    {
        config = Config.defaults( GraphDatabaseSettings.lock_acquisition_timeout, Duration.ofMinutes( 1 ) );

        //Given
        try ( OtherThreadExecutor executor1 = new OtherThreadExecutor( "test1" );
              OtherThreadExecutor executor2 = new OtherThreadExecutor( "test2" );
              Locks.Client client1 = manager.newClient();
              Locks.Client client2 = manager.newClient();
              Locks.Client client3 = manager.newClient();
              Locks.Client client4 = manager.newClient() )
        {
            client1.initialize( LeaseService.NoLeaseClient.INSTANCE, 1, EmptyMemoryTracker.INSTANCE, config );
            client2.initialize( LeaseService.NoLeaseClient.INSTANCE, 2, EmptyMemoryTracker.INSTANCE, config );
            client3.initialize( LeaseService.NoLeaseClient.INSTANCE, 3, EmptyMemoryTracker.INSTANCE, config );
            client4.initialize( LeaseService.NoLeaseClient.INSTANCE, 4, EmptyMemoryTracker.INSTANCE, config );

            //When
            client4.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 0 );
            executor1.executeDontWait( () ->
                                      {
                                          client3.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 1 );
                                          client3.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 0 );
                                          return null;
                                      } );
            executor1.waitUntilWaiting( details -> details.isAt( ForsetiClient.class, "acquireExclusive" ) );
            executor2.executeDontWait( () ->
                                      {
                                          client2.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 2 );
                                          client2.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 1 );
                                          return null;
                                      } );
            executor2.waitUntilWaiting( details -> details.isAt( ForsetiClient.class, "acquireExclusive" ) );

            client4.prepareForCommit();
            //Then (1 waiting for 2 waiting for 3 waiting for 4), 4 committing
            assertThatThrownBy( () -> client1.acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 2 ) )
                    .isInstanceOf( DeadlockDetectedException.class ).hasMessageContaining( "committing on the same thread" );
        }
    }

    @Test
    void lockClientsShouldNotHaveMutatingEqualsAndHashCode()
    {
        int uniqueClients = 10_000;
        var allClientsSet = new HashSet<Locks.Client>( uniqueClients );
        var allClientsList = new ArrayList<Locks.Client>( uniqueClients );

        for ( int i = 0; i < uniqueClients; i++ )
        {
            Locks.Client client = manager.newClient();
            allClientsSet.add( client );
            allClientsList.add( client );
            client.initialize( LeaseService.NoLeaseClient.INSTANCE, i, EmptyMemoryTracker.INSTANCE, config );
        }

        for ( int i = 0; i < 100; i++ )
        {
            allClientsSet.forEach( client -> client.initialize( LeaseService.NoLeaseClient.INSTANCE, random.nextLong(), EmptyMemoryTracker.INSTANCE, config ) );
        }

        allClientsList.forEach( o -> assertTrue( allClientsSet.remove( o ) ) );
        assertThat( allClientsSet ).isEmpty();
    }

    @Test
    void shouldHaveCorrectActiveLocks()
    {
        try ( Locks.Client client = manager.newClient() )
        {
            client.initialize( LeaseService.NoLeaseClient.INSTANCE, 0, EmptyMemoryTracker.INSTANCE, config );
            takeAndAssertActiveLocks( client );
        }
    }

    @Test
    void shouldHaveCorrectActiveLocksConcurrently() throws Throwable
    {
        Race race = new Race();
        AtomicLong tx = new AtomicLong();
        AtomicBoolean done = new AtomicBoolean();
        BinaryLatch start = new BinaryLatch();
        race.addContestants( 5, throwing( () ->
        {
            start.release();
            while ( !done.get() )
            {
                try ( Locks.Client client = manager.newClient() )
                {
                    client.initialize( LeaseService.NoLeaseClient.INSTANCE, tx.incrementAndGet(), EmptyMemoryTracker.INSTANCE, config );
                    long id = random.nextLong( 10 );
                    if ( random.nextBoolean() )
                    {
                        client.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, id );
                    }
                    else
                    {
                        client.acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP, id );
                    }
                    Thread.sleep( 1 );
                }
                catch ( Exception ignored )
                {
                }
            }
        } ) );
        Race.Async async = race.goAsync();
        try ( Locks.Client client = manager.newClient() )
        {
            client.initialize( LeaseService.NoLeaseClient.INSTANCE, tx.incrementAndGet(), EmptyMemoryTracker.INSTANCE, config );
            start.await();
            takeAndAssertActiveLocks( client );
        }
        finally
        {
            done.set( true );
        }
        async.await( 1, TimeUnit.MINUTES );
    }

    private void takeAndAssertActiveLocks( Locks.Client client )
    {
        Map<Long,Integer> exclusiveLocks = new HashMap<>();
        Map<Long,Integer> sharedLocks = new HashMap<>();
        for ( int i = 0; i < 10000; i++ )
        {
            boolean takeLock = exclusiveLocks.isEmpty() && sharedLocks.isEmpty() || random.nextBoolean();
            if ( takeLock )
            {
                long id = random.nextLong( 10 );
                if ( random.nextBoolean() )
                {
                    client.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, id );
                    exclusiveLocks.compute( id, ( key, count ) -> count == null ? 1 : count + 1 );
                }
                else
                {
                    client.acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP, id );
                    sharedLocks.compute( id, ( key, count ) -> count == null ? 1 : count + 1 );
                }
            }
            else
            {
                if ( sharedLocks.isEmpty() || !exclusiveLocks.isEmpty() && random.nextBoolean() )
                {
                    Long toRemove = random.among( exclusiveLocks.keySet().toArray( new Long[0] ) );
                    client.releaseExclusive( ResourceTypes.RELATIONSHIP, toRemove );
                    exclusiveLocks.compute( toRemove, ( key, count ) -> count == 1 ? null : count - 1 );
                }
                else
                {
                    Long toRemove = random.among( sharedLocks.keySet().toArray( new Long[0] ) );
                    client.releaseShared( ResourceTypes.RELATIONSHIP, toRemove );
                    sharedLocks.compute( toRemove, ( key, count ) -> count == 1 ? null : count - 1 );
                }
            }

            int totalLocks = Sets.mutable.withAll( exclusiveLocks.keySet() ).withAll( sharedLocks.keySet() ).size();
            List<ActiveLock> activeLocks = client.activeLocks().collect( Collectors.toList() );

            assertThat( client.activeLockCount() ).isEqualTo( totalLocks );
            assertThat( activeLocks.size() ).isEqualTo( client.activeLockCount() );
            activeLocks.forEach(
                    lock -> assertThat( lock.lockType().equals( LockType.EXCLUSIVE ) ? exclusiveLocks : sharedLocks ).containsKey( lock.resourceId() ) );
        }
    }
}
