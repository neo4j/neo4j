/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import static org.neo4j.kernel.impl.cache.ReferenceCache.MAX_NUM_PUT_BEFORE_POLL;

@RunWith(Parameterized.class)
public class ReferenceCacheTest
{

    static class SpyCreatingValueFactory implements ReferenceWithKey.Factory
    {
        ArrayList<ReferenceWithKey> values = new ArrayList<>();
        private final ArrayList<Object> hardReferencesToStopGC = new ArrayList<>();
        private final ReferenceWithKey.Factory refFactory;

        SpyCreatingValueFactory( ReferenceWithKey.Factory referenceFactory )
        {
            refFactory = referenceFactory;
        }

        @Override
        public <FK, FV> ReferenceWithKey<FK, FV> newReference( FK key, FV value, ReferenceQueue<? super FV> queue )
        {
            ReferenceWithKey<FK, FV> ref = Mockito.spy( refFactory.newReference( key, value, queue ) );
            hardReferencesToStopGC.add( value );
            values.add( ref );
            return ref;
        }

        public void clearAndQueueReferenceNo( int index )
        {
            ReferenceWithKey val = values.get( index );
            val.clear();
            val.enqueue();
        }

        public void reset()
        {
            values.clear();
            hardReferencesToStopGC.clear();
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters()
    {
        return asList( new Object[][]{
                {new SpyCreatingValueFactory( WeakValue.WEAK_VALUE_FACTORY )},
                {new SpyCreatingValueFactory( SoftValue.SOFT_VALUE_FACTORY )}
        });
    }

    private SpyCreatingValueFactory spyFactory;

    public ReferenceCacheTest( SpyCreatingValueFactory factory )
    {
        this.spyFactory = factory;
        spyFactory.reset(); // Instance is shared across tests
    }

    @Test
    public void shouldHandleExistingCacheEntryBeingGarbageCollectedDuringPutIfAbsent() throws Exception
    {
        // Given
        ReferenceCache<TestCacheTypes.Entity> cache = new ReferenceCache<>( "MyCache!", spyFactory );

        TestCacheTypes.Entity originalEntity = new TestCacheTypes.Entity( 0 );
        cache.put( originalEntity );

        // Clear the weak reference that the cache will have created (emulating GC doing this)
        spyFactory.clearAndQueueReferenceNo( 0 );

        // When
        TestCacheTypes.Entity newEntity = new TestCacheTypes.Entity( 0 );
        TestCacheTypes.Entity returnedEntity = cache.put( newEntity );

        // Then
        assertEquals(newEntity, returnedEntity);
        assertEquals(newEntity, cache.get(0));
    }

    @Test
    public void shouldForceACacheCleanupAfterManyPutsWithoutReading() throws Exception
    {
        // Given
        ReferenceCache<TestCacheTypes.Entity> cache = new ReferenceCache<>( "MyCache!", spyFactory );

        for ( int i = 0; i < MAX_NUM_PUT_BEFORE_POLL; i++ )
        {
            cache.put( new TestCacheTypes.Entity( i ) );
        }

        // Clear the weak reference that the cache will have created (emulating GC doing this)
        for ( int i = 0; i < MAX_NUM_PUT_BEFORE_POLL / 2; i++ )
        {
            spyFactory.clearAndQueueReferenceNo( i );
        }

        // this should trigger a poll and remove the collected values from the cache
        cache.put( new TestCacheTypes.Entity( MAX_NUM_PUT_BEFORE_POLL ) );

        assertEquals( (MAX_NUM_PUT_BEFORE_POLL / 2) + 1, cache.size() );
        for ( int i = 0; i < MAX_NUM_PUT_BEFORE_POLL / 2; i++ )
        {
            assertNull( cache.get( i ) );
        }
    }

    @Test
    public void shouldReturnTheValueIfNotGCed() throws Exception
    {
        // Given
        ReferenceCache<TestCacheTypes.Entity> cache = new ReferenceCache<>( "MyCache!", spyFactory );

        TestCacheTypes.Entity entity = new TestCacheTypes.Entity( 0 );
        cache.put( entity );

        // When
        TestCacheTypes.Entity returnedEntity = cache.get( 0 );

        // Then
        assertEquals( entity, returnedEntity );
    }

    @Test
    public void shouldHandleReferenceGarbageCollectedDuringGet() throws Exception
    {
        // Given
        ReferenceCache<TestCacheTypes.Entity> cache = new ReferenceCache<>( "MyCache!", spyFactory );

        cache.put( new TestCacheTypes.Entity( 0 ) );
        cache.put( new TestCacheTypes.Entity( 1 ) );
        cache.put( new TestCacheTypes.Entity( 2 ) );

        // Clear the weak reference that the cache will have created (emulating GC doing this)
        spyFactory.clearAndQueueReferenceNo( 0 );
        spyFactory.clearAndQueueReferenceNo( 1 );

        // When
        TestCacheTypes.Entity returnedEntity = cache.get( 1 );

        // Then
        assertNull( returnedEntity );
        assertEquals( 1, cache.size() );
    }

    @Test
    public void shouldPollAfterAPutAllInvocation()
    {
        // Given
        ReferenceCache<TestCacheTypes.Entity> cache = new ReferenceCache<>( "MyCache!", spyFactory );

        for ( int i = 0; i < MAX_NUM_PUT_BEFORE_POLL / 2; i++ )
        {
            cache.put( new TestCacheTypes.Entity( i ) );
        }

        // Clear the weak reference that the cache will have created (emulating GC doing this)
        for ( int i = 0; i < MAX_NUM_PUT_BEFORE_POLL / 3; i++ )
        {
            spyFactory.clearAndQueueReferenceNo( i );
        }


        List<TestCacheTypes.Entity> entities = new ArrayList<>();
        for ( int i = MAX_NUM_PUT_BEFORE_POLL / 2; i < MAX_NUM_PUT_BEFORE_POLL + 1; i++ )
        {
            entities.add( new TestCacheTypes.Entity( i ) );
        }

        // this should trigger a poll and remove the collected values from the cache
        cache.putAll( entities );

        assertEquals( MAX_NUM_PUT_BEFORE_POLL - (MAX_NUM_PUT_BEFORE_POLL / 3) + 1, cache.size() );
        for ( int i = 0; i < MAX_NUM_PUT_BEFORE_POLL / 3; i++ )
        {
            assertNull( cache.get( i ) );
        }
    }

    @Test
    public void shouldHandleReferenceGarbageCollectedDuringRemove() throws Exception
    {
        // Given
        ReferenceCache<TestCacheTypes.Entity> cache = new ReferenceCache<>( "MyCache!", spyFactory );

        cache.put( new TestCacheTypes.Entity( 0 ) );
        cache.put( new TestCacheTypes.Entity( 1 ) );
        cache.put( new TestCacheTypes.Entity( 2 ) );
        cache.put( new TestCacheTypes.Entity( 3 ) );

        spyFactory.clearAndQueueReferenceNo( 0 );
        spyFactory.clearAndQueueReferenceNo( 1 );

        // When
        TestCacheTypes.Entity returnedEntity = cache.remove( 1 );

        // Then
        assertNull( returnedEntity );
        assertEquals( 2, cache.size() );
        assertNull( cache.get( 0 ) );
        assertNull( cache.get( 1 ) );
        assertNotNull( cache.get( 2 ) );
        assertNotNull( cache.get( 3 ) );
    }
}
