/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class ReferenceCacheTest
{
    static class SpyCreatingWeakValueFactory implements ReferenceWithKey.Factory
    {
        ArrayList<ReferenceWithKey> weakValues = new ArrayList<>();
        private final ArrayList<Object> hardReferencesToStopGC = new ArrayList<>();
        private final ReferenceWithKey.Factory refFactory;

        SpyCreatingWeakValueFactory( ReferenceWithKey.Factory referenceFactory )
        {
            refFactory = referenceFactory;
        }

        @Override
        public <FK, FV> ReferenceWithKey<FK, FV> newReference( FK key, FV value, ReferenceQueue<? super FV> queue )
        {
            ReferenceWithKey<FK, FV> ref = Mockito.spy( refFactory.newReference( key, value, queue ) );
            hardReferencesToStopGC.add( value );
            weakValues.add( ref );
            return ref;
        }

        public void clearAndQueueReferenceNo( int index )
        {
            ReferenceWithKey val = weakValues.get( index );
            val.clear();
            val.enqueue();
        }

        public void reset()
        {
            weakValues.clear();
            hardReferencesToStopGC.clear();
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters()
    {
        return asList( new Object[][]{
            {new SpyCreatingWeakValueFactory( WeakValue.WEAK_VALUE_FACTORY )},
            {new SpyCreatingWeakValueFactory( SoftValue.SOFT_VALUE_FACTORY )}
        });
    }

    private SpyCreatingWeakValueFactory spyFactory;

    public ReferenceCacheTest( SpyCreatingWeakValueFactory factory )
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
    public void shouldHandleReferenceGarbageCollectedDuringGet() throws Exception
    {
        // Given
        ReferenceCache<TestCacheTypes.Entity> cache = new ReferenceCache<>( "MyCache!", spyFactory );

        final TestCacheTypes.Entity originalEntity = new TestCacheTypes.Entity( 0 );
        cache.put( originalEntity );

        // Clear the weak reference that the cache will have created (emulating GC doing this)
        when( spyFactory.weakValues.get( 0 ).get() ).thenAnswer( entityFirstTimeNullAfterThat( originalEntity ) );

        // When
        TestCacheTypes.Entity returnedEntity = cache.get( 0 );

        // Then
        assertEquals(originalEntity, returnedEntity);
    }

    private Answer<Object> entityFirstTimeNullAfterThat( final TestCacheTypes.Entity originalEntity )
    {
        return new Answer<Object>()
        {
            int invocations = 0;

            @Override
            public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                if(invocations++ == 0)
                {
                    return originalEntity;
                } else
                {
                    return null;
                }
            }
        };
    }

}
