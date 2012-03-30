/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.Before;
import org.junit.Test;

public class TestGCResistantCache
{
    private GCResistantCache<Entity> cache;

    @Before
    public void setup()
    {
        cache = new GCResistantCache<Entity>( new AtomicReferenceArray<Entity>( 10 ) );
    }
    
    @Test
    public void assertThatPutPutsSomething()
    {
        long key = 5;
        Entity entity = new Entity( key, 10 );
        cache.put( entity );
        assertEquals( entity, cache.get( key ) );
    }
    
    @Test
    public void assertThatRemoveRemovesSomething()
    {
        long key = 5;
        Entity entity = new Entity( key, 10 );
        cache.put( entity );
        assertEquals( entity, cache.get( key ) );
        cache.remove( key );
        assertEquals( null, cache.get( key ) );
    }
    
    @Test
    public void assertThatPutKeepsCorrectSize()
    {
        final int size = 10;
        SneakyEntity oldEntity = new SneakyEntity( 0l, size )
        {
            @Override
            void doThisBadStuffInSizeCall()
            {
                // when AAC.put asks the old object for size this will emulate a cache.updateSize( oldObj, size, size + 1 )
                updateSize( size + 1 );
                cache.updateSize( this, size + 1 );
            }
        };
        cache.put( oldEntity ); // will increase internal size to 11 and update cache
        oldEntity.updateSize( size ); // will reset entity size to 10
        Entity newEntity = new Entity( 0l, 11 );
        cache.put( newEntity ); // will increase oldEntity size + 1 when cache.put asks for it 
        assertEquals( 11, cache.size() );
    }
    
    @Test
    public void assertThatRemoveKeepsCorrectSize()
    {
        final int size = 10;
        SneakyEntity entity = new SneakyEntity( 0l, size )
        {
            @Override
            void doThisBadStuffInSizeCall()
            {
                // when AAC.remove asks the object for size this will emulate a cache.updateSize( oldObj, size, size + 1 )
                updateSize( size + 1 );
                cache.updateSize( this, size + 1 );
            }
        };
        cache.put( entity ); // will increase internal size to 11 and update cache
        entity.updateSize( size ); // will reset entity size to 10
        cache.remove( entity.getId() );
        assertEquals( 0, cache.size() );
    }
    
    @Test(expected = NullPointerException.class )
    public void assertNullPutTriggersNPE()
    {
        cache.put( null );
    }
    
    @Test(expected = IndexOutOfBoundsException.class )
    public void assertPutCanHandleWrongId()
    {
        Entity entity = new Entity( -1l, 1 );
        cache.put( entity );
    }

    @Test(expected = IndexOutOfBoundsException.class )
    public void assertGetCanHandleWrongId()
    {
        cache.get( -1l );
    }
    
    @Test(expected = IndexOutOfBoundsException.class )
    public void assertRemoveCanHandleWrongId()
    {
        cache.remove( -1l );
    }
    
    private static class Entity implements EntityWithSize
    {
        private final long id;
        private int size;
        private int registeredSize;
        
        Entity( long id, int size )
        {
            this.id = id;
            this.size = size;
        }
        
        @Override
        public int size()
        {
            return size;
        }

        @Override
        public long getId()
        {
            return id;
        }
        
        public void updateSize( int newSize )
        {
            this.size = newSize;
        }

        @Override
        public void setRegisteredSize( int size )
        {
            this.registeredSize = size;
        }

        @Override
        public int getRegisteredSize()
        {
            return registeredSize;
        }
    }
    
    private static abstract class SneakyEntity extends Entity
    {
        SneakyEntity( long id, int size )
        {
            super( id, size );
        }
        
        @Override
        public int size()
        {
            int size = super.size();
            doThisBadStuffInSizeCall();
            return size;
        }
        
        abstract void doThisBadStuffInSizeCall();
    }
}
