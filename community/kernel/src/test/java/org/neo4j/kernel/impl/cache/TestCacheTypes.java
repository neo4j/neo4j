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

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestCacheTypes
{
    @Test
    public void softCacheShouldHonorPutSemantics() throws Exception
    {
        assertCacheHonorsPutsSemantics( new SoftLruCache<>( "test" ) );
    }

    @Test
    public void weakCacheShouldHonorPutSemantics() throws Exception
    {
        assertCacheHonorsPutsSemantics( new WeakLruCache<>( "test" ) );
    }

    @Test
    public void strongCacheShouldHonorPutSemantics() throws Exception
    {
        assertCacheHonorsPutsSemantics( new StrongReferenceCache<>( "test" ) );
    }

    private void assertCacheHonorsPutsSemantics( Cache<EntityWithSizeObject> cache )
    {
        Entity version1 = new Entity( 10 );
        assertTrue( version1 == cache.put( version1 ) );

        // WHEN
        Entity version2 = new Entity( 10 );

        // THEN
        assertTrue( version1 == cache.put( version2 ) );
    }

    public static class Entity implements EntityWithSizeObject
    {
        private int registeredSize;
        private final long id;

        Entity( long id )
        {
            this.id = id;
        }

        @Override
        public int sizeOfObjectInBytesIncludingOverhead()
        {
            return 0;
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public void setRegisteredSize( int size )
        {
            registeredSize = size;
        }

        @Override
        public int getRegisteredSize()
        {
            return registeredSize;
        }
    }
}
