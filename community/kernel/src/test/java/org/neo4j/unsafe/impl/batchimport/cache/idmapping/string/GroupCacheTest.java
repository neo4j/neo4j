/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.junit.Test;

import static org.junit.Assert.fail;

import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;

public class GroupCacheTest
{
    @Test
    public void shouldHandleSingleByteCount()
    {
        // given
        int max = 256;
        GroupCache cache = GroupCache.select( HEAP, 100, max );

        // when
        assertSetAndGet( cache, 10, 45 );
        assertSetAndGet( cache, 100, 145 );
        assertSetAndGet( cache, 1000, 245 );

        // then
        try
        {
            cache.set( 10000, 345 );
            fail( "Shouldn't handle that" );
        }
        catch ( ArithmeticException e )
        {
            // OK
        }
    }

    @Test
    public void shouldSwitchToTwoByteVersionBeyondSingleByteGroupIds()
    {
        // given
        int max = 257;
        GroupCache cache = GroupCache.select( HEAP, 100, max );

        // when
        assertSetAndGet( cache, 10, 123 );
        assertSetAndGet( cache, 100, 1234 );
        assertSetAndGet( cache, 1000, 12345 );
        assertSetAndGet( cache, 10000, 0xFFFF );

        // then
        try
        {
            cache.set( 100000, 123456 );
            fail( "Shouldn't handle that" );
        }
        catch ( ArithmeticException e )
        {
            // OK
        }
    }

    private void assertSetAndGet( GroupCache cache, long nodeId, int groupId )
    {
        cache.set( nodeId, groupId );
    }
}
