/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.com.master;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlavePrioritiesTest
{
    @Test
    public void roundRobinWithTwoSlavesAndPushFactorTwo()
    {
        // Given
        SlavePriority roundRobin = SlavePriorities.roundRobin();

        // When
        Iterator<Slave> slaves = roundRobin.prioritize( slaves( 2, 3 ) ).iterator();

        // Then
        assertEquals( 2, slaves.next().getServerId() );
        assertEquals( 3, slaves.next().getServerId() );
    }

    @Test
    public void roundRobinWithTwoSlavesAndPushFactorOne()
    {
        // Given
        SlavePriority roundRobin = SlavePriorities.roundRobin();

        // When
        Slave slave1 = roundRobin.prioritize( slaves( 2, 3 ) ).iterator().next();
        Slave slave2 = roundRobin.prioritize( slaves( 2, 3 ) ).iterator().next();

        // Then
        assertEquals( 2, slave1.getServerId() );
        assertEquals( 3, slave2.getServerId() );
    }

    @Test
    public void roundRobinWithTwoSlavesAndPushFactorOneWhenSlaveIsAdded()
    {
        // Given
        SlavePriority roundRobin = SlavePriorities.roundRobin();

        // When
        Slave slave1 = roundRobin.prioritize( slaves( 2, 3 ) ).iterator().next();
        Slave slave2 = roundRobin.prioritize( slaves( 2, 3 ) ).iterator().next();
        Slave slave3 = roundRobin.prioritize( slaves( 2, 3, 4 ) ).iterator().next();

        // Then
        assertEquals( 2, slave1.getServerId() );
        assertEquals( 3, slave2.getServerId() );
        assertEquals( 4, slave3.getServerId() );
    }

    @Test
    public void roundRobinWithTwoSlavesAndPushFactorOneWhenSlaveIsRemoved()
    {
        // Given
        SlavePriority roundRobin = SlavePriorities.roundRobin();

        // When
        Slave slave1 = roundRobin.prioritize( slaves( 2, 3, 4 ) ).iterator().next();
        Slave slave2 = roundRobin.prioritize( slaves( 2, 3, 4 ) ).iterator().next();
        Slave slave3 = roundRobin.prioritize( slaves( 2, 3 ) ).iterator().next();

        // Then
        assertEquals( 2, slave1.getServerId() );
        assertEquals( 3, slave2.getServerId() );
        assertEquals( 2, slave3.getServerId() );
    }

    @Test
    public void roundRobinWithSingleSlave()
    {
        // Given
        SlavePriority roundRobin = SlavePriorities.roundRobin();

        // When
        Iterator<Slave> slaves = roundRobin.prioritize( slaves( 2 ) ).iterator();

        // Then
        assertEquals( 2, slaves.next().getServerId() );
    }

    @Test
    public void roundRobinWithNoSlaves()
    {
        // Given
        SlavePriority roundRobin = SlavePriorities.roundRobin();

        // When
        Iterator<Slave> slaves = roundRobin.prioritize( slaves() ).iterator();

        // Then
        assertFalse( slaves.hasNext() );
    }

    private static Iterable<Slave> slaves( int... ids )
    {
        List<Slave> slaves = new ArrayList<>( ids.length );
        for ( int id : ids )
        {
            Slave slaveMock = mock( Slave.class );
            when( slaveMock.getServerId() ).thenReturn( id );
            slaves.add( slaveMock );
        }
        return slaves;
    }
}
