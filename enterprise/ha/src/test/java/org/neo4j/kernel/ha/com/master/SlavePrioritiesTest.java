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
