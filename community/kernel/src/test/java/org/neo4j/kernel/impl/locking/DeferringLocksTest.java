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
import org.neo4j.test.RandomRule;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static java.lang.Math.abs;

public class DeferringLocksTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldDeferAllLocks() throws Exception
    {
        // GIVEN
        Locks actualLocks = mock( Locks.class );
        Locks.Client actualClient = mock( Locks.Client.class );
        when( actualLocks.newClient() ).thenReturn( actualClient );
        DeferringLocks locks = new DeferringLocks( actualLocks );
        Locks.Client client = locks.newClient();

        // WHEN
        Set<Resource> expected = new HashSet<>();
        ResourceType[] types = ResourceTypes.values();
        System.out.println( "1" );
        for ( int i = 0; i < 10_000; i++ )
        {
            Resource resource = new Resource( random.among( types ), abs( random.nextLong() ) );
            client.acquireExclusive( resource.type, resource.id );
            expected.add( resource );
        }
        System.out.println( "2" );
        verifyNoMoreInteractions( actualClient );
        client.prepare();
        System.out.println( "3" );

        // THEN
        for ( Resource resource : expected )
        {
            verify( actualClient ).acquireExclusive( resource.type, resource.id );
        }
        System.out.println( "4" );
    }

    private static class Resource
    {
        private final ResourceType type;
        private final long id;

        public Resource( ResourceType type, long id )
        {
            this.type = type;
            this.id = id;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (id ^ (id >>> 32));
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
                return true;
            if ( obj == null )
                return false;
            if ( getClass() != obj.getClass() )
                return false;
            Resource other = (Resource) obj;
            if ( id != other.id )
                return false;
            if ( type == null )
            {
                if ( other.type != null )
                    return false;
            }
            else if ( !type.equals( other.type ) )
                return false;
            return true;
        }
    }
}
