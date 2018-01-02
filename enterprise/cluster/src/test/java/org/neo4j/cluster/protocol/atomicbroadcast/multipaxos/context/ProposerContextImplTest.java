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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProposerContextImplTest
{
    @Test
    public void shouldOnlyReturnMembersAsAcceptorsIfTheyAreAlive() throws Exception
    {
        assertEquals( 5, limitedAcceptors( 5, instanceIds( 5 ) ) );
        assertEquals( 3, limitedAcceptors( 3, instanceIds( 5 ) ) );
        assertEquals( 3, limitedAcceptors( 3, instanceIds( 3 ) ) );
        assertEquals( 2, limitedAcceptors( 2, instanceIds( 2 ) ) );
        assertEquals( 1, limitedAcceptors( 1, instanceIds( 1 ) ) );
        assertEquals( 0, limitedAcceptors( 1, instanceIds( 0 ) ) );
    }

    @Test
    public void shouldCalculateMajorityOfAcceptors() throws Exception
    {
        ProposerContextImpl proposerContext = new ProposerContextImpl( new InstanceId( 1 ),
                null, null, null, null, null );

        assertEquals( 1, proposerContext.getMinimumQuorumSize( acceptorUris( 1 ) ) );
        assertEquals( 2, proposerContext.getMinimumQuorumSize( acceptorUris( 2 ) ) );
        assertEquals( 2, proposerContext.getMinimumQuorumSize( acceptorUris( 3 ) ) );
        assertEquals( 3, proposerContext.getMinimumQuorumSize( acceptorUris( 4 ) ) );
        assertEquals( 3, proposerContext.getMinimumQuorumSize( acceptorUris( 5 ) ) );
        assertEquals( 4, proposerContext.getMinimumQuorumSize( acceptorUris( 6 ) ) );
        assertEquals( 4, proposerContext.getMinimumQuorumSize( acceptorUris( 7 ) ) );
    }

    private List<URI> acceptorUris( int numberOfAcceptors )
    {
        List<URI> items = new ArrayList<>();

        for ( int i = 0; i < numberOfAcceptors; i++ )
        {
            items.add( URI.create( String.valueOf( i ) ) );
        }

        return items;
    }

    private List<InstanceId> instanceIds( int numberOfAcceptors )
    {
        List<InstanceId> items = new ArrayList<>();

        for ( int i = 0; i < numberOfAcceptors; i++ )
        {
            items.add( new InstanceId( i ) );
        }

        return items;
    }

    private int limitedAcceptors( int maxAcceptors, List<InstanceId> alive )
    {
        CommonContextState commonContextState = new CommonContextState( null, maxAcceptors );

        HeartbeatContext heartbeatContext = mock( HeartbeatContext.class );
        when( heartbeatContext.getAlive() ).thenReturn( alive );

        // when
        ProposerContextImpl proposerContext = new ProposerContextImpl( new InstanceId( 1 ), commonContextState,
                null, null, null, heartbeatContext );

        return proposerContext.getAcceptors().size();
    }
}