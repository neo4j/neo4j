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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProposerContextImplTest
{
    @Test
    public void shouldOnlyReturnMembersAsAcceptorsIfTheyAreAlive()
    {
        assertEquals( 5, limitedAcceptors( 5, instanceIds( 5 ) ) );
        assertEquals( 3, limitedAcceptors( 3, instanceIds( 5 ) ) );
        assertEquals( 3, limitedAcceptors( 3, instanceIds( 3 ) ) );
        assertEquals( 2, limitedAcceptors( 2, instanceIds( 2 ) ) );
        assertEquals( 1, limitedAcceptors( 1, instanceIds( 1 ) ) );
        assertEquals( 0, limitedAcceptors( 1, instanceIds( 0 ) ) );
    }

    @Test
    public void shouldCalculateMajorityOfAcceptors()
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
        when( heartbeatContext.getUriForId( any( InstanceId.class ) ) ).thenReturn( URI.create( "http://localhost:8080" ) );

        // when
        ProposerContextImpl proposerContext = new ProposerContextImpl( new InstanceId( 1 ), commonContextState,
                null, null, null, heartbeatContext );

        return proposerContext.getAcceptors().size();
    }
}
