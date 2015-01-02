/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.net.URI;

import org.junit.Test;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.statemachine.State;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LearnerStateTest
{

    @Test
    public void shouldUseLastKnownOnlineClusterMemberAndSetTimeoutForCatchup() throws Throwable
    {
        // Given
        LearnerState state = LearnerState.learner;
        LearnerContext ctx = mock(LearnerContext.class);
        MessageHolder outgoing = mock( MessageHolder.class );
        org.neo4j.cluster.InstanceId upToDateClusterMember = new org.neo4j.cluster.InstanceId( 1 );

        // What we know
        when(ctx.getLastLearnedInstanceId()).thenReturn( 0l );
        when(ctx.getPaxosInstance( new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1l ) )).thenReturn( new PaxosInstance( null, new org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId( 1l )));
        when(ctx.getLastKnownAliveUpToDateInstance()).thenReturn( upToDateClusterMember );
        when(ctx.getUriForId( upToDateClusterMember )).thenReturn( new URI("c:/1") );

        // What we know the cluster knows
        when( ctx.getLastKnownLearnedInstanceInCluster() ).thenReturn( 1l );

        // When
        Message<LearnerMessage> message = Message.to( LearnerMessage.catchUp, new URI( "c:/2" ), 2l )
                .setHeader( Message.FROM, "c:/2" );
        State newState = state.handle( ctx, message, outgoing );

        // Then

        assertThat(newState, equalTo((State)LearnerState.learner));
        verify(outgoing).offer( Message.to( LearnerMessage.learnRequest, new URI( "c:/1" ),
                new LearnerMessage.LearnRequestState() ).setHeader(
                org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InstanceId.INSTANCE,
                Long.toString( 1l ) ) );
        verify(ctx).setTimeout( "learn", Message.timeout( LearnerMessage.learnTimedout, message ) );
    }

}