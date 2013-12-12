/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.omega.MessageArgumentMatcher;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProposerStateTest
{
    @Test
    public void ifProposingWithClosedInstanceThenRetryWithNextInstance() throws Throwable
    {
        ProposerContext context = Mockito.mock(ProposerContext.class);
        when(context.getLogger( any(Class.class) )).thenReturn( StringLogger.DEV_NULL );

        InstanceId instanceId = new InstanceId( 42 );
        PaxosInstanceStore paxosInstanceStore = new PaxosInstanceStore();

        // The instance is closed
        PaxosInstance paxosInstance = new PaxosInstance( paxosInstanceStore, instanceId ); // the instance
        paxosInstance.closed( instanceId, "1/15#" ); // is closed for that conversation, not really important
        when( context.unbookInstance( instanceId ) ).thenReturn( Message.internal( ProposerMessage.accepted, "the closed payload" ) );

        when( context.getPaxosInstance( instanceId ) ).thenReturn( paxosInstance ); // required for

        // But in the meantime it was reused and has now (of course) timed out
        String theTimedoutPayload = "the timed out payload";
        Message message = Message.internal( ProposerMessage.phase1Timeout, theTimedoutPayload );
        message.setHeader( InstanceId.INSTANCE, instanceId.toString() );

        // Handle it
        MessageHolder mockHolder = mock( MessageHolder.class );
        ProposerState.proposer.handle( context, message, mockHolder );

        // Verify it was resent as a propose with the same value
        verify( mockHolder, times(1) ).offer(
                Matchers.<Message<? extends MessageType>>argThat(
                        new MessageArgumentMatcher().onMessageType( ProposerMessage.propose ).withPayload( theTimedoutPayload )
                ) );
        verify( context, times(1) ).unbookInstance( instanceId );
    }
}
