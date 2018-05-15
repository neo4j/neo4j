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
package org.neo4j.causalclustering.messaging.marshalling.v2;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Instant;
import java.util.Collections;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstaller;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstaller;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.TokenType;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith( Parameterized.class )
public class RaftMessageEncoderDecoderTest
{
    private static final MemberId MEMBER_ID = new MemberId( UUID.randomUUID() );
    @Parameterized.Parameter()
    public RaftMessages.RaftMessage raftMessage;
    private final RaftMessageHandler handler = new RaftMessageHandler();

    @Parameterized.Parameters( name = "{0}" )
    public static RaftMessages.RaftMessage[] data()
    {
        return new RaftMessages.RaftMessage[]{
                                new RaftMessages.Heartbeat( MEMBER_ID, 1, 2, 3 ),
                                new RaftMessages.HeartbeatResponse( MEMBER_ID ),
                                new RaftMessages.NewEntry.Request( MEMBER_ID, new ReplicatedTransaction( new byte[]{1, 2, 3} ) ),
                new RaftMessages.AppendEntries.Request( MEMBER_ID, 1, 2, 3,
                        new RaftLogEntry[]{new RaftLogEntry( 0, new ReplicatedTokenRequest( TokenType.LABEL, "name", new byte[]{2, 3, 4} ) ),
                                new RaftLogEntry( 1, new ReplicatedLockTokenRequest( MEMBER_ID, 2 ) )}, 5 ),
                                new RaftMessages.AppendEntries.Response( MEMBER_ID, 1, true, 2, 3 ),
                                new RaftMessages.Vote.Request( MEMBER_ID, Long.MAX_VALUE, MEMBER_ID, Long.MIN_VALUE, 1 ),
                new RaftMessages.Vote.Response( MEMBER_ID, 1, true ),
                                new RaftMessages.PreVote.Request( MEMBER_ID, Long.MAX_VALUE, MEMBER_ID, Long.MIN_VALUE, 1 ),
                                new RaftMessages.PreVote.Response( MEMBER_ID, 1, true ),
                                new RaftMessages.LogCompactionInfo( MEMBER_ID, Long.MAX_VALUE, Long.MIN_VALUE )};
    }

    private EmbeddedChannel outbound;
    private EmbeddedChannel inbound;

    @Before
    public void setupChannels() throws Exception
    {
        outbound = new EmbeddedChannel();
        inbound = new EmbeddedChannel();

        new RaftProtocolClientInstaller( new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER ), Collections.emptyList(),
                NullLogProvider.getInstance() ).install( outbound );
        new RaftProtocolServerInstaller( handler, new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER ), Collections.emptyList(),
                NullLogProvider.getInstance() ).install( inbound );
    }

    @After
    public void cleanUp()
    {
        if ( outbound != null )
        {
            outbound.close();
        }
        if ( inbound != null )
        {
            inbound.close();
        }
        outbound = inbound = null;
    }

    @Test
    public void shouldEncodeDecodeRaftMessage()
    {
        ClusterId clusterId = new ClusterId( UUID.randomUUID() );
        RaftMessages.ReceivedInstantClusterIdAwareMessage<RaftMessages.RaftMessage> idAwareMessage =
                RaftMessages.ReceivedInstantClusterIdAwareMessage.of( Instant.now(), clusterId, raftMessage );

        outbound.writeOutbound( idAwareMessage );

        Object o;
        while ( (o = outbound.readOutbound()) != null )
        {
            inbound.writeInbound( o );
        }
        RaftMessages.ReceivedInstantClusterIdAwareMessage<RaftMessages.RaftMessage> message = handler.getRaftMessage();
        System.out.println( message );
        assertEquals( clusterId, message.clusterId() );
        assertEquals( raftMessage, message.message() );
        assertNull( inbound.readInbound() );
    }

    class RaftMessageHandler extends SimpleChannelInboundHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<RaftMessages.RaftMessage>>
    {

        private RaftMessages.ReceivedInstantClusterIdAwareMessage<RaftMessages.RaftMessage> msg;

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, RaftMessages.ReceivedInstantClusterIdAwareMessage<RaftMessages.RaftMessage> msg )
        {
            this.msg = msg;
        }

        RaftMessages.ReceivedInstantClusterIdAwareMessage<RaftMessages.RaftMessage> getRaftMessage()
        {
            return msg;
        }
    }
}
