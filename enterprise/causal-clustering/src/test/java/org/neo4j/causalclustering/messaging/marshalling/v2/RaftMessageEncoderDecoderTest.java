/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.messaging.marshalling.v2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.stream.ChunkedInput;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolClientInstallerV1;
import org.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolServerInstallerV1;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.TokenType;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.logging.FormattedLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Warning! This test ensures that all raft protocol work as expected in their current implementation. However, it does not know about changes to the
 * protocols that breaks backward compatibility.
 */
@RunWith( Parameterized.class )
public class RaftMessageEncoderDecoderTest
{
    private static final MemberId MEMBER_ID = new MemberId( UUID.randomUUID() );
    private static final int[] PROTOCOLS = {1, 2};
    @Parameterized.Parameter()
    public RaftMessages.RaftMessage raftMessage;
    @Parameterized.Parameter( 1 )
    public int raftProtocol;
    private final RaftMessageHandler handler = new RaftMessageHandler();

    @Parameterized.Parameters( name = "Raft v{1} with message {0}" )
    public static Object[] data()
    {
        return setUpParams( new RaftMessages.RaftMessage[]{new RaftMessages.Heartbeat( MEMBER_ID, 1, 2, 3 ), new RaftMessages.HeartbeatResponse( MEMBER_ID ),
                new RaftMessages.NewEntry.Request( MEMBER_ID, new DummyRequest( new byte[]{1, 2, 3, 4, 5, 6, 7, 8} ) ),
                new RaftMessages.NewEntry.Request( MEMBER_ID, ReplicatedTransaction.from( new byte[]{1, 2, 3, 4, 5, 6, 7, 8} ) ),
                new RaftMessages.NewEntry.Request( MEMBER_ID, ReplicatedTransaction.from( new PhysicalTransactionRepresentation( Collections.emptyList() ) ) ),
                new RaftMessages.NewEntry.Request( MEMBER_ID, new DistributedOperation(
                        new DistributedOperation( ReplicatedTransaction.from( new byte[]{1, 2, 3, 4, 5} ), new GlobalSession( UUID.randomUUID(), MEMBER_ID ),
                                new LocalOperationId( 1, 2 ) ), new GlobalSession( UUID.randomUUID(), MEMBER_ID ), new LocalOperationId( 3, 4 ) ) ),
                new RaftMessages.AppendEntries.Request( MEMBER_ID, 1, 2, 3,
                        new RaftLogEntry[]{new RaftLogEntry( 0, new ReplicatedTokenRequest( TokenType.LABEL, "name", new byte[]{2, 3, 4} ) ),
                                new RaftLogEntry( 1, new ReplicatedLockTokenRequest( MEMBER_ID, 2 ) )}, 5 ),
                new RaftMessages.AppendEntries.Response( MEMBER_ID, 1, true, 2, 3 ),
                new RaftMessages.Vote.Request( MEMBER_ID, Long.MAX_VALUE, MEMBER_ID, Long.MIN_VALUE, 1 ), new RaftMessages.Vote.Response( MEMBER_ID, 1, true ),
                new RaftMessages.PreVote.Request( MEMBER_ID, Long.MAX_VALUE, MEMBER_ID, Long.MIN_VALUE, 1 ),
                new RaftMessages.PreVote.Response( MEMBER_ID, 1, true ), new RaftMessages.LogCompactionInfo( MEMBER_ID, Long.MAX_VALUE, Long.MIN_VALUE )} );
    }

    private static Object[] setUpParams( RaftMessages.RaftMessage[] messages )
    {
        return Arrays.stream( messages ).flatMap( (Function<RaftMessages.RaftMessage,Stream<?>>) RaftMessageEncoderDecoderTest::params ).toArray();
    }

    private static Stream<Object[]> params( RaftMessages.RaftMessage raftMessage )
    {
        return Arrays.stream( PROTOCOLS ).mapToObj( p -> new Object[]{raftMessage, p} );
    }

    private EmbeddedChannel outbound;
    private EmbeddedChannel inbound;

    @Before
    public void setupChannels() throws Exception
    {
        outbound = new EmbeddedChannel();
        inbound = new EmbeddedChannel();

        if ( raftProtocol == 2 )
        {
            new RaftProtocolClientInstallerV2( new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER ), Collections.emptyList(),
                    FormattedLogProvider.toOutputStream( System.out ) ).install( outbound );
            new RaftProtocolServerInstallerV2( handler, new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER ), Collections.emptyList(),
                    FormattedLogProvider.toOutputStream( System.out ) ).install( inbound );
        }
        else if ( raftProtocol == 1 )
        {
            new RaftProtocolClientInstallerV1( new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER ), Collections.emptyList(),
                    FormattedLogProvider.toOutputStream( System.out ) ).install( outbound );
            new RaftProtocolServerInstallerV1( handler, new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER ), Collections.emptyList(),
                    FormattedLogProvider.toOutputStream( System.out ) ).install( inbound );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown raft protocol " + raftProtocol );
        }
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
    public void shouldEncodeDecodeRaftMessage() throws Exception
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
        assertEquals( clusterId, message.clusterId() );
        raftMessageEquals( raftMessage, message.message() );
        assertNull( inbound.readInbound() );
        ReferenceCountUtil.release( handler.msg );
    }

    private void raftMessageEquals( RaftMessages.RaftMessage raftMessage, RaftMessages.RaftMessage message ) throws Exception
    {
        if ( raftMessage instanceof RaftMessages.NewEntry.Request )
        {
            assertEquals( message.from(), raftMessage.from() );
            assertEquals( message.type(), raftMessage.type() );
            contentEquals( ((RaftMessages.NewEntry.Request) raftMessage).content(), ((RaftMessages.NewEntry.Request) raftMessage).content() );
        }
        else if ( raftMessage instanceof RaftMessages.AppendEntries.Request )
        {
            assertEquals( message.from(), raftMessage.from() );
            assertEquals( message.type(), raftMessage.type() );
            RaftLogEntry[] entries1 = ((RaftMessages.AppendEntries.Request) raftMessage).entries();
            RaftLogEntry[] entries2 = ((RaftMessages.AppendEntries.Request) message).entries();
            for ( int i = 0; i < entries1.length; i++ )
            {
                RaftLogEntry raftLogEntry1 = entries1[i];
                RaftLogEntry raftLogEntry2 = entries2[i];
                assertEquals( raftLogEntry1.term(), raftLogEntry2.term() );
                contentEquals( raftLogEntry1.content(), raftLogEntry2.content() );
            }
        }
    }

    private void contentEquals( ReplicatedContent one, ReplicatedContent two ) throws Exception
    {
        if ( one instanceof ReplicatedTransaction )
        {
            ByteBuf buffer1 = Unpooled.buffer();
            ByteBuf buffer2 = Unpooled.buffer();
            encode( buffer1, ((ReplicatedTransaction) one).encode() );
            encode( buffer2, ((ReplicatedTransaction) two).encode() );
            assertEquals( buffer1, buffer2 );
        }
        else if ( one instanceof DistributedOperation )
        {
            assertEquals( ((DistributedOperation) one).globalSession(), ((DistributedOperation) two).globalSession() );
            assertEquals( ((DistributedOperation) one).operationId(), ((DistributedOperation) two).operationId() );
            contentEquals( ((DistributedOperation) one).content(), ((DistributedOperation) two).content() );
        }
        else
        {
            assertEquals( one, two );
        }
    }

    private static void encode( ByteBuf buffer, ChunkedInput<ByteBuf> marshal ) throws Exception
    {
        while ( !marshal.isEndOfInput() )
        {
            ByteBuf tmp = marshal.readChunk( UnpooledByteBufAllocator.DEFAULT );
            if ( tmp != null )
            {
                buffer.writeBytes( tmp );
                tmp.release();
            }
        }
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
