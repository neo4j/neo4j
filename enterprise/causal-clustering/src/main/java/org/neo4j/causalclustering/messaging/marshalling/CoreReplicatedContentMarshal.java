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
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSetSerializer;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenSerializer;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionSerializer;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class CoreReplicatedContentMarshal
{
    private static final byte TX_CONTENT_TYPE = 0;
    private static final byte RAFT_MEMBER_SET_TYPE = 1;
    private static final byte ID_RANGE_REQUEST_TYPE = 2;
    private static final byte TOKEN_REQUEST_TYPE = 4;
    private static final byte NEW_LEADER_BARRIER_TYPE = 5;
    private static final byte LOCK_TOKEN_REQUEST = 6;
    private static final byte DISTRIBUTED_OPERATION = 7;
    private static final byte DUMMY_REQUEST = 8;

    public static Codec<ReplicatedContent> codec()
    {
        return new ReplicatedContentCodec( new CoreReplicatedContentMarshal() );
    }

    public static SafeChannelMarshal<ReplicatedContent> marshaller()
    {
        return new ReplicatedContentMarshaller( new CoreReplicatedContentMarshal() );
    }

    private CoreReplicatedContentMarshal()
    {
    }

    private ContentBuilder<ReplicatedContent> unmarshal( byte contentType, ByteBuf buffer ) throws IOException, EndOfStreamException
    {
        switch ( contentType )
        {
        case TX_CONTENT_TYPE:
        {
            return ContentBuilder.finished( ReplicatedTransactionSerializer.decode( buffer ) );
        }
        case DUMMY_REQUEST:
            return ContentBuilder.finished( DummyRequest.decode( buffer ) );
        default:
            return unmarshal( contentType, new NetworkReadableClosableChannelNetty4( buffer ) );
        }
    }

    private ContentBuilder<ReplicatedContent> unmarshal( byte contentType, ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        switch ( contentType )
        {
        case TX_CONTENT_TYPE:
            return ContentBuilder.finished( ReplicatedTransactionSerializer.unmarshal( channel ) );
        case RAFT_MEMBER_SET_TYPE:
            return ContentBuilder.finished( MemberIdSetSerializer.unmarshal( channel ) );
        case ID_RANGE_REQUEST_TYPE:
            return ContentBuilder.finished( ReplicatedIdAllocationRequestSerializer.unmarshal( channel ) );
        case TOKEN_REQUEST_TYPE:
            return ContentBuilder.finished( ReplicatedTokenRequestSerializer.unmarshal( channel ) );
        case NEW_LEADER_BARRIER_TYPE:
            return ContentBuilder.finished( new NewLeaderBarrier() );
        case LOCK_TOKEN_REQUEST:
            return ContentBuilder.finished( ReplicatedLockTokenSerializer.unmarshal( channel ) );
        case DISTRIBUTED_OPERATION:
        {
            return DistributedOperation.deserialize( channel );
        }
        case DUMMY_REQUEST:
            return ContentBuilder.finished( DummyRequest.Marshal.INSTANCE.unmarshal( channel ) );
        default:
            throw new IllegalStateException( "Not a recognized content type: " + contentType );
        }
    }

    private static class ReplicatedContentCodec implements Codec<ReplicatedContent>
    {
        private final CoreReplicatedContentMarshal serializer;

        ReplicatedContentCodec( CoreReplicatedContentMarshal serializer )
        {
            this.serializer = serializer;
        }

        @Override
        public void encode( ReplicatedContent type, List<Object> output ) throws IOException
        {
            type.handle( new EncodingHandlerReplicated( output ) );
        }

        @Override
        public ContentBuilder<ReplicatedContent> decode( ByteBuf byteBuf ) throws IOException, EndOfStreamException
        {
            return serializer.unmarshal( byteBuf.readByte(), byteBuf );
        }
    }

    private static class ReplicatedContentMarshaller extends SafeChannelMarshal<ReplicatedContent>
    {
        private final CoreReplicatedContentMarshal serializer;

        ReplicatedContentMarshaller( CoreReplicatedContentMarshal serializer )
        {
            this.serializer = serializer;
        }

        @Override
        public void marshal( ReplicatedContent replicatedContent, WritableChannel channel ) throws IOException
        {
            replicatedContent.handle( new MarshallingHandlerReplicated( channel ) );
        }

        @Override
        protected ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            byte type = channel.get();
            ContentBuilder<ReplicatedContent> contentBuilder = serializer.unmarshal( type, channel );
            while ( !contentBuilder.isComplete() )
            {
                type = channel.get();
                contentBuilder = contentBuilder.combine( serializer.unmarshal( type, channel ) );
            }
            return contentBuilder.build();
        }
    }

    private static class EncodingHandlerReplicated implements ReplicatedContentHandler
    {

        private final List<Object> output;

        EncodingHandlerReplicated( List<Object> output )
        {
            this.output = output;
        }

        @Override
        public void handle( ReplicatedTransaction replicatedTransaction )
        {
            output.add( ChunkedReplicatedContent.chunked( TX_CONTENT_TYPE, new MaxTotalSize( replicatedTransaction.encode() ) ) );
        }

        @Override
        public void handle( MemberIdSet memberIdSet )
        {
            output.add( ChunkedReplicatedContent.single( RAFT_MEMBER_SET_TYPE, channel -> MemberIdSetSerializer.marshal( memberIdSet, channel ) ) );
        }

        @Override
        public void handle( ReplicatedIdAllocationRequest replicatedIdAllocationRequest )
        {
            output.add( ChunkedReplicatedContent.single( ID_RANGE_REQUEST_TYPE,
                    channel -> ReplicatedIdAllocationRequestSerializer.marshal( replicatedIdAllocationRequest, channel ) ) );
        }

        @Override
        public void handle( ReplicatedTokenRequest replicatedTokenRequest )
        {
            output.add( ChunkedReplicatedContent.single( TOKEN_REQUEST_TYPE,
                    channel -> ReplicatedTokenRequestSerializer.marshal( replicatedTokenRequest, channel ) ) );
        }

        @Override
        public void handle( NewLeaderBarrier newLeaderBarrier )
        {
            output.add( ChunkedReplicatedContent.single( NEW_LEADER_BARRIER_TYPE, channel ->
            {
            } ) );
        }

        @Override
        public void handle( ReplicatedLockTokenRequest replicatedLockTokenRequest )
        {
            output.add( ChunkedReplicatedContent.single( LOCK_TOKEN_REQUEST,
                    channel -> ReplicatedLockTokenSerializer.marshal( replicatedLockTokenRequest, channel ) ) );
        }

        @Override
        public void handle( DistributedOperation distributedOperation )
        {
            output.add( ChunkedReplicatedContent.single( DISTRIBUTED_OPERATION, distributedOperation::marshalMetaData ) );
        }

        @Override
        public void handle( DummyRequest dummyRequest )
        {
            output.add( ChunkedReplicatedContent.chunked( DUMMY_REQUEST, dummyRequest.encoder() ) );
        }
    }

    private static class MarshallingHandlerReplicated implements ReplicatedContentHandler
    {

        private final WritableChannel writableChannel;

        MarshallingHandlerReplicated( WritableChannel writableChannel )
        {
            this.writableChannel = writableChannel;
        }

        @Override
        public void handle( ReplicatedTransaction replicatedTransaction ) throws IOException
        {
            writableChannel.put( TX_CONTENT_TYPE );
            replicatedTransaction.marshal( writableChannel );
        }

        @Override
        public void handle( MemberIdSet memberIdSet ) throws IOException
        {
            writableChannel.put( RAFT_MEMBER_SET_TYPE );
            MemberIdSetSerializer.marshal( memberIdSet, writableChannel );
        }

        @Override
        public void handle( ReplicatedIdAllocationRequest replicatedIdAllocationRequest ) throws IOException
        {
            writableChannel.put( ID_RANGE_REQUEST_TYPE );
            ReplicatedIdAllocationRequestSerializer.marshal( replicatedIdAllocationRequest, writableChannel );
        }

        @Override
        public void handle( ReplicatedTokenRequest replicatedTokenRequest ) throws IOException
        {
            writableChannel.put( TOKEN_REQUEST_TYPE );
            ReplicatedTokenRequestSerializer.marshal( replicatedTokenRequest, writableChannel );
        }

        @Override
        public void handle( NewLeaderBarrier newLeaderBarrier ) throws IOException
        {
            writableChannel.put( NEW_LEADER_BARRIER_TYPE );
        }

        @Override
        public void handle( ReplicatedLockTokenRequest replicatedLockTokenRequest ) throws IOException
        {
            writableChannel.put( LOCK_TOKEN_REQUEST );
            ReplicatedLockTokenSerializer.marshal( replicatedLockTokenRequest, writableChannel );
        }

        @Override
        public void handle( DistributedOperation distributedOperation ) throws IOException
        {
            writableChannel.put( DISTRIBUTED_OPERATION );
            distributedOperation.marshalMetaData( writableChannel );
        }

        @Override
        public void handle( DummyRequest dummyRequest ) throws IOException
        {
            writableChannel.put( DUMMY_REQUEST );
            DummyRequest.Marshal.INSTANCE.marshal( dummyRequest, writableChannel );
        }
    }
}
