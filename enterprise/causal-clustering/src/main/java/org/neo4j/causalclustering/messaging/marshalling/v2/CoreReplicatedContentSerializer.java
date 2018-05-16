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

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSetSerializer;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenSerializer;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.tx.ChunkedReplicatedTransactionInput;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionSerializer;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentChunk;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static java.util.Collections.singleton;
import static org.neo4j.causalclustering.messaging.marshalling.v2.SerializableContent.simple;

public class CoreReplicatedContentSerializer extends SafeChannelMarshal<ReplicatedContent>
{
    private static final byte TX_CONTENT_TYPE = 0;
    private static final byte RAFT_MEMBER_SET_TYPE = 1;
    private static final byte ID_RANGE_REQUEST_TYPE = 2;
    private static final byte TOKEN_REQUEST_TYPE = 4;
    private static final byte NEW_LEADER_BARRIER_TYPE = 5;
    private static final byte LOCK_TOKEN_REQUEST = 6;
    private static final byte DISTRIBUTED_OPERATION = 7;
    private static final byte DUMMY_REQUEST = 8;

    public Collection<SerializableContent> toSerializable( ReplicatedContent content )
    {
        if ( content instanceof ReplicatedTransaction )
        {
            return singleton( new ChunkedReplicatedTransactionInput( TX_CONTENT_TYPE, (ReplicatedTransaction) content ) );
        }
        else if ( content instanceof MemberIdSet )
        {
            return singleton( simple( RAFT_MEMBER_SET_TYPE, channel -> MemberIdSetSerializer.marshal( (MemberIdSet) content, channel ) ) );
        }
        else if ( content instanceof ReplicatedIdAllocationRequest )
        {
            return singleton( simple( ID_RANGE_REQUEST_TYPE,
                    channel -> ReplicatedIdAllocationRequestSerializer.marshal( (ReplicatedIdAllocationRequest) content, channel ) ) );
        }
        else if ( content instanceof ReplicatedTokenRequest )
        {
            return singleton( simple( TOKEN_REQUEST_TYPE, channel -> ReplicatedTokenRequestSerializer.marshal( (ReplicatedTokenRequest) content, channel ) ) );
        }
        else if ( content instanceof NewLeaderBarrier )
        {
            return singleton( simple( NEW_LEADER_BARRIER_TYPE, channel ->
            {
            } ) );
        }
        else if ( content instanceof ReplicatedLockTokenRequest )
        {
            return singleton( simple( LOCK_TOKEN_REQUEST, channel -> ReplicatedLockTokenSerializer.marshal( (ReplicatedLockTokenRequest) content, channel ) ) );
        }
        else if ( content instanceof DistributedOperation )
        {
            ArrayList<SerializableContent> list = new ArrayList<>( toSerializable( ((DistributedOperation) content).content() ) );
            list.add( 0, simple( DISTRIBUTED_OPERATION, channel ->
            {
                channel.putLong( ((DistributedOperation) content).globalSession().sessionId().getMostSignificantBits() );
                channel.putLong( ((DistributedOperation) content).globalSession().sessionId().getLeastSignificantBits() );
                new MemberId.Marshal().marshal( ((DistributedOperation) content).globalSession().owner(), channel );

                channel.putLong( ((DistributedOperation) content).operationId().localSessionId() );
                channel.putLong( ((DistributedOperation) content).operationId().sequenceNumber() );
            } ) );
            return list;
        }
        else if ( content instanceof DummyRequest )
        {
            return singleton( simple( DUMMY_REQUEST, channel -> DummyRequest.Marshal.INSTANCE.marshal( (DummyRequest) content, channel ) ) );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type " + content.getClass() );
        }
    }

    public ContentBuilder<ReplicatedContent> decode( ByteBuf byteBuf ) throws IOException, EndOfStreamException
    {
        byte type = byteBuf.readByte();
        byteBuf.readerIndex( byteBuf.readerIndex() - 1 );
        switch ( type )
        {
        case TX_CONTENT_TYPE:
            ReplicatedContentChunk replicatedContentChunk = ReplicatedContentChunk.deSerialize( byteBuf );
            UnfinishedChunk unfinishedChunk = UnfinishedChunk.create( replicatedContentChunk.content() );
            return new ContentBuilder<>( replicatedContent ->
            {
                UnfinishedChunk chunk;
                if ( replicatedContent != null )
                {
                    chunk = unfinishedChunk.consume( (UnfinishedChunk) replicatedContent );
                    //                    chunk = ( (UnfinishedChunk) replicatedContent ).consume( unfinishedChunk );
                }
                else
                {
                    chunk = unfinishedChunk;
                }
                if ( !replicatedContentChunk.isFirst() )
                {
                    return chunk;
                }
                else
                {
                    int length = chunk.byteBuf.readableBytes();
                    byte[] bytes = new byte[length];
                    chunk.byteBuf.readBytes( bytes );
                    chunk.byteBuf.release();
                    return new ReplicatedTransaction( bytes );
                }
            }, replicatedContentChunk.isLast() );

        default:
            return read( new NetworkReadableClosableChannelNetty4( byteBuf ) );
        }
    }

    public ContentBuilder<ReplicatedContent> read( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        byte type = channel.get();
        switch ( type )
        {
        case TX_CONTENT_TYPE:
            return new ContentBuilder<>( ReplicatedTransactionSerializer.unmarshal( channel ) );
        case RAFT_MEMBER_SET_TYPE:
            return new ContentBuilder<>( MemberIdSetSerializer.unmarshal( channel ) );
        case ID_RANGE_REQUEST_TYPE:
            return new ContentBuilder<>( ReplicatedIdAllocationRequestSerializer.unmarshal( channel ) );
        case TOKEN_REQUEST_TYPE:
            return new ContentBuilder<>( ReplicatedTokenRequestSerializer.unmarshal( channel ) );
        case NEW_LEADER_BARRIER_TYPE:
            return new ContentBuilder<>( new NewLeaderBarrier() );
        case LOCK_TOKEN_REQUEST:
            return new ContentBuilder<>( ReplicatedLockTokenSerializer.unmarshal( channel ) );
        case DISTRIBUTED_OPERATION:
        {
            long mostSigBits = channel.getLong();
            long leastSigBits = channel.getLong();
            MemberId owner = new MemberId.Marshal().unmarshal( channel );
            GlobalSession globalSession = new GlobalSession( new UUID( mostSigBits, leastSigBits ), owner );

            long localSessionId = channel.getLong();
            long sequenceNumber = channel.getLong();
            LocalOperationId localOperationId = new LocalOperationId( localSessionId, sequenceNumber );

            return new ContentBuilder<>( replicatedContent ->
            {
                if ( replicatedContent instanceof UnfinishedChunk )
                {
                    throw new IllegalStateException( "Cannot combine with unfinished chunk" );
                }
                return new DistributedOperation( replicatedContent, globalSession, localOperationId );
            }, false );
        }
        case DUMMY_REQUEST:
            return new ContentBuilder<>( DummyRequest.Marshal.INSTANCE.unmarshal( channel ) );
        default:
            throw new IllegalStateException( "Not a recognized content type: " + type );
        }
    }

    private static class UnfinishedChunk implements ReplicatedContent
    {
        ByteBuf byteBuf;

        private UnfinishedChunk( ByteBuf content )
        {
            byteBuf = content.copy();
        }

        public static UnfinishedChunk create( ByteBuf content )
        {
            return new UnfinishedChunk( content );
        }

        public UnfinishedChunk consume( UnfinishedChunk chunk )
        {
            byteBuf.writeBytes( chunk.byteBuf );
            chunk.byteBuf.release();
            return this;
        }

        public byte[] array()
        {
            return Arrays.copyOf( byteBuf.array(), byteBuf.writerIndex() );
        }
    }

    @Override
    public void marshal( ReplicatedContent coreReplicatedContent, WritableChannel channel ) throws IOException
    {
        for ( SerializableContent serializableContent : toSerializable( coreReplicatedContent ) )
        {
            serializableContent.serialize( channel );
        }
    }

    @Override
    protected ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        ContentBuilder<ReplicatedContent> contentBuilder = read( channel );
        while ( !contentBuilder.isComplete() )
        {
            contentBuilder = contentBuilder.combine( read( channel ) );
        }
        return contentBuilder.build();
    }
}
