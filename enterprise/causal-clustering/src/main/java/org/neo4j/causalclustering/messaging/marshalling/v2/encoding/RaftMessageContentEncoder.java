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
package org.neo4j.causalclustering.messaging.marshalling.v2.encoding;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.io.IOException;
import java.util.List;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.Codec;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentType;

import static org.neo4j.causalclustering.messaging.marshalling.v2.encoding.RaftLogEntryTermsSerializer.serializeTerms;

/**
 * Serializes a raft messages content in the order Message, RaftLogTerms, ReplicatedContent.
 */
public class RaftMessageContentEncoder extends MessageToMessageEncoder<RaftMessages.ClusterIdAwareMessage>
{

    private final Codec<ReplicatedContent> codec;

    public RaftMessageContentEncoder( Codec<ReplicatedContent> replicatedContentCodec )
    {
        this.codec = replicatedContentCodec;
    }

    @Override
    protected void encode( ChannelHandlerContext ctx, RaftMessages.ClusterIdAwareMessage msg, List<Object> out ) throws Exception
    {
        out.add( msg );
        Handler replicatedContentHandler = new Handler( out, ctx.alloc() );
        msg.message().dispatch( replicatedContentHandler );
    }

    private class Handler implements RaftMessages.Handler<Void,Exception>
    {
        private final List<Object> out;
        private final ByteBufAllocator alloc;

        private Handler( List<Object> out, ByteBufAllocator alloc )
        {
            this.out = out;
            this.alloc = alloc;
        }

        @Override
        public Void handle( RaftMessages.Vote.Request request ) throws Exception
        {
            return null;
        }

        @Override
        public Void handle( RaftMessages.Vote.Response response ) throws Exception
        {
            return null;
        }

        @Override
        public Void handle( RaftMessages.PreVote.Request request ) throws Exception
        {
            return null;
        }

        @Override
        public Void handle( RaftMessages.PreVote.Response response ) throws Exception
        {
            return null;
        }

        @Override
        public Void handle( RaftMessages.AppendEntries.Request request ) throws Exception
        {
            out.add( serializeTerms( request.entries(), alloc ) );
            for ( RaftLogEntry entry : request.entries() )
            {
                serializableContents( entry.content(), out );
            }
            return null;
        }

        @Override
        public Void handle( RaftMessages.AppendEntries.Response response ) throws Exception
        {
            return null;
        }

        @Override
        public Void handle( RaftMessages.Heartbeat heartbeat ) throws Exception
        {
            return null;
        }

        @Override
        public Void handle( RaftMessages.LogCompactionInfo logCompactionInfo ) throws Exception
        {
            return null;
        }

        @Override
        public Void handle( RaftMessages.HeartbeatResponse heartbeatResponse ) throws Exception
        {
            return null;
        }

        @Override
        public Void handle( RaftMessages.NewEntry.Request request ) throws Exception
        {
            serializableContents( request.content(), out );
            return null;
        }

        @Override
        public Void handle( RaftMessages.Timeout.Election election ) throws Exception
        {
            return illegalOutbound( election );
        }

        @Override
        public Void handle( RaftMessages.Timeout.Heartbeat heartbeat ) throws Exception
        {
            return illegalOutbound( heartbeat );
        }

        @Override
        public Void handle( RaftMessages.NewEntry.BatchRequest batchRequest ) throws Exception
        {
            return illegalOutbound( batchRequest );
        }

        @Override
        public Void handle( RaftMessages.PruneRequest pruneRequest ) throws Exception
        {
            return illegalOutbound( pruneRequest );
        }

        private Void illegalOutbound( RaftMessages.BaseRaftMessage raftMessage )
        {
            // not network
            throw new IllegalStateException( "Illegal outbound call: " + raftMessage.getClass() );
        }

        private void serializableContents( ReplicatedContent content, List<Object> out ) throws IOException
        {
            out.add( ContentType.ReplicatedContent );
            codec.encode( content, out );
        }
    }
}
