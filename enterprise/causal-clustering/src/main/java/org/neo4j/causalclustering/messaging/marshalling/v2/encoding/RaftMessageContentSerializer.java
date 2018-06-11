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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentType;

import static org.neo4j.causalclustering.messaging.marshalling.v2.encoding.RaftLogEntryTermEncoder.serializable;

/**
 * Serializes a raft messages content in the order Message, RaftLogTerms, ReplicatedContent.
 */
public class RaftMessageContentSerializer extends MessageToMessageEncoder<RaftMessages.ClusterIdAwareMessage>
{

    private final CoreReplicatedContentMarshal serializer;
    private Handler replicatedContentHandler = new Handler();

    public RaftMessageContentSerializer( CoreReplicatedContentMarshal serializer )
    {
        this.serializer = serializer;
    }

    @Override
    protected void encode( ChannelHandlerContext ctx, RaftMessages.ClusterIdAwareMessage msg, List<Object> out ) throws Exception
    {
        out.add( ContentType.Message );
        out.add( msg );
        Object[] dispatch = msg.message().dispatch( replicatedContentHandler );
        if ( dispatch == null )
        {
            // there was an error serializing
            throw new IllegalArgumentException( "Error reading raft message content" );
        }
        out.addAll( Arrays.asList( dispatch ) );
    }

    private class Handler implements RaftMessages.Handler<Object[],Exception>
    {
        @Override
        public Object[] handle( RaftMessages.Vote.Request request ) throws Exception
        {
            return noContent();
        }

        @Override
        public Object[] handle( RaftMessages.Vote.Response response ) throws Exception
        {
            return noContent();
        }

        @Override
        public Object[] handle( RaftMessages.PreVote.Request request ) throws Exception
        {
            return noContent();
        }

        @Override
        public Object[] handle( RaftMessages.PreVote.Response response ) throws Exception
        {
            return noContent();
        }

        @Override
        public Object[] handle( RaftMessages.AppendEntries.Request request ) throws Exception
        {
            Stream<Object> terms = Stream.of( ContentType.RaftLogEntryTerms,
                    serializable( Arrays.stream( request.entries() ).mapToLong( RaftLogEntry::term ).toArray() ) );

            Stream<Object> contents = Arrays.stream( request.entries() )
                    .flatMap( entry -> serializableContents( entry.content() ) );

            return Stream.concat( terms, contents ).toArray( Object[]::new );
        }

        @Override
        public Object[] handle( RaftMessages.AppendEntries.Response response ) throws Exception
        {
            return noContent();
        }

        @Override
        public Object[] handle( RaftMessages.Heartbeat heartbeat ) throws Exception
        {
            return noContent();
        }

        @Override
        public Object[] handle( RaftMessages.LogCompactionInfo logCompactionInfo ) throws Exception
        {
            return noContent();
        }

        @Override
        public Object[] handle( RaftMessages.HeartbeatResponse heartbeatResponse ) throws Exception
        {
            return noContent();
        }

        @Override
        public Object[] handle( RaftMessages.NewEntry.Request request ) throws Exception
        {
            return serializableContents( request.content() ).toArray();
        }

        @Override
        public Object[] handle( RaftMessages.Timeout.Election election ) throws Exception
        {
            return illegalOutbound( election );
        }

        @Override
        public Object[] handle( RaftMessages.Timeout.Heartbeat heartbeat ) throws Exception
        {
            return illegalOutbound( heartbeat );
        }

        @Override
        public Object[] handle( RaftMessages.NewEntry.BatchRequest batchRequest ) throws Exception
        {
            return illegalOutbound( batchRequest );
        }

        @Override
        public Object[] handle( RaftMessages.PruneRequest pruneRequest ) throws Exception
        {
            return illegalOutbound( pruneRequest );
        }

        private Object[] noContent()
        {
            return new Object[]{};
        }

        private Object[] illegalOutbound( RaftMessages.BaseRaftMessage raftMessage )
        {
            // not network
            throw new IllegalStateException( "Illegal outbound call: " + raftMessage.getClass() );
        }

        private Stream<Object> serializableContents( ReplicatedContent content )
        {
            return Stream.concat( Stream.of( ContentType.ReplicatedContent ), serializer.toSerializable( content ).stream() );
        }
    }
}
