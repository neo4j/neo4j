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
package org.neo4j.causalclustering.messaging.marshalling.v2.encoding;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentType;
import org.neo4j.causalclustering.messaging.marshalling.v2.CoreReplicatedContentSerializer;

public class RaftMessageContentEncoder extends MessageToMessageEncoder<RaftMessages.ClusterIdAwareMessage>
{

    private final CoreReplicatedContentSerializer serializer;

    public RaftMessageContentEncoder( CoreReplicatedContentSerializer serializer )
    {
        this.serializer = serializer;
    }

    @Override
    protected void encode( ChannelHandlerContext ctx, RaftMessages.ClusterIdAwareMessage msg, List<Object> out ) throws Exception
    {
        Object[] dispatch = msg.message().dispatch( new Handler() );
        if ( dispatch == null )
        {
            // there was an error serializing
            throw new IllegalArgumentException( "Error reading raft message content" );
        }
        else if ( dispatch.length != 0 )
        {
            out.addAll( Arrays.asList( dispatch ) );
        }
        out.add( ContentType.Message );
        out.add( msg );
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
            return Arrays
                    .stream( request.entries() )
                    .flatMap( entry -> Stream.concat( serializableContents( entry.content() ),
                            Stream.of( ContentType.RaftLogEntries, RaftLogEntryTermEncoder.serializable( entry.term() ) ) ) )
                    .toArray( Object[]::new );
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
