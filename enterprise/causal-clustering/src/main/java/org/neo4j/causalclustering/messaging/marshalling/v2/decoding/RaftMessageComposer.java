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
package org.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;

public class RaftMessageComposer extends MessageToMessageDecoder<Object>
{
    private final Queue<ReplicatedContent> replicatedContents = new LinkedBlockingQueue<>();
    private final RaftLogEntries raftLogEntries = new RaftLogEntries();
    private final Clock clock;

    public RaftMessageComposer( Clock clock )
    {
        this.clock = clock;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, Object msg, List<Object> out )
    {
        if ( msg instanceof ReplicatedContent )
        {
            replicatedContents.add( (ReplicatedContent) msg );
        }
        else if ( msg instanceof RaftLogEntryTermDecoder.RaftLogEntryTerm )
        {
            long term = ((RaftLogEntryTermDecoder.RaftLogEntryTerm) msg).term();
            raftLogEntries.add( new RaftLogEntry( term, replicatedContents.poll() ) );
        }
        else if ( msg instanceof RaftMessageDecoder.RaftMessageCreator )
        {
            RaftMessageDecoder.RaftMessageCreator messageCreator = (RaftMessageDecoder.RaftMessageCreator) msg;
            out.add( RaftMessages.ReceivedInstantClusterIdAwareMessage.of( clock.instant(), messageCreator.clusterId(),
                    messageCreator.result().apply( raftLogEntries, replicatedContents::poll ) ) );
        }
        else
        {
            throw new IllegalStateException( "Unexpected object in the pipeline: " + msg );
        }
    }

    private class RaftLogEntries implements Supplier<RaftLogEntry[]>
    {
        private final ArrayList<RaftLogEntry> raftLogEntries = new ArrayList<>();

        void add( RaftLogEntry raftLogEntry )
        {
            raftLogEntries.add( raftLogEntry );
        }

        @Override
        public RaftLogEntry[] get()
        {
            RaftLogEntry[] array = this.raftLogEntries.toArray( RaftLogEntry.empty );
            raftLogEntries.clear();
            return array;
        }
    }
}
