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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class RaftLogEntryTermEncoder extends MessageToByteEncoder<RaftLogEntryTermEncoder.RaftLogEntryTermSerializer>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, RaftLogEntryTermSerializer msg, ByteBuf out )
    {
        out.writeLong( msg.term );
    }

    static RaftLogEntryTermSerializer serializable( long term )
    {
        return new RaftLogEntryTermSerializer( term );
    }

    static class RaftLogEntryTermSerializer
    {
        private final long term;

        private RaftLogEntryTermSerializer( long term )
        {
            this.term = term;
        }
    }
}
