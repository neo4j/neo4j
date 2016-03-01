/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.log;

import java.io.IOException;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.coreedge.raft.log.PhysicalRaftLog.RecordType.APPEND;

public class RaftLogAppendRecord extends RaftLogRecord
{
    private final RaftLogEntry logEntry;

    RaftLogAppendRecord( long logIndex, RaftLogEntry logEntry )
    {
        super( APPEND, logIndex );
        this.logEntry = logEntry;
    }

    public RaftLogEntry getLogEntry()
    {
        return logEntry;
    }

    public static RaftLogAppendRecord read( ReadableChannel channel, ChannelMarshal<ReplicatedContent> marshal ) throws IOException
    {
        long appendIndex = channel.getLong();
        long term = channel.getLong();
        ReplicatedContent content = marshal.unmarshal( channel );
        if ( content == null )
        {
            throw ReadPastEndException.INSTANCE;
        }
        return new RaftLogAppendRecord( appendIndex, new RaftLogEntry( term, content ) );
    }

    public static void write( WritableChannel channel, ChannelMarshal<ReplicatedContent> marshal, long appendIndex, long term, ReplicatedContent content ) throws IOException
    {
        channel.put( APPEND.value() );
        channel.putLong( appendIndex );
        channel.putLong( term );
        marshal.marshal( content, channel );
    }

    @Override
    public String toString()
    {
        return String.format( "RaftLogAppendRecord{%s, %s}", super.toString(), logEntry );
    }
}
