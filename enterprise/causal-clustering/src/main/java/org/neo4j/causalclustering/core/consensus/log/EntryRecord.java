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
package org.neo4j.causalclustering.core.consensus.log;

import java.io.IOException;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * A log entry and its log index.
 */
public class EntryRecord
{
    private final long logIndex;
    private final RaftLogEntry logEntry;

    public EntryRecord( long logIndex, RaftLogEntry logEntry )
    {
        this.logIndex = logIndex;
        this.logEntry = logEntry;
    }

    public RaftLogEntry logEntry()
    {
        return logEntry;
    }

    public long logIndex()
    {
        return logIndex;
    }

    public static EntryRecord read( ReadableChannel channel, ChannelMarshal<ReplicatedContent> contentMarshal )
            throws IOException, EndOfStreamException
    {
        try
        {
            long appendIndex = channel.getLong();
            long term = channel.getLong();
            ReplicatedContent content = contentMarshal.unmarshal( channel );
            return new EntryRecord( appendIndex, new RaftLogEntry( term, content ) );
        }
        catch ( ReadPastEndException e )
        {
            throw new EndOfStreamException( e );
        }
    }

    public static void write( WritableChannel channel, ChannelMarshal<ReplicatedContent> contentMarshal,
            long logIndex, long term, ReplicatedContent content ) throws IOException
    {
        channel.putLong( logIndex );
        channel.putLong( term );
        contentMarshal.marshal( content, channel );
    }

    @Override
    public String toString()
    {
        return String.format( "%d: %s", logIndex, logEntry );
    }
}
