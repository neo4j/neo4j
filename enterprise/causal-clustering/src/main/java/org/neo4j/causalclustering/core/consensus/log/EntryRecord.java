/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
