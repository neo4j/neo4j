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
package org.neo4j.coreedge.raft.log.physical;

import java.io.IOException;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.storageengine.api.ReadPastEndException;

public class RaftAppendRecordCursor implements IOCursor<RaftLogAppendRecord>
{
    private final ReadAheadChannel<LogVersionedStoreChannel> channel;
    private final ChannelMarshal<ReplicatedContent> marshal;
    private CursorValue<RaftLogAppendRecord> currentRecord = new CursorValue<>();

    public RaftAppendRecordCursor( ReadAheadChannel<LogVersionedStoreChannel> channel, ChannelMarshal<ReplicatedContent> marshal )
    {
        this.channel = channel;
        this.marshal = marshal;
    }

    @Override
    public boolean next() throws IOException
    {
        try
        {
            while ( true )
            {
                byte type = channel.get();
                switch ( PhysicalRaftLog.RecordType.forValue( type ) )
                {
                    case APPEND:
                        currentRecord.set( RaftLogAppendRecord.read( channel, marshal ) );
                        return true;
                    case CONTINUATION:
                        RaftLogContinuationRecord.read( channel );
                        break;
                    default:
                        throw new IllegalStateException( "Not really sure how we got here. Read type value was " + type );
                }
            }
        }
        catch ( ReadPastEndException notEnoughBytes )
        {
            return false;
        }
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @Override
    public RaftLogAppendRecord get()
    {
        return currentRecord.get();
    }
}
