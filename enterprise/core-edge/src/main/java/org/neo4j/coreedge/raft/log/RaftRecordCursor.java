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
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.storageengine.api.ReadPastEndException;

public class RaftRecordCursor<T extends ReadableClosablePositionAwareChannel> implements IOCursor<RaftLogRecord>
{
    private final T channel;
    private final ChannelMarshal<ReplicatedContent> marshal;
    private RaftLogRecord lastFoundRecord;

    public RaftRecordCursor( T channel, ChannelMarshal<ReplicatedContent> marshal )
    {
        this.channel = channel;
        this.marshal = marshal;
    }

    @Override
    public boolean next() throws IOException
    {
        try
        {
            byte type = channel.get();
            switch ( PhysicalRaftLog.RecordType.forValue( type ) )
            {
                case APPEND:
                    lastFoundRecord = RaftLogAppendRecord.read( channel, marshal );
                    return true;
                case COMMIT:
                    lastFoundRecord = RaftLogCommitRecord.read( channel );
                    return true;
                case CONTINUATION:
                    lastFoundRecord = RaftLogContinuationRecord.read( channel );
                    return true;
                default:
                    throw new IllegalStateException( "Not really sure how we got here. Read type value was " + type );
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
    public RaftLogRecord get()
    {
        return lastFoundRecord;
    }
}
