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

import static org.neo4j.coreedge.raft.log.EntryRecord.read;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;

/**
 * A cursor for iterating over RAFT log entries starting at an index and until the end of the segment is met.
 * The segment is demarcated by the ReadAheadChannel provided, which should properly signal the end of the channel.
 */
public class EntryRecordCursor implements IOCursor<EntryRecord>
{
    private final ReadAheadChannel<StoreChannel> reader;

    private final LogPosition position;
    private final CursorValue<EntryRecord> currentRecord = new CursorValue<>();
    private ChannelMarshal<ReplicatedContent> contentMarshal;
    private final Consumer<LogPosition> onClose;

    public EntryRecordCursor(
            ReadAheadChannel<StoreChannel> reader,
            ChannelMarshal<ReplicatedContent> contentMarshal,
            long startIndex,
            Consumer<LogPosition> onClose ) throws IOException
    {
        this.reader = reader;
        this.contentMarshal = contentMarshal;
        this.onClose = onClose;
        position = new LogPosition( startIndex, reader.position() );
    }

    @Override
    public boolean next() throws IOException
    {
        EntryRecord entryRecord = read( reader, contentMarshal );
        if ( entryRecord != null )
        {
            currentRecord.set( entryRecord );
            position.logIndex++;
            position.byteOffset = reader.position();
            return true;
        }
        else
        {
            currentRecord.invalidate();
            return false;
        }
    }

    @Override
    public void close() throws IOException
    {
        onClose.accept( position );
    }

    @Override
    public EntryRecord get()
    {
        return currentRecord.get();
    }
}
