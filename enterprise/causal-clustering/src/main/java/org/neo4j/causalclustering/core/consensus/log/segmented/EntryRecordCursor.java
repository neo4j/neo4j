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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.EntryRecord;
import org.neo4j.causalclustering.core.consensus.log.LogPosition;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;

import static org.neo4j.causalclustering.core.consensus.log.EntryRecord.read;

/**
 * A cursor for iterating over RAFT log entries starting at an index and until the end of the segment is met.
 * The segment is demarcated by the ReadAheadChannel provided, which should properly signal the end of the channel.
 */
class EntryRecordCursor implements IOCursor<EntryRecord>
{
    private ReadAheadChannel<StoreChannel> bufferedReader;

    private final LogPosition position;
    private final CursorValue<EntryRecord> currentRecord = new CursorValue<>();
    private final Reader reader;
    private ChannelMarshal<ReplicatedContent> contentMarshal;
    private final SegmentFile segment;

    private boolean hadError;
    private boolean closed;

    EntryRecordCursor( Reader reader, ChannelMarshal<ReplicatedContent> contentMarshal,
            long currentIndex, long wantedIndex, SegmentFile segment ) throws IOException, EndOfStreamException
    {
        this.bufferedReader = new ReadAheadChannel<>( reader.channel() );
        this.reader = reader;
        this.contentMarshal = contentMarshal;
        this.segment = segment;

        /* The cache lookup might have given us an earlier position, scan forward to the exact position. */
        while ( currentIndex < wantedIndex )
        {
            read( bufferedReader, contentMarshal );
            currentIndex++;
        }

        this.position = new LogPosition( currentIndex, bufferedReader.position() );
    }

    @Override
    public boolean next() throws IOException
    {
        EntryRecord entryRecord;
        try
        {
            entryRecord = read( bufferedReader, contentMarshal );
        }
        catch ( EndOfStreamException e )
        {
            currentRecord.invalidate();
            return false;
        }
        catch ( IOException e )
        {
            hadError = true;
            throw e;
        }

        currentRecord.set( entryRecord );
        position.byteOffset = bufferedReader.position();
        position.logIndex++;
        return true;
    }

    @Override
    public void close() throws IOException
    {
        if ( closed )
        {
            /* This is just a defensive measure, for catching user errors from messing up the refCount. */
            throw new IllegalStateException( "Already closed" );
        }

        bufferedReader = null;
        closed = true;
        segment.refCount().decrease();

        if ( hadError )
        {
            /* If the reader had en error, then it should be closed instead of returned to the pool. */
            reader.close();
        }
        else
        {
            segment.positionCache().put( position );
            segment.readerPool().release( reader );
        }
    }

    @Override
    public EntryRecord get()
    {
        return currentRecord.get();
    }
}
