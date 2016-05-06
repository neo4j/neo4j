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
package org.neo4j.coreedge.raft.log.segmented;

import static org.neo4j.coreedge.raft.log.EntryRecord.read;

import java.io.File;
import java.io.IOException;

import org.neo4j.coreedge.raft.log.EntryRecord;
import org.neo4j.coreedge.raft.log.EntryRecordCursor;
import org.neo4j.coreedge.raft.log.LogPosition;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * Keeps track of a segment of the RAFT log, i.e. a consecutive set of entries.
 * A segment can have several concurrent readers but just a single writer.
 *
 * The single writer should perform all write and control operations,
 * since these are not thread-safe.
 *
 * Concurrent reading is thread-safe.
 */
class SegmentFile
{
    private static final SegmentHeader.Marshal headerMarshal = new SegmentHeader.Marshal();

    private final Log log;
    private final FileSystemAbstraction fileSystem;
    private final File file;
    private final StoreChannelPool readerPool;
    private final ChannelMarshal<ReplicatedContent> contentMarshal;

    private final SegmentHeader header;

    private PhysicalFlushableChannel bufferedWriter;
    private boolean markedForDisposal;
    private Runnable onDisposal;
    private volatile boolean isDisposed;

    SegmentFile(
            FileSystemAbstraction fileSystem,
            File file,
            ChannelMarshal<ReplicatedContent> contentMarshal,
            LogProvider logProvider,
            SegmentHeader header )
    {
        this.fileSystem = fileSystem;
        this.file = file;
        this.contentMarshal = contentMarshal;
        this.header = header;

        log = logProvider.getLog( getClass() );
        readerPool = new StoreChannelPool( fileSystem, file, "r", logProvider );
    }

    static SegmentFile create(
            FileSystemAbstraction fileSystem,
            File file,
            ChannelMarshal<ReplicatedContent> contentMarshal,
            LogProvider logProvider,
            SegmentHeader header ) throws IOException
    {
        SegmentFile segment = new SegmentFile( fileSystem, file, contentMarshal, logProvider, header );

        if ( fileSystem.fileExists( file ) )
        {
            throw new IllegalStateException( "File was not expected to exist" );
        }

        headerMarshal.marshal( header, segment.getOrCreateWriter() );
        return segment;
    }

    /**
     * Channels must be closed when no longer used, so that they are released back to the pool of readers.
     */
    IOCursor<EntryRecord> getReader( long logIndex ) throws IOException, DisposedException
    {
        assert logIndex > header.prevIndex();
        long offsetIndex = logIndex - (header.prevIndex() + 1);

        LogPosition position = findCachedStartingPosition( offsetIndex );

        ReadAheadChannel<StoreChannel> reader =
                new ReadAheadChannel<StoreChannel>( readerPool.acquire( position.byteOffset ) )
                {
                    @Override
                    public void close()
                    {
                        // TODO: Consider including position of channel.
                        readerPool.release( channel );
                    }
                };

        long currentIndex = position.logIndex;
        assert currentIndex == 0; // until we properly implement caching
        try
        {
            while ( currentIndex < offsetIndex )
            {
                read( reader, contentMarshal );
                currentIndex++;
            }
        }
        catch ( ReadPastEndException e )
        {
            reader.close();
            return IOCursor.getEmpty();
        }

        return new EntryRecordCursor( reader, contentMarshal, currentIndex, logPosition -> {
            try
            {
                // TODO: Cache the end position.
                // TODO: explain why we need the end position in the cache
                reader.close();
            }
            catch ( IOException e )
            {
                log.error( "Failed to close reader: " + file );
            }
        } );
    }

    private LogPosition findCachedStartingPosition( long offsetIndex )
    {
        // TODO: Implement cache lookups.
        return new LogPosition( 0, SegmentHeader.SIZE );
    }

    private PhysicalFlushableChannel getOrCreateWriter() throws IOException
    {
        if ( bufferedWriter == null )
        {
            StoreChannel channel = fileSystem.open( file, "rw" );
            channel.position( channel.size() );
            bufferedWriter = new PhysicalFlushableChannel( channel );
        }
        return bufferedWriter;
    }

    /**
     * There is just a single writer and it is closed when the segment is disposed of.
     */
    WritableChannel writer() throws IOException, DisposedException
    {
        if( markedForDisposal )
        {
            throw new DisposedException();
        }
        return getOrCreateWriter();
    }

    long position() throws IOException
    {
        return getOrCreateWriter().position();
    }

    void closeWriter()
    {
        if ( bufferedWriter != null )
        {
            try
            {
                flush();
                bufferedWriter.close();
                bufferedWriter = null;
                checkFullDisposal();
            }
            catch ( IOException e )
            {
                log.error( "Failed to close writer: " + bufferedWriter, e );
            }
        }
    }

    public void write( long logIndex, RaftLogEntry entry ) throws IOException
    {
        EntryRecord.write( getOrCreateWriter(), contentMarshal, logIndex, entry.term(), entry.content() );
        flush();
    }

    void flush() throws IOException
    {
        bufferedWriter.prepareForFlush().flush();
    }

    /**
     * Marks this segment for eventual disposal. The segment will not be disposed of until
     * all readers and writers to the segment file are closed.
     *
     * @param onDisposal Called when the segment is fully disposed with no readers and a closed writer.
     * @throws DisposedException Thrown if this segment already is marked for disposal.
     */
    void markForDisposal( Runnable onDisposal ) throws DisposedException
    {
        if( markedForDisposal )
        {
            throw new DisposedException();
        }

        this.onDisposal = onDisposal;
        this.markedForDisposal = true;

        readerPool.markForDisposal( this::checkFullDisposal );
    }

    private synchronized void checkFullDisposal()
    {
        if ( bufferedWriter == null && readerPool.isDisposed() )
        {
            isDisposed = true;
            onDisposal.run();
        }
    }

    boolean isDisposed()
    {
        return isDisposed;
    }

    public void delete()
    {
        fileSystem.deleteFile( file );
    }

    public SegmentHeader header()
    {
        return header;
    }
}
