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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.coreedge.raft.replication.MarshallingException;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.server.ByteBufMarshal;
import org.neo4j.cursor.CursorValue;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Writes a raft log to disk using 3 files:
 * <p>
 * 1. entries.log
 * ┌─────────────────────────────┐
 * │term                  8 bytes│
 * │contentPointer        8 bytes│
 * ├─────────────────────────────┤
 * │record length        16 bytes│
 * └─────────────────────────────┘
 * <p>
 * 2. content.log
 * ┌─────────────────────────────┐
 * │contentLength         4 bytes│
 * ├─────────────────────────────┤
 * │contentType           1 bytes│
 * │content              variable│
 * ├─────────────────────────────┤
 * │record length        variable│
 * └─────────────────────────────┘
 * <p>
 * 3. meta.log
 * ┌─────────────────────────────┐
 * │prevIndex             8 bytes│
 * │prevTerm              8 bytes│
 * ├─────────────────────────────┤
 * │record length        16 bytes│
 * └─────────────────────────────┘
 */
public class NaiveDurableRaftLog extends LifecycleAdapter implements RaftLog
{
    public static final int ENTRY_RECORD_LENGTH = 16;
    public static final int CONTENT_LENGTH_BYTES = 4;
    public static final int META_BYTES = 8 * 2;
    public static final String NAIVE_LOG_DIRECTORY_NAME = "raft-log";

    private StoreChannel entriesChannel;
    private StoreChannel contentChannel;
    private final StoreChannel metaChannel;

    private final FileSystemAbstraction fileSystem;
    private final File directory;
    private final ByteBufMarshal<ReplicatedContent> marshal;
    private final Log log;
    private long appendIndex = -1;
    private long contentOffset;
    private long term = -1;
    private long prevIndex = -1;
    private long prevTerm = -1;

    public NaiveDurableRaftLog( FileSystemAbstraction fileSystem, File directory,
                                ByteBufMarshal<ReplicatedContent> marshal, LogProvider logProvider )
    {
        this.fileSystem = fileSystem;
        this.directory = directory;
        this.marshal = marshal;

        directory.mkdirs();

        try
        {
            entriesChannel = fileSystem.open( new File( directory, "entries.log" ), "rw" );
            contentChannel = fileSystem.open( new File( directory, "content.log" ), "rw" );
            metaChannel = fileSystem.open( new File( directory, "meta.log" ), "rw" );
            readMetadata();

            appendIndex = prevIndex + entriesChannel.size() / ENTRY_RECORD_LENGTH;
            contentOffset = contentChannel.size();
            log = logProvider.getLog( getClass() );

            log.info( "Raft log created. appendIndex: %d", appendIndex );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        Exception container = new Exception( "Exception happened during shutdown of RaftLog. See suppressed " +
                "exceptions for details" );
        boolean shouldThrow;
        shouldThrow = forceAndCloseChannel( entriesChannel, container );
        shouldThrow = forceAndCloseChannel( contentChannel, container ) || shouldThrow;
        shouldThrow = forceAndCloseChannel( metaChannel, container ) || shouldThrow;
        if ( shouldThrow )
        {
            throw container;
        }
    }

    /**
     * This method will try to force and close a store channel. If any of these two operations fails, the exception
     * will be added as suppressed in the provided container. In such a case, true will be returned.
     *
     * @param channel   The channel to close
     * @param container The container to add supressed exceptions in the case of failure
     * @return True iff an exception was thrown by either force() or close()
     */
    private boolean forceAndCloseChannel( StoreChannel channel, Exception container )
    {
        boolean exceptionHappened = false;
        try
        {
            channel.force( false );
            channel.close();
        }
        catch ( Exception e )
        {
            exceptionHappened = true;
            container.addSuppressed( e );
        }
        return exceptionHappened;
    }

    @Override
    public long append( RaftLogEntry logEntry ) throws IOException
    {
        if ( logEntry.term() >= term )
        {
            term = logEntry.term();
        }
        else
        {
            throw new IllegalStateException( String.format( "Non-monotonic term %d for in entry %s in term %d",
                    logEntry.term(), logEntry.toString(), term ) );
        }

        try
        {
            appendIndex++;
            int length = writeContent( logEntry, contentChannel );
            writeEntry( appendIndex - (prevIndex + 1), new Entry( logEntry.term(), contentOffset ), entriesChannel );
            contentOffset += length;
            return appendIndex;
        }
        catch ( MarshallingException | IOException e )
        {
            throw new IOException( "Failed to append log entry", e );
        }
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        if ( appendIndex >= fromIndex )
        {
            Entry entry = readEntry( fromIndex );
            contentChannel.truncate( entry.contentPointer );
            contentOffset = entry.contentPointer;

            entriesChannel.truncate( ENTRY_RECORD_LENGTH * fromIndex );
            entriesChannel.force( false );

            appendIndex = fromIndex - 1;
        }
        else
        {
            throw new IllegalArgumentException( "Cannot truncate at index " + fromIndex + " when append index is " + appendIndex );
        }
        term = readEntryTerm( appendIndex );
    }

    @Override
    public long prune( long safeIndex ) throws IOException
    {
        try
        {
            if( safeIndex > prevIndex )
            {
                long safeIndexTerm = readEntryTerm( safeIndex );

                StoreChannel tempEntriesChannel = fileSystem.open( new File( directory, "temp-entries.log" ), "rw" );
                StoreChannel tempContentChannel = fileSystem.open( new File( directory, "temp-content.log" ), "rw" );

                contentOffset = 0;
                for ( long i = safeIndex + 1; i <= appendIndex; i++ )
                {
                    RaftLogEntry logEntry = readLogEntry( i );
                    int length = writeContent( logEntry, tempContentChannel );
                    writeEntry( i - (safeIndex + 1), new Entry( logEntry.term(), contentOffset ), tempEntriesChannel );
                    contentOffset += length;
                }
                tempEntriesChannel.close();
                tempContentChannel.close();
                entriesChannel.close();
                contentChannel.close();
                fileSystem.deleteFile( new File( directory, "entries.log" ) );
                fileSystem.deleteFile( new File( directory, "content.log" ) );
                fileSystem.renameFile( new File( directory, "temp-entries.log" ), new File( directory, "entries.log" ) );
                fileSystem.renameFile( new File( directory, "temp-content.log" ), new File( directory, "content.log" ) );
                entriesChannel = fileSystem.open( new File( directory, "entries.log" ), "rw" );
                contentChannel = fileSystem.open( new File( directory, "content.log" ), "rw" );

                prevTerm = safeIndexTerm;
                prevIndex = safeIndex;

                storeMetadata();
            }

            return prevIndex;
        }
        catch ( MarshallingException e )
        {
            throw new IOException( e );
        }
    }

    @Override
    public long skip( long index, long term ) throws IOException
    {
        if( index > appendIndex )
        {
            entriesChannel.close();
            contentChannel.close();
            fileSystem.deleteFile( new File( directory, "entries.log" ) );
            fileSystem.deleteFile( new File( directory, "content.log" ) );
            entriesChannel = fileSystem.open( new File( directory, "entries.log" ), "rw" );
            contentChannel = fileSystem.open( new File( directory, "content.log" ), "rw" );

            appendIndex = index;
            prevIndex = index;
            prevTerm = term;

            storeMetadata();
        }
        return appendIndex;
    }

    @Override
    public long appendIndex()
    {
        return appendIndex;
    }

    @Override
    public long prevIndex()
    {
        return prevIndex;
    }

    private RaftLogEntry readLogEntry( long logIndex ) throws IOException
    {
        Entry entry = readEntry( logIndex );
        ReplicatedContent content;
        try
        {
            content = readContentFrom( entry.contentPointer );
        }
        catch ( MarshallingException e )
        {
            throw new IOException( e );
        }

        return new RaftLogEntry( entry.term, content );
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException
    {
        if( logIndex == prevIndex )
        {
            return prevTerm;
        }
        else if ( logIndex < prevIndex || logIndex > appendIndex )
        {
            return -1;
        }

        return readEntry( logIndex ).term;
    }

    private static class Entry
    {
        private final long term;

        private final long contentPointer;

        public Entry( long term, long contentPointer )
        {
            this.term = term;
            this.contentPointer = contentPointer;
        }
        @Override
        public String toString()
        {
            return "Entry{" +
                    "term=" + term +
                    ", contentPointer=" + contentPointer +
                    '}';
        }
    }

    @Override
    public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
    {
        return new RaftLogCursor()
        {
            private long currentIndex = fromIndex - 1; // the cursor starts "before" the first entry
            private CursorValue<RaftLogEntry> current = new CursorValue<>();

            @Override
            public boolean next() throws IOException
            {
                currentIndex++;

                try
                {
                    current.set( readLogEntry( currentIndex ) );
                    return true;
                }
                catch ( IllegalArgumentException e )
                {
                    current.invalidate();
                }

                return false;
            }

            @Override
            public void close() throws IOException
            {
            }

            @Override
            public long index()
            {
                return currentIndex;
            }

            @Override
            public RaftLogEntry get()
            {
                return current.get();
            }
        };
    }

    private void writeEntry( long index, Entry entry, StoreChannel entriesChannel ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( ENTRY_RECORD_LENGTH );
        buffer.putLong( entry.term );
        buffer.putLong( entry.contentPointer );
        buffer.flip();

        entriesChannel.writeAll( buffer, index * ENTRY_RECORD_LENGTH );
        entriesChannel.force( false );
    }

    private Entry readEntry( long logIndex ) throws IOException
    {
        if ( logIndex <= prevIndex || logIndex > appendIndex )
        {
            throw new IllegalArgumentException("compaction exception");
        }

        ByteBuffer buffer = ByteBuffer.allocate( ENTRY_RECORD_LENGTH );
        entriesChannel.read( buffer, ( logIndex - (prevIndex + 1) ) * ENTRY_RECORD_LENGTH );
        buffer.flip();
        long term = buffer.getLong();
        long contentPointer = buffer.getLong();
        return new Entry( term, contentPointer );
    }

    private int writeContent( RaftLogEntry logEntry, StoreChannel contentChannel ) throws MarshallingException, IOException
    {
        ByteBuf buffer = Unpooled.buffer();
        marshal.marshal( logEntry.content(), buffer );
        ByteBuffer contentBuffer = buffer.internalNioBuffer( 0, buffer.writerIndex() );
        int length = CONTENT_LENGTH_BYTES + contentBuffer.remaining();

        ByteBuffer contentLengthBuffer = ByteBuffer.allocate( CONTENT_LENGTH_BYTES );
        contentLengthBuffer.putInt( length );
        contentLengthBuffer.flip();
        contentChannel.writeAll( contentLengthBuffer, contentOffset );
        contentChannel.writeAll( contentBuffer, contentOffset + CONTENT_LENGTH_BYTES );
        contentChannel.force( false );

        return length;
    }

    private ReplicatedContent readContentFrom( long contentPointer ) throws IOException, MarshallingException
    {
        ByteBuffer lengthBuffer = ByteBuffer.allocate( CONTENT_LENGTH_BYTES );
        contentChannel.read( lengthBuffer, contentPointer );
        lengthBuffer.flip();
        int contentLength = lengthBuffer.getInt();

        ByteBuffer contentBuffer = ByteBuffer.allocate( contentLength - CONTENT_LENGTH_BYTES );
        contentChannel.read( contentBuffer, contentPointer + CONTENT_LENGTH_BYTES );
        contentBuffer.flip();
        ByteBuf byteBuf = Unpooled.wrappedBuffer( contentBuffer );
        return marshal.unmarshal( byteBuf );
    }

    private void storeMetadata() throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( META_BYTES );
        buffer.putLong( prevIndex );
        buffer.putLong( prevTerm );
        buffer.flip();
        metaChannel.writeAll( buffer, 0 );
        metaChannel.force( false );
    }

    private void readMetadata() throws IOException
    {
        if ( metaChannel.size() < META_BYTES )
        {
            prevIndex = -1;
            prevTerm = -1;
        }
        else
        {
            ByteBuffer buffer = ByteBuffer.allocate( META_BYTES );
            metaChannel.read( buffer, 0 );
            buffer.flip();
            prevIndex = buffer.getLong();
            prevTerm = buffer.getLong();
        }
    }
}
