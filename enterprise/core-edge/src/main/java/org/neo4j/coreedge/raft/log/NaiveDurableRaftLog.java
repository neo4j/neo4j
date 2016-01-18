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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.coreedge.raft.log.monitoring.RaftLogAppendIndexMonitor;
import org.neo4j.coreedge.raft.log.monitoring.RaftLogCommitIndexMonitor;
import org.neo4j.coreedge.raft.replication.MarshallingException;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Serializer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.coreedge.raft.log.RaftLog.*;


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
 * 3. commit.log
 * ┌─────────────────────────────┐
 * │committedIndex        8 bytes│
 * ├─────────────────────────────┤
 * │record length         8 bytes│
 * └─────────────────────────────┘
 */
public class NaiveDurableRaftLog extends LifecycleAdapter implements RaftLog
{
    public static final int ENTRY_RECORD_LENGTH = 16;
    public static final int CONTENT_LENGTH_BYTES = 4;
    public static final int COMMIT_INDEX_BYTES = 8;
    public static final String DIRECTORY_NAME = "raft-log";

    private final ByteBuffer entryRecordBuffer = ByteBuffer.allocate( ENTRY_RECORD_LENGTH );
    private final ByteBuffer contentLengthBuffer = ByteBuffer.allocate( CONTENT_LENGTH_BYTES );
    private final ByteBuffer commitIndexBuffer = ByteBuffer.allocate( COMMIT_INDEX_BYTES );


    private final Set<Listener> listeners = new CopyOnWriteArraySet<>();

    private final StoreChannel entriesChannel;
    private final StoreChannel contentChannel;
    private final StoreChannel commitChannel;

    private final Serializer serializer;
    private long appendIndex = -1;
    private long contentOffset;
    private long commitIndex = -1;
    private long term = -1;

    public NaiveDurableRaftLog( FileSystemAbstraction fileSystem, File directory, Serializer serializer)
    {
        this.serializer = serializer;

        directory.mkdirs();

        try
        {
            entriesChannel = fileSystem.open( new File( directory, "entries.log" ), "rw" );
            contentChannel = fileSystem.open( new File( directory, "content.log" ), "rw" );
            commitChannel = fileSystem.open( new File( directory, "commit.log" ), "rw" );
            appendIndex = entriesChannel.size() / ENTRY_RECORD_LENGTH - 1;
            contentOffset = contentChannel.size();
            commitIndex = readCommitIndex();
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
        boolean shouldThrow = false;
        shouldThrow = forceAndCloseChannel( entriesChannel, container );
        shouldThrow = forceAndCloseChannel( contentChannel, container ) || shouldThrow;
        shouldThrow = forceAndCloseChannel( commitChannel, container ) || shouldThrow;
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
    public void replay() throws Throwable
    {
        int index = 0;
        for (; index <= commitIndex; index++ )
        {
            ReplicatedContent content = readEntryContent( index );
            for ( Listener listener : listeners )
            {
                listener.onAppended( content, index );
                listener.onCommitted( content, index );
            }
        }
        for (; index <= appendIndex; index++ )
        {
            ReplicatedContent content = readEntryContent( index );
            for ( Listener listener : listeners )
            {
                listener.onAppended( content, index );
            }
        }
    }

    @Override
    public void registerListener( Listener listener )
    {
        listeners.add( listener );
    }

    @Override
    public long append( RaftLogEntry logEntry ) throws RaftStorageException
    {
        if ( logEntry.term() >= term )
        {
            term = logEntry.term();
        }
        else
        {
            throw new RaftStorageException( String.format( "Non-monotonic term %d for in entry %s in term %d",
                    logEntry.term(), logEntry.toString(), term ) );
        }

        try
        {
            int length = writeContent( logEntry );
            writeEntry( new Entry( logEntry.term(), contentOffset ) );
            contentOffset += length;
            appendIndex++;
            for ( Listener listener : listeners )
            {
                listener.onAppended( logEntry.content(), appendIndex );
            }
            return appendIndex;
        }
        catch ( MarshallingException | IOException e )
        {
            throw new RaftStorageException( "Failed to append log entry", e );
        }

    }

    @Override
    public void truncate( long fromIndex ) throws RaftStorageException
    {
        try
        {
            if ( fromIndex <= commitIndex )
            {
                throw new IllegalArgumentException( "cannot truncate before the commit index" );
            }

            if ( appendIndex >= fromIndex )
            {
                Entry entry = readEntry( fromIndex );
                contentChannel.truncate( entry.contentPointer );
                contentOffset = entry.contentPointer;

                entriesChannel.truncate( ENTRY_RECORD_LENGTH * fromIndex );
                entriesChannel.force( false );

                appendIndex = fromIndex - 1;

                for ( Listener listener : listeners )
                {
                    listener.onTruncated( fromIndex );
                }
            }
            term = readEntryTerm( appendIndex );
        }
        catch ( IOException e )
        {
            throw new RaftStorageException( "Failed to truncate", e );
        }
    }

    @Override
    public void commit( final long newCommitIndex ) throws RaftStorageException
    {
        if ( commitIndex == appendIndex )
        {
            return;
        }
        long actualNewCommitIndex = newCommitIndex;
        if ( newCommitIndex > appendIndex )
        {
            actualNewCommitIndex = appendIndex;
        }
        // INVARIANT: If newCommitIndex was greater than appendIndex, commitIndex is equal to appendIndex
        try
        {
            storeCommitIndex( actualNewCommitIndex );
        }
        catch ( IOException e )
        {
            throw new RaftStorageException( "Failed to commit", e );
        }

        while ( commitIndex < actualNewCommitIndex )
        {
            commitIndex++;
            for ( Listener listener : listeners )
            {
                ReplicatedContent content = readEntryContent( commitIndex );
                listener.onCommitted( content, commitIndex );
            }
        }
    }

    @Override
    public long appendIndex()
    {
        return appendIndex;
    }

    @Override
    public long commitIndex()
    {
        return commitIndex;
    }

    @Override
    public RaftLogEntry readLogEntry( long logIndex ) throws RaftStorageException
    {
        try
        {
            Entry entry = readEntry( logIndex );
            ReplicatedContent content = readContentFrom( entry.contentPointer );

            return new RaftLogEntry( entry.term, content );
        }
        catch ( IOException | MarshallingException e )
        {
            throw new RaftStorageException( "Failed to read log entry", e );
        }
    }

    @Override
    public ReplicatedContent readEntryContent( long logIndex ) throws RaftStorageException
    {
        return readLogEntry( logIndex ).content();
    }

    @Override
    public long readEntryTerm( long logIndex ) throws RaftStorageException
    {
        try
        {
            return readEntry( logIndex ).term;
        }
        catch ( IOException e )
        {
            throw new RaftStorageException( "Failed to read term", e );
        }
    }

    @Override
    public boolean entryExists( long logIndex )
    {
        return appendIndex >= logIndex;
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
    }

    private void writeEntry( Entry entry ) throws IOException
    {
        entryRecordBuffer.clear();
        entryRecordBuffer.putLong( entry.term );
        entryRecordBuffer.putLong( entry.contentPointer );
        entryRecordBuffer.flip();

        entriesChannel.writeAll( entryRecordBuffer, (appendIndex + 1) * ENTRY_RECORD_LENGTH );
        entriesChannel.force( false );
    }

    private Entry readEntry( long logIndex ) throws IOException
    {
        if ( logIndex < 0 || logIndex > appendIndex )
        {
            return new Entry( -1, -1 );
        }

        entryRecordBuffer.clear();
        entriesChannel.read( entryRecordBuffer, logIndex * ENTRY_RECORD_LENGTH );
        entryRecordBuffer.flip();
        long term = entryRecordBuffer.getLong();
        long contentPointer = entryRecordBuffer.getLong();
        return new Entry( term, contentPointer );
    }

    private int writeContent( RaftLogEntry logEntry ) throws MarshallingException, IOException
    {
        ByteBuffer contentBuffer = serializer.serialize( logEntry.content() );
        int length = CONTENT_LENGTH_BYTES + contentBuffer.remaining();

        contentLengthBuffer.clear();
        contentLengthBuffer.putInt( length );
        contentLengthBuffer.flip();
        contentChannel.writeAll( contentLengthBuffer, contentOffset );
        contentChannel.writeAll( contentBuffer, contentOffset + CONTENT_LENGTH_BYTES );
        contentChannel.force( false );

        return length;
    }

    private ReplicatedContent readContentFrom( long contentPointer ) throws IOException, MarshallingException
    {
        contentLengthBuffer.clear();
        contentChannel.read( contentLengthBuffer, contentPointer );
        contentLengthBuffer.flip();
        int contentLength = contentLengthBuffer.getInt();

        ByteBuffer contentBuffer = ByteBuffer.allocate( contentLength - CONTENT_LENGTH_BYTES );
        contentChannel.read( contentBuffer, contentPointer + CONTENT_LENGTH_BYTES );
        contentBuffer.flip();
        return serializer.deserialize( contentBuffer );
    }

    private void storeCommitIndex( long commitIndex ) throws IOException
    {
        commitIndexBuffer.clear();
        commitIndexBuffer.putLong( commitIndex );
        commitIndexBuffer.flip();
        commitChannel.writeAll( commitIndexBuffer, 0 );
        commitChannel.force( false );
    }

    private long readCommitIndex() throws IOException
    {
        if ( commitChannel.size() < COMMIT_INDEX_BYTES )
        {
            return -1;
        }
        commitIndexBuffer.clear();
        commitChannel.read( commitIndexBuffer, 0 );
        commitIndexBuffer.flip();
        return commitIndexBuffer.getLong();
    }
}
