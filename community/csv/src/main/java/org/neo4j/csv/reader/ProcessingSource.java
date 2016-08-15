/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.csv.reader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.neo4j.csv.reader.Source.Chunk;

/**
 * In a scenario where there's one reader reading chunks of data, handing those chunks to one or
 * more processors (parsers) of that data, this class comes in handy. This pattern allows for
 * multiple {@link BufferedCharSeeker seeker instances}, each operating over one chunk, not transitioning itself
 * into the next.
 */
public class ProcessingSource implements Closeable
{
    // Marker for a buffer slot being unallocated
    private static final char[] UNALLOCATED = new char[0];
    // Marker for a buffer being allocated, although currently used
    private static final char[] IN_USE = new char[0];

    private final CharReadable reader;
    private final int chunkSize;
    private char[] backBuffer; // grows on demand
    private int backBufferCursor;

    // Buffer reuse. Each item starts out as UNALLOCATED, transitions into IN_USE and tied to a Chunk,
    // which will put its allocated buffer back into that slot on Chunk#close(). After that flipping between
    // an allocated char[] and IN_USE.
    private final AtomicReferenceArray<char[]> buffers;

    public ProcessingSource( CharReadable reader, int chunkSize, int maxNumberOfBufferedChunks )
    {
        this.reader = reader;
        this.chunkSize = chunkSize;
        this.backBuffer = new char[chunkSize >> 4];
        this.buffers = new AtomicReferenceArray<>( maxNumberOfBufferedChunks );
        for ( int i = 0; i < buffers.length(); i++ )
        {
            buffers.set( i, UNALLOCATED );
        }
    }

    /**
     * Must be called by a single thread, the same thread every time.
     *
     * @return the next {@link Chunk} of data, ending with a new-line or not for the last chunk.
     * @throws IOException on reading error.
     */
    public Chunk nextChunk() throws IOException
    {
        Buffer buffer = newBuffer();
        int offset = 0;

        if ( backBufferCursor > 0 )
        {   // Read from and reset back buffer
            assert backBufferCursor < chunkSize;
            System.arraycopy( backBuffer, 0, buffer.data, 0, backBufferCursor );
            offset += backBufferCursor;
            backBufferCursor = 0;
        }

        int leftToRead = chunkSize - offset;
        int read = reader.read( buffer.data, offset, leftToRead );
        if ( read == leftToRead )
        {   // Read from reader. We read data into the whole buffer and there seems to be more data left in reader.
            // This means we're most likely not at the end so seek backwards to the last newline character and
            // put the characters after the newline character(s) into the back buffer.
            int newlineOffset = offsetOfLastNewline( buffer.data );
            if ( newlineOffset > -1 )
            {   // We found a newline character some characters back
                backBufferCursor = chunkSize - (newlineOffset + 1);
                System.arraycopy( buffer.data, newlineOffset + 1, backBuffer( backBufferCursor ), 0, backBufferCursor );
                read -= backBufferCursor;
            }
            else
            {   // There was no newline character, isn't that weird?
                throw new IllegalStateException( "Weird input data, no newline character in the whole buffer " +
                        chunkSize + ", not supported a.t.m." );
            }
        }
        // else we couldn't completely fill the buffer, this means that we're at the end of a data source, we're good.

        if ( read > -1 )
        {
            offset += read;
        }

        return new ProcessingChunk( buffer, offset, reader.sourceDescription() );
    }

    private char[] backBuffer( int length )
    {
        if ( length > backBuffer.length )
        {
            backBuffer = Arrays.copyOf( backBuffer, length );
        }
        return backBuffer;
    }

    private Buffer newBuffer()
    {
        // Scan through the array to find one
        for ( int i = 0; i < buffers.length(); i++ )
        {
            char[] current = buffers.get( i );
            if ( current == UNALLOCATED || current != IN_USE )
            {
                // Mark that this buffer is currently being used
                buffers.set( i, IN_USE );
                return new Buffer( current == UNALLOCATED ? new char[chunkSize] : current, i );
            }
        }

        // With external push-back this shouldn't be an issue, but instead of introducing blocking
        // here just fall back to creating a new buffer which will not be eligible for reuse.
        return new Buffer( new char[chunkSize], -1 );
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }

    private static int offsetOfLastNewline( char[] buffer )
    {
        for ( int i = buffer.length-1; i >= 0; i-- )
        {
            if ( buffer[i] == '\n' )
            {
                return i;
            }
        }
        return -1;
    }

    private class ProcessingChunk implements Chunk
    {
        private final Buffer buffer;
        private final int length;
        private final String sourceDescription;

        public ProcessingChunk( Buffer buffer, int length, String sourceDescription )
        {
            this.buffer = buffer;
            this.length = length;
            this.sourceDescription = sourceDescription;
        }

        @Override
        public int startPosition()
        {
            return 0;
        }

        @Override
        public String sourceDescription()
        {
            return sourceDescription;
        }

        @Override
        public int maxFieldSize()
        {
            return chunkSize;
        }

        @Override
        public int length()
        {
            return length;
        }

        @Override
        public char[] data()
        {
            return buffer.data;
        }

        @Override
        public int backPosition()
        {
            return 0;
        }

        @Override
        public void close()
        {
            if ( buffer.reuseIndex != -1 )
            {
                // Give the buffer back to the source so that it can be reused
                buffers.set( buffer.reuseIndex, buffer.data );
            }
            // else this was a detached buffer which we cannot really put back into a reuse slot
        }
    }

    private static class Buffer
    {
        private final char[] data;
        private final int reuseIndex;

        Buffer( char[] data, int reuseIndex )
        {
            this.data = data;
            this.reuseIndex = reuseIndex;
        }
    }
}
