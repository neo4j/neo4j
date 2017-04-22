/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.csv.reader.Source.Chunk;

/**
 * In a scenario where there's one reader reading chunks of data, handing those chunks to one or
 * more processors (parsers) of that data, this class comes in handy. This pattern allows for
 * multiple {@link BufferedCharSeeker seeker instances}, each operating over one chunk, not transitioning itself
 * into the next.
 */
public class CharReadableChunker implements Closeable
{
    private final CharReadable reader;
    private final int chunkSize;
    private char[] backBuffer; // grows on demand
    private int backBufferCursor;
    private volatile long position;

    public CharReadableChunker( CharReadable reader, int chunkSize )
    {
        this.reader = reader;
        this.chunkSize = chunkSize;
        this.backBuffer = new char[chunkSize >> 4];
    }

    public ProcessingChunk newChunk()
    {
        return new ProcessingChunk( new char[chunkSize] );
    }

    /**
     * Fills the given chunk with data from the underlying {@link CharReadable}, up to a good cut-off point
     * in the vicinity of the buffer size.
     *
     * @param into {@link ProcessingChunk} to read data into.
     * @return the next {@link Chunk} of data, ending with a new-line or not for the last chunk.
     * @throws IOException on reading error.
     */
    public synchronized boolean nextChunk( ProcessingChunk into ) throws IOException
    {
        int offset = 0;

        if ( backBufferCursor > 0 )
        {   // Read from and reset back buffer
            assert backBufferCursor < chunkSize;
            System.arraycopy( backBuffer, 0, into.buffer, 0, backBufferCursor );
            offset += backBufferCursor;
            backBufferCursor = 0;
        }

        int leftToRead = chunkSize - offset;
        int read = reader.read( into.buffer, offset, leftToRead );
        if ( read == leftToRead )
        {   // Read from reader. We read data into the whole buffer and there seems to be more data left in reader.
            // This means we're most likely not at the end so seek backwards to the last newline character and
            // put the characters after the newline character(s) into the back buffer.
            int newlineOffset = offsetOfLastNewline( into.buffer );
            if ( newlineOffset > -1 )
            {   // We found a newline character some characters back
                backBufferCursor = chunkSize - (newlineOffset + 1);
                System.arraycopy( into.buffer, newlineOffset + 1, backBuffer( backBufferCursor ), 0, backBufferCursor );
                read -= backBufferCursor;
            }
            else
            {   // There was no newline character, isn't that weird?
                throw new IllegalStateException( "Weird input data, no newline character in the whole buffer " +
                        chunkSize + ", not supported a.t.m." );
            }
        }
        // else we couldn't completely fill the buffer, this means that we're at the end of a data source, we're good.

        if ( read > 0 )
        {
            offset += read;
            position += read;
            into.initialize( offset, reader.sourceDescription() );
            return true;
        }
        return false;
    }

    private char[] backBuffer( int length )
    {
        if ( length > backBuffer.length )
        {
            backBuffer = Arrays.copyOf( backBuffer, length );
        }
        return backBuffer;
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }

    public long position()
    {
        return position;
    }

    private static int offsetOfLastNewline( char[] buffer )
    {
        for ( int i = buffer.length - 1; i >= 0; i-- )
        {
            if ( buffer[i] == '\n' )
            {
                return i;
            }
        }
        return -1;
    }

    public static class ProcessingChunk implements Chunk
    {
        private final char[] buffer;
        private int length;
        private String sourceDescription;

        public ProcessingChunk( char[] buffer )
        {
            this.buffer = buffer;
        }

        void initialize( int length, String sourceDescription )
        {
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
            return buffer.length;
        }

        @Override
        public int length()
        {
            return length;
        }

        @Override
        public char[] data()
        {
            return buffer;
        }

        @Override
        public int backPosition()
        {
            return 0;
        }
    }
}
