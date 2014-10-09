/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input.csv.reader;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;

import static org.neo4j.helpers.Format.MB;

/**
 * Much like a {@link BufferedReader} for a {@link Reader}.
 */
public class BufferedCharSeeker implements CharSeeker
{
    public static final int DEFAULT_BUFFER_SIZE = 2 * MB;

    private static final char EOL_CHAR = '\n';
    private static final char EOL_CHAR_2 = '\r';
    private static final char EOF_CHAR = (char) -1;

    private final Readable reader;
    private final char[] buffer;
    private final CharBuffer charBuffer;
    private int bufferPos;
    private long bufferStartPos;
    private int seekStartPos;
    private int lineNumber = 1;

    public BufferedCharSeeker( Readable reader )
    {
        this( reader, DEFAULT_BUFFER_SIZE );
    }

    public BufferedCharSeeker( Readable reader, int bufferSize )
    {
        this.reader = reader;
        this.buffer = new char[bufferSize];
        this.bufferStartPos = -bufferSize;
        this.charBuffer = CharBuffer.wrap( buffer );
        this.bufferPos = bufferSize;
    }

    @Override
    public boolean seek( Mark mark, int[] untilOneOfChars ) throws IOException
    {
        // Keep a start position in case we need to further fill the buffer in nextChar, a value can at maximum be the
        // whole buffer, so max one fill per value is supported.
        seekStartPos = bufferPos; // seekStartPos updated in nextChar if buffer flips over, that's why it's a member
        int ch;
        while ( (ch = nextChar()) != EOL_CHAR && ch != EOL_CHAR_2 && ch != EOF_CHAR )
        {
            // Found a delimiter?
            for ( int i = 0; i < untilOneOfChars.length; i++ )
            {
                if ( ch == untilOneOfChars[i] )
                {
                    // Yes, set marker and return true
                    mark.set( lineNumber, seekStartPos, bufferPos - 1, ch );
                    return true;
                }
            }
        }

        try
        {
            if ( bufferPos - seekStartPos == 1 )
            {
                // We didn't find any of the characters sought for
                mark.set( lineNumber, -1, -1, Mark.END_OF_LINE_CHARACTER );
                return false;
            }
            // We found the last value of the line or stream
            int skipped = skipEolChars();
            mark.set( lineNumber, seekStartPos, bufferPos - 1 - skipped, Mark.END_OF_LINE_CHARACTER );
            lineNumber++;
            return true;
        }
        finally
        {
            if ( ch == EOF_CHAR )
            {
                bufferPos--; // so that we see it again next time
            }
        }
    }

    @Override
    public <T> T extract( Mark mark, Extractor<T> extractor )
    {
        long from = mark.startPosition();
        long to = mark.position();
        return extractor.extract( buffer, (int)(from), (int)(to-from) );
    }

    private int skipEolChars() throws IOException
    {
        int ch;
        int skipped = 0;
        while ( (ch = nextChar()) == EOL_CHAR || ch == EOL_CHAR_2 )
        {   // Just loop through, skipping them
            skipped++;
        }
        bufferPos--; // since nextChar advances one step
        return skipped;
    }

    private int nextChar() throws IOException
    {
        fillBufferIfWeHaveExhaustedIt();
        return buffer[bufferPos++];
    }

    private void fillBufferIfWeHaveExhaustedIt() throws IOException
    {
        if ( bufferPos >= buffer.length )
        {
            if ( seekStartPos == 0 )
            {
                throw new IllegalStateException( "Tried to read in a value larger than buffer size " + charBuffer.capacity() );
            }
            charBuffer.position( seekStartPos );
            charBuffer.compact();
            int position = charBuffer.position();
            reader.read( charBuffer );
            if ( charBuffer.hasRemaining() )
            {
                charBuffer.put( EOF_CHAR );
            }
            bufferPos = position;
            bufferStartPos += seekStartPos;
            seekStartPos = 0;
        }
    }

    @Override
    public void close() throws IOException
    {
        // Check instanceof since we use the more generic Readable interface instead of Reader directly
        // and we don't want to create an unnecessary indirection
        if ( reader instanceof Closeable )
        {
            ((Closeable) reader).close();
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[readPos:" + bufferStartPos + ", buffer:" + charBuffer +
                ", seekPos:" + seekStartPos + ", line:" + lineNumber + "]";
    }
}
