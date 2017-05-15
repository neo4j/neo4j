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

import java.io.IOException;

import org.neo4j.csv.reader.Source.Chunk;

public class MultiLineAwareChunker extends CharReadableChunker
{
    private final int fieldsPerEntry;
    private final SectionedCharBuffer charBuffer;
    private final BufferedCharSeeker seeker;
    private final Mark mark = new Mark();
    private final char delimiter;

    public MultiLineAwareChunker( CharReadable reader, Configuration config, int fieldsPerEntry, char delimiter )
    {
        super( reader, config.bufferSize() );
        this.fieldsPerEntry = fieldsPerEntry;
        this.delimiter = delimiter;
        this.charBuffer = new SectionedCharBuffer( config.bufferSize() );
        this.seeker = new BufferedCharSeeker( new AutoReadingSource( reader, charBuffer ), config );
    }

    @Override
    public synchronized boolean nextChunk( Chunk chunk ) throws IOException
    {
        // Parse the stream, entry per entry. Entry == "one logical line of data" which may span
        // multiple physical lines, which makes it hard to predict if just jumping into a position
        // in the file. Wouldn't it be nice with a specific character for delimiter between entries,
        // instead of using NEWLINE?

        ChunkImpl into = (ChunkImpl) chunk;
        char[] array = into.data();
        int prevPosition = charBuffer.back();
        boolean doContinue = true;
        int cursor = fillFromBackBuffer( array );
        int prevEntryStart;
        while ( doContinue )
        {
            prevEntryStart = cursor;

            // Read one entry worth of data
            for ( int i = 0; i < fieldsPerEntry; i++ )
            {
                if ( !seeker.seek( mark, delimiter ) )
                {
                    // this seems to be the end of the stream
                    doContinue = false;
                    break;
                }

                // And copy into the chunk array.
                // TODO would be nice to not require copy here though.
                int position = mark.position();
                if ( position < prevPosition )
                {
                    // This means that the reader have fetched a new buffer internally
                    prevPosition = charBuffer.back();
                }
                int length = position - prevPosition;
                if ( cursor + length >= array.length )
                {
                    // We can't fit this value in the array. Store the entries which have been read so far
                    // in the back buffer and also finally this value
                    storeInBackBuffer( array, prevEntryStart, cursor - prevEntryStart );
                    storeInBackBuffer( charBuffer.array(), prevPosition, length );
                    doContinue = false;
                    cursor = prevEntryStart;
                    break;
                }

                System.arraycopy( charBuffer.array(), prevPosition, array, cursor, length );
                cursor += length;
                prevPosition = position;
            }
        }
        if ( cursor > 0 )
        {
            into.initialize( cursor, reader.sourceDescription() );
            return true;
        }
        return false;
    }
}
