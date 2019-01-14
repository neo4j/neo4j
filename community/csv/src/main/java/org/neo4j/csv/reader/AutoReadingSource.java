/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

/**
 * In a scenario where there's one thread, or perhaps a {@link ThreadAheadReadable} doing both the
 * reading and parsing one {@link BufferedCharSeeker} is used over a stream of chunks, where the next
 * chunk seamlessly transitions into the next, this class comes in handy. It uses a {@link CharReadable}
 * and {@link SectionedCharBuffer} to do this.
 */
public class AutoReadingSource implements Source
{
    private final CharReadable reader;
    private SectionedCharBuffer charBuffer;

    public AutoReadingSource( CharReadable reader, int bufferSize )
    {
        this( reader, new SectionedCharBuffer( bufferSize ) );
    }

    public AutoReadingSource( CharReadable reader, SectionedCharBuffer charBuffer )
    {
        this.reader = reader;
        this.charBuffer = charBuffer;
    }

    @Override
    public Chunk nextChunk( int seekStartPos ) throws IOException
    {
        charBuffer = reader.read( charBuffer, seekStartPos == -1 ? charBuffer.pivot() : seekStartPos );

        return new Chunk()
        {
            @Override
            public int startPosition()
            {
                return charBuffer.pivot();
            }

            @Override
            public String sourceDescription()
            {
                return reader.sourceDescription();
            }

            @Override
            public int backPosition()
            {
                return charBuffer.back();
            }

            @Override
            public int length()
            {
                return charBuffer.available();
            }

            @Override
            public int maxFieldSize()
            {
                return charBuffer.pivot();
            }

            @Override
            public char[] data()
            {
                return charBuffer.array();
            }
        };
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }
}
