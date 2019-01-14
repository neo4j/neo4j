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
import java.io.Reader;

/**
 * Wraps a {@link Reader} into a {@link CharReadable}.
 */
class WrappedCharReadable extends CharReadable.Adapter
{
    private final long length;
    private final Reader reader;
    private long position;
    private final String sourceDescription;

    WrappedCharReadable( long length, Reader reader )
    {
        this.length = length;
        this.reader = reader;
        sourceDescription = reader.toString();
    }

    @Override
    public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
    {
        buffer.compact( buffer, from );
        buffer.readFrom( reader );
        position += buffer.available();
        return buffer;
    }

    @Override
    public int read( char[] into, int offset, int length ) throws IOException
    {
        int totalRead = 0;
        boolean eof = false;
        while ( totalRead < length )
        {
            int read = reader.read( into, offset + totalRead, length - totalRead );
            if ( read == -1 )
            {
                eof = true;
                break;
            }
            totalRead += read;
        }
        position += totalRead;
        return totalRead == 0 && eof ? -1 : totalRead;
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }

    @Override
    public long position()
    {
        return position;
    }

    @Override
    public String sourceDescription()
    {
        return sourceDescription;
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public String toString()
    {
        return sourceDescription;
    }
}
