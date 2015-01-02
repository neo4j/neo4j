/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Iterator;

import org.neo4j.collection.RawIterator;

/**
 * Have multiple {@link CharReadable} instances look like one. The provided {@link CharReadable readables} should
 * be opened lazily, in {@link Iterator#next()}, and will be closed in here, if they implement {@link Closeable}.
 */
public class MultiReadable implements CharReadable, Closeable
{
    private final RawIterator<CharReadable,IOException> actual;
    private CharReadable current = Readables.EMPTY;
    private int readFromCurrent;

    public MultiReadable( RawIterator<CharReadable,IOException> actual )
    {
        this.actual = actual;
    }

    @Override
    public int read( char[] buffer, int offset, int length ) throws IOException
    {
        int read = 0;
        while ( read < length )
        {
            int readThisTime = current.read( buffer, offset + read, length - read );
            if ( readThisTime == -1 )
            {
                if ( actual.hasNext() )
                {
                    closeCurrent();
                    current = actual.next();

                    // Even if there's no line-ending at the end of this source we should introduce one
                    // otherwise the last line of this source and the first line of the next source will
                    // look like one long line.
                    if ( readFromCurrent > 0 )
                    {
                        buffer[offset + read++] = '\n';
                        readFromCurrent = 0;
                    }
                }
                else
                {
                    break;
                }
            }
            else
            {
                read += readThisTime;
                readFromCurrent += readThisTime;
            }
        }
        return read == 0 ? -1 : read;
    }

    private void closeCurrent() throws IOException
    {
        current.close();
    }

    @Override
    public void close() throws IOException
    {
        closeCurrent();
    }
}
