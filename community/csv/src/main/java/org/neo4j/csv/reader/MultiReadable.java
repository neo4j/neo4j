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
package org.neo4j.csv.reader;

import java.io.Closeable;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Iterator;

/**
 * Have multiple {@link Readable} instances look like one. The provided {@link Readable readables} should
 * be opened lazily, in {@link Iterator#next()}, and will be closed in here, if they implement {@link Closeable}.
 */
public class MultiReadable implements Readable, Closeable
{
    private final Iterator<Readable> actual;
    private Readable current = Readables.EMPTY;
    private int readFromCurrent;

    public MultiReadable( Iterator<Readable> actual )
    {
        this.actual = actual;
    }

    @Override
    public int read( CharBuffer cb ) throws IOException
    {
        int read = 0;
        while ( cb.hasRemaining() )
        {
            int readThisTime = current.read( cb );
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
                        cb.put( '\n' );
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
        if ( current instanceof Closeable )
        {
            ((Closeable) current).close();
        }
    }

    @Override
    public void close() throws IOException
    {
        closeCurrent();
    }
}
