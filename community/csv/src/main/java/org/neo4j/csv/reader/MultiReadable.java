/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.io.Reader;
import java.util.Iterator;

import org.neo4j.collection.RawIterator;

/**
 * Have multiple {@link CharReadable} instances look like one. The provided {@link CharReadable readables} should
 * be opened lazily, in {@link Iterator#next()}, and will be closed in here, if they implement {@link Closeable}.
 */
public class MultiReadable extends CharReadable.Adapter implements Closeable
{
    private final RawIterator<Reader,IOException> actual;
    private Reader current;
    private boolean requiresNewLine;
    private long position;
    private String currentSourceDescription = Readables.EMPTY.sourceDescription();

    public MultiReadable( RawIterator<Reader,IOException> actual ) throws IOException
    {
        this.actual = actual;
        goToNextSource();
    }

    private boolean goToNextSource() throws IOException
    {
        if ( actual.hasNext() )
        {
            closeCurrent();
            current = actual.next();
            currentSourceDescription = current.toString();
            return true;
        }
        return false;
    }

    @Override
    public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
    {
        buffer.compact( buffer, from );
        while ( current != null )
        {
            buffer.readFrom( current );
            if ( buffer.hasAvailable() )
            {
                position += buffer.available();
                char lastReadChar = buffer.array()[buffer.front()-1];
                requiresNewLine = lastReadChar != '\n' && lastReadChar != '\r';
                return buffer;
            }

            // Even if there's no line-ending at the end of this source we should introduce one
            // otherwise the last line of this source and the first line of the next source will
            // look like one long line.
            if ( requiresNewLine )
            {
                buffer.append( '\n' );
                position++;
                requiresNewLine = false;
                return buffer;
            }

            if ( !goToNextSource() )
            {
                break;
            }
        }
        return buffer;
    }

    private void closeCurrent() throws IOException
    {
        if ( current != null )
        {
            current.close();
        }
    }

    @Override
    public void close() throws IOException
    {
        closeCurrent();
    }

    @Override
    public long position()
    {
        return position;
    }

    @Override
    public String sourceDescription()
    {
        return currentSourceDescription;
    }
}
