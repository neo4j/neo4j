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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Iterator;

/**
 * Means of instantiating common {@link Readable} instances.
 */
public class Readables
{
    private Readables()
    {
        throw new AssertionError( "No instances allowed" );
    }

    public static final Readable EMPTY = new Readable()
    {
        @Override
        public int read( CharBuffer cb ) throws IOException
        {
            return -1;
        }
    };

    private interface Function<IN,OUT>
    {
        OUT apply( IN in );
    }

    private static final Function<File,Readable> FROM_FILE = new Function<File,Readable>()
    {
        @Override
        public Readable apply( File in )
        {
            try
            {
                return new FileReader( in );
            }
            catch ( FileNotFoundException e )
            {
                throw new RuntimeException( e );
            }
        }
    };

    private static final Function<Readable,Readable> IDENTITY = new Function<Readable,Readable>()
    {
        @Override
        public Readable apply( Readable in )
        {
            return in;
        }
    };

    public static Readable file( File file ) throws FileNotFoundException
    {
        return new FileReader( file );
    }

    public static Readable multipleFiles( File... files )
    {
        return new MultiReadable( iterator( files, FROM_FILE ) );
    }

    public static Readable multipleSources( Readable... sources )
    {
        return new MultiReadable( iterator( sources, IDENTITY ) );
    }

    public static Readable multipleFiles( Iterator<File> files )
    {
        return new MultiReadable( iterator( files, FROM_FILE ) );
    }

    public static Readable multipleSources( Iterator<Readable> sources )
    {
        return new MultiReadable( sources );
    }

    private static <IN,OUT> Iterator<OUT> iterator( final Iterator<IN> items, final Function<IN,OUT> converter )
    {
        return new Iterator<OUT>()
        {
            @Override
            public boolean hasNext()
            {
                return items.hasNext();
            }

            @Override
            public OUT next()
            {
                return converter.apply( items.next() );
            }

            @Override
            public void remove()
            {
                items.remove();
            }
        };
    }

    private static <IN,OUT> Iterator<OUT> iterator( final IN[] items, final Function<IN,OUT> converter )
    {
        return new Iterator<OUT>()
        {
            private int cursor;

            @Override
            public boolean hasNext()
            {
                return cursor < items.length;
            }

            @Override
            public OUT next()
            {
                if ( !hasNext() )
                {
                    throw new IllegalStateException();
                }
                return converter.apply( items[cursor++] );
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
