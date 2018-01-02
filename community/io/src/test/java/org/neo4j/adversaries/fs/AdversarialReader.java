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
package org.neo4j.adversaries.fs;

import java.io.IOException;
import java.io.Reader;
import java.nio.BufferOverflowException;
import java.nio.CharBuffer;

import org.neo4j.adversaries.Adversary;

@SuppressWarnings( "unchecked" )
public class AdversarialReader extends Reader
{
    private final Reader reader;
    private final Adversary adversary;

    public AdversarialReader( Reader reader, Adversary adversary )
    {
        this.reader = reader;
        this.adversary = adversary;
    }

    @Override
    public int read( CharBuffer target ) throws IOException
    {
        if ( adversary.injectFailureOrMischief(
                IOException.class, BufferOverflowException.class, IndexOutOfBoundsException.class ) )
        {
            CharBuffer dup = target.duplicate();
            dup.limit( Math.max( target.limit() / 2, 1 ) );
            return reader.read( dup );
        }
        return reader.read( target );
    }

    @Override
    public int read() throws IOException
    {
        adversary.injectFailure( IOException.class );
        return reader.read();
    }

    @Override
    public int read( char[] cbuf ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            char[] dup = new char[ Math.max( cbuf.length / 2, 1 ) ];
            int read = reader.read( dup );
            System.arraycopy( dup, 0, cbuf, 0, read );
            return read;
        }
        return reader.read( cbuf );
    }

    @Override
    public int read( char[] cbuf, int off, int len ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class ) )
        {
            return reader.read( cbuf, off, Math.max( len / 2, 1 ) );
        }
        return reader.read( cbuf, off, len );
    }

    @Override
    public long skip( long n ) throws IOException
    {
        adversary.injectFailure( IllegalArgumentException.class, IOException.class );
        return reader.skip( n );
    }

    @Override
    public boolean ready() throws IOException
    {
        adversary.injectFailure( IOException.class );
        return reader.ready();
    }

    @Override
    public boolean markSupported()
    {
        adversary.injectFailure();
        return reader.markSupported();
    }

    @Override
    public void mark( int readAheadLimit ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        reader.mark( readAheadLimit );
    }

    @Override
    public void reset() throws IOException
    {
        adversary.injectFailure( IOException.class );
        reader.reset();
    }

    @Override
    public void close() throws IOException
    {
        adversary.injectFailure( IOException.class );
        reader.close();
    }
}
