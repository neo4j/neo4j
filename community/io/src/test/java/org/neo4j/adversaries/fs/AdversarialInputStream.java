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
import java.io.InputStream;

import org.neo4j.adversaries.Adversary;

@SuppressWarnings( "unchecked" )
public class AdversarialInputStream extends InputStream
{
    private final InputStream inputStream;
    private final Adversary adversary;

    public AdversarialInputStream( InputStream inputStream, Adversary adversary )
    {
        this.inputStream = inputStream;
        this.adversary = adversary;
    }

    @Override
    public int read() throws IOException
    {
        adversary.injectFailure( IOException.class );
        return inputStream.read();
    }

    @Override
    public int read( byte[] b ) throws IOException
    {
        if ( adversary.injectFailureOrMischief( IOException.class, NullPointerException.class ) )
        {
            byte[] dup = new byte[Math.max( b.length / 2, 1 )];
            int read = inputStream.read( dup );
            System.arraycopy( dup, 0, b, 0, read );
            return read;
        }
        return inputStream.read( b );
    }

    @Override
    public int read( byte[] b, int off, int len ) throws IOException
    {
        if ( adversary.injectFailureOrMischief(
                IOException.class, NullPointerException.class, IndexOutOfBoundsException.class ) )
        {
            int halflen = Math.max( len / 2, 1 );
            return inputStream.read( b, off, halflen );
        }
        return inputStream.read( b, off, len );
    }

    @Override
    public long skip( long n ) throws IOException
    {
        adversary.injectFailure( IOException.class, NullPointerException.class, IndexOutOfBoundsException.class );
        return inputStream.skip( n );
    }

    @Override
    public int available() throws IOException
    {
        adversary.injectFailure( IOException.class );
        return inputStream.available();
    }

    @Override
    public void close() throws IOException
    {
        adversary.injectFailure( IOException.class );
        inputStream.close();
    }

    @Override
    public void mark( int readlimit )
    {
        adversary.injectFailure();
        inputStream.mark( readlimit );
    }

    @Override
    public void reset() throws IOException
    {
        adversary.injectFailure( IOException.class );
        inputStream.reset();
    }

    @Override
    public boolean markSupported()
    {
        adversary.injectFailure();
        return inputStream.markSupported();
    }
}
