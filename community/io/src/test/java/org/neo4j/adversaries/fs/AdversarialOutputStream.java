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
import java.io.OutputStream;

import org.neo4j.adversaries.Adversary;

@SuppressWarnings( "unchecked" )
public class AdversarialOutputStream extends OutputStream
{
    private final OutputStream outputStream;
    private final Adversary adversary;

    public AdversarialOutputStream( OutputStream outputStream, Adversary adversary )
    {
        this.outputStream = outputStream;
        this.adversary = adversary;
    }

    @Override
    public void write( int b ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        outputStream.write( b );
    }

    @Override
    public void write( byte[] b ) throws IOException
    {
        adversary.injectFailure( NullPointerException.class, IndexOutOfBoundsException.class, IOException.class );
        outputStream.write( b );
    }

    @Override
    public void write( byte[] b, int off, int len ) throws IOException
    {
        adversary.injectFailure( NullPointerException.class, IndexOutOfBoundsException.class, IOException.class );
        outputStream.write( b, off, len );
    }

    @Override
    public void flush() throws IOException
    {
        adversary.injectFailure( IOException.class );
        outputStream.flush();
    }

    @Override
    public void close() throws IOException
    {
        adversary.injectFailure( IOException.class );
        outputStream.close();
    }
}
