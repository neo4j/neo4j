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
import java.io.Writer;

import org.neo4j.adversaries.Adversary;

@SuppressWarnings( "unchecked" )
public class AdversarialWriter extends Writer
{
    private final Writer writer;
    private final Adversary adversary;

    public AdversarialWriter( Writer writer, Adversary adversary )
    {
        this.writer = writer;
        this.adversary = adversary;
    }

    @Override
    public void write( int c ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        writer.write( c );
    }

    @Override
    public void write( char[] cbuf ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        writer.write( cbuf );
    }

    @Override
    public void write( char[] cbuf, int off, int len ) throws IOException
    {
        writer.write( cbuf, off, len );
    }

    @Override
    public void write( String str ) throws IOException
    {
        adversary.injectFailure(
                StringIndexOutOfBoundsException.class, IOException.class,
                IndexOutOfBoundsException.class, ArrayStoreException.class,
                NullPointerException.class );
        writer.write( str );
    }

    @Override
    public void write( String str, int off, int len ) throws IOException
    {
        adversary.injectFailure(
                StringIndexOutOfBoundsException.class, IOException.class,
                IndexOutOfBoundsException.class, ArrayStoreException.class,
                NullPointerException.class );
        writer.write( str, off, len );
    }

    @Override
    public Writer append( CharSequence csq ) throws IOException
    {
        adversary.injectFailure(
                StringIndexOutOfBoundsException.class, IOException.class,
                IndexOutOfBoundsException.class, ArrayStoreException.class,
                NullPointerException.class );
        return writer.append( csq );
    }

    @Override
    public Writer append( CharSequence csq, int start, int end ) throws IOException
    {
        adversary.injectFailure(
                StringIndexOutOfBoundsException.class, IOException.class,
                IndexOutOfBoundsException.class, ArrayStoreException.class,
                NullPointerException.class );
        return writer.append( csq, start, end );
    }

    @Override
    public Writer append( char c ) throws IOException
    {
        adversary.injectFailure( IOException.class );
        return writer.append( c );
    }

    @Override
    public void flush() throws IOException
    {
        adversary.injectFailure( IOException.class );
        writer.flush();
    }

    @Override
    public void close() throws IOException
    {
        adversary.injectFailure( IOException.class );
        writer.close();
    }
}
