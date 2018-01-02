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
package org.neo4j.io.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

public class AbstractStoreChannel implements StoreChannel
{
    @Override
    public FileLock tryLock() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write( ByteBuffer src, long position ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeAll( ByteBuffer src, long position ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeAll( ByteBuffer src ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoreChannel position( long newPosition ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoreChannel truncate( long size ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() throws IOException
    {
        force( false );
    }
}
