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
package org.neo4j.graphdb.mockfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.neo4j.kernel.impl.nioneo.store.StoreChannel;

public class LimitedFileChannel implements StoreChannel
{

    private final StoreChannel inner;
    private LimitedFilesystemAbstraction fs;

    public LimitedFileChannel( StoreChannel inner, LimitedFilesystemAbstraction limitedFilesystemAbstraction )
    {
        this.inner = inner;
        fs = limitedFilesystemAbstraction;
    }

    @Override
    public int read( ByteBuffer byteBuffer ) throws IOException
    {
        return inner.read( byteBuffer );
    }

    @Override
    public long read( ByteBuffer[] byteBuffers, int i, int i1 ) throws IOException
    {
        return inner.read( byteBuffers, i, i1 );
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        return 0;
    }

    @Override
    public int write( ByteBuffer byteBuffer ) throws IOException
    {
        fs.ensureHasSpace();
        return inner.write( byteBuffer );
    }

    @Override
    public long write( ByteBuffer[] byteBuffers, int i, int i1 ) throws IOException
    {
        fs.ensureHasSpace();
        return inner.write( byteBuffers, i, i1 );
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        return 0;
    }

    @Override
    public long position() throws IOException
    {
        return inner.position();
    }

    @Override
    public LimitedFileChannel position( long l ) throws IOException
    {
        return new LimitedFileChannel( inner.position( l ), fs );
    }

    @Override
    public long size() throws IOException
    {
        return inner.size();
    }

    @Override
    public LimitedFileChannel truncate( long l ) throws IOException
    {
        return new LimitedFileChannel( inner.truncate( l ), fs );
    }

    @Override
    public void force( boolean b ) throws IOException
    {
        fs.ensureHasSpace();
        inner.force( b );
    }

    @Override
    public int read( ByteBuffer byteBuffer, long l ) throws IOException
    {
        return inner.read( byteBuffer, l );
    }

    @Override
    public FileLock tryLock() throws IOException
    {
        return inner.tryLock();
    }

    @Override
    public int write( ByteBuffer byteBuffer, long l ) throws IOException
    {
        return inner.write( byteBuffer, l );
    }

    @Override
    public MappedByteBuffer map( FileChannel.MapMode mapMode, long l, long l1 ) throws IOException
    {
        return inner.map( mapMode, l, l1 );
    }

    @Override
    public boolean isOpen()
    {
        return inner.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        inner.close();
    }
}
