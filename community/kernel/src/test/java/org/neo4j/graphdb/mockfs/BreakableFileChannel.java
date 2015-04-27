/*
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

import org.neo4j.io.fs.StoreChannel;

public class BreakableFileChannel implements StoreChannel
{
    private final StoreChannel inner;
    private final File theFile;
    private FileSystemGuard fs;
    private int bytesWritten = 0;

    public BreakableFileChannel( StoreChannel open, File theFile, FileSystemGuard guard )
    {
        this.inner = open;
        this.theFile = theFile;
        this.fs = guard;
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
        return inner.read( dsts );
    }

    @Override
    public int write( ByteBuffer byteBuffer ) throws IOException
    {
        return inner.write( byteBuffer );
    }

    @Override
    public long write( ByteBuffer[] byteBuffers, int i, int i1 ) throws IOException
    {
        return inner.write( byteBuffers, i, i1 );
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        return inner.write( srcs );
    }

    @Override
    public void writeAll( ByteBuffer src, long position ) throws IOException
    {
        inner.writeAll( src, position );
    }

    @Override
    public void writeAll( ByteBuffer src ) throws IOException
    {
        inner.writeAll( src );
    }

    @Override
    public long position() throws IOException
    {
        return inner.position();
    }

    @Override
    public BreakableFileChannel position( long l ) throws IOException
    {
        inner.position( l );
        return this;
    }

    @Override
    public long size() throws IOException
    {
        return inner.size();
    }

    @Override
    public BreakableFileChannel truncate( long l ) throws IOException
    {
        inner.truncate( l );
        return this;
    }

    @Override
    public void force( boolean b ) throws IOException
    {
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
        int writtenInThisCall = 0;
        for ( int i = 0; i < byteBuffer.limit(); i++ )
        {
            fs.checkOperation( BreakableFileSystemAbstraction.OperationType.WRITE, theFile, bytesWritten, 1, inner.position() );

            ByteBuffer toWrite = byteBuffer.slice();
            toWrite.limit( 1 );
            inner.write( toWrite, l + writtenInThisCall );
            byteBuffer.get();
            bytesWritten++;
            writtenInThisCall++;
        }
        return writtenInThisCall;
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
