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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.neo4j.io.fs.StoreChannel;

public class PhysicalLogVersionedStoreChannel implements VersionedStoreChannel
{
    private final StoreChannel delegateChannel;
    private long version;

    public PhysicalLogVersionedStoreChannel( StoreChannel delegateChannel, long version )
    {
        this.delegateChannel = delegateChannel;
        this.version = version;
    }

    public PhysicalLogVersionedStoreChannel( StoreChannel delegateChannel )
    {
        this( delegateChannel, -1 );
    }

    public void setVersion( long version )
    {
        this.version = version;
    }

    @Override
    public FileLock tryLock() throws IOException
    {
        return delegateChannel.tryLock();
    }

    @Override
    public int write( ByteBuffer src, long position ) throws IOException
    {
        return delegateChannel.write( src, position );
    }

    @Override
    public void writeAll( ByteBuffer src, long position ) throws IOException
    {
        delegateChannel.writeAll( src, position );
    }

    @Override
    public void writeAll( ByteBuffer src ) throws IOException
    {
        delegateChannel.writeAll( src );
    }

    @Override
    public MappedByteBuffer map( FileChannel.MapMode mode, long position, long size ) throws IOException
    {
        return delegateChannel.map( mode, position, size );
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        return delegateChannel.read( dst, position );
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        delegateChannel.force( metaData );
    }

    @Override
    public StoreChannel position( long newPosition ) throws IOException
    {
        return delegateChannel.position( newPosition );
    }

    @Override
    public StoreChannel truncate( long size ) throws IOException
    {
        return delegateChannel.truncate( size );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        return delegateChannel.read( dst );
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        return delegateChannel.write( src );
    }

    @Override
    public long position() throws IOException
    {
        return delegateChannel.position();
    }

    @Override
    public long size() throws IOException
    {
        return delegateChannel.size();
    }

    @Override
    public boolean isOpen()
    {
        return delegateChannel.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        delegateChannel.close();
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        return delegateChannel.write( srcs, offset, length );
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        return delegateChannel.write( srcs );
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        return delegateChannel.read( dsts, offset, length );
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        return delegateChannel.read( dsts );
    }

    @Override
    public long getVersion()
    {
        return version;
    }

    @Override
    public void getCurrentPosition( LogPositionMarker positionMarker ) throws IOException
    {
        positionMarker.mark( version, position() );
    }
}
