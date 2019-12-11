/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.neo4j.io.pagecache.impl.SingleFilePageSwapper;

import static org.neo4j.util.FeatureToggles.flag;

public class StoreFileChannel implements StoreChannel
{
    private static final boolean PRINT_REFLECTION_EXCEPTIONS = flag( SingleFilePageSwapper.class, "printReflectionExceptions", false );
    private static final Class<?> CLS_FILE_CHANNEL_IMPL = getInternalFileChannelClass();
    private static final MethodHandle POSITION_LOCK_GETTER = getPositionLockGetter();
    private static final MethodHandle MAKE_CHANNEL_UNINTERRUPTIBLE = getUninterruptibleSetter();

    private static Class<?> getInternalFileChannelClass()
    {
        Class<?> cls = null;
        try
        {
            cls = Class.forName( "sun.nio.ch.FileChannelImpl" );
        }
        catch ( Throwable throwable )
        {
            if ( PRINT_REFLECTION_EXCEPTIONS )
            {
                throwable.printStackTrace();
            }
        }
        return cls;
    }

    private static MethodHandle getUninterruptibleSetter()
    {
        try
        {
            if ( CLS_FILE_CHANNEL_IMPL != null )
            {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                Method uninterruptibleSetter = CLS_FILE_CHANNEL_IMPL.getMethod( "setUninterruptible" );
                return lookup.unreflect( uninterruptibleSetter );
            }
            else
            {
                return null;
            }
        }
        catch ( Throwable e )
        {
            if ( PRINT_REFLECTION_EXCEPTIONS )
            {
                e.printStackTrace();
            }
            return null;
        }
    }

    private static MethodHandle getPositionLockGetter()
    {
        try
        {
            if ( CLS_FILE_CHANNEL_IMPL != null )
            {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                Field field = CLS_FILE_CHANNEL_IMPL.getDeclaredField( "positionLock" );
                field.setAccessible( true );
                return lookup.unreflectGetter( field );
            }
            else
            {
                return null;
            }
        }
        catch ( Throwable e )
        {
            if ( PRINT_REFLECTION_EXCEPTIONS )
            {
                e.printStackTrace();
            }
            return null;
        }
    }

    private final FileChannel channel;

    public StoreFileChannel( FileChannel channel )
    {
        this.channel = channel;
    }

    public StoreFileChannel( StoreFileChannel channel )
    {
        this.channel = channel.channel;
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        return channel.write( srcs );
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        return channel.write( srcs, offset, length );
    }

    @Override
    public void writeAll( ByteBuffer src, long position ) throws IOException
    {
        long filePosition = position;
        long expectedEndPosition = filePosition + src.limit() - src.position();
        int bytesWritten;
        while ( (filePosition += bytesWritten = channel.write( src, filePosition )) < expectedEndPosition )
        {
            if ( bytesWritten < 0 )
            {
                throw new IOException( "Unable to write to disk, reported bytes written was " + bytesWritten );
            }
        }
    }

    @Override
    public void writeAll( ByteBuffer src ) throws IOException
    {
        long bytesToWrite = src.limit() - src.position();
        int bytesWritten;
        while ( (bytesToWrite -= bytesWritten = write( src )) > 0 )
        {
            if ( bytesWritten < 0 )
            {
                throw new IOException( "Unable to write to disk, reported bytes written was " + bytesWritten );
            }
        }
    }

    @Override
    public StoreFileChannel truncate( long size ) throws IOException
    {
        channel.truncate( size );
        return this;
    }

    @Override
    public FileChannel fileChannel()
    {
        return channel;
    }

    @Override
    public boolean hasPositionLock()
    {
        return POSITION_LOCK_GETTER != null && channel.getClass() == CLS_FILE_CHANNEL_IMPL;
    }

    @Override
    public Object getPositionLock()
    {
        if ( POSITION_LOCK_GETTER == null )
        {
            return null;
        }
        try
        {
            return (Object) POSITION_LOCK_GETTER.invoke( channel );
        }
        catch ( Throwable th )
        {
            throw new LinkageError( "Cannot get FileChannel.positionLock", th );
        }
    }

    @Override
    public void tryMakeUninterruptible()
    {
        if ( MAKE_CHANNEL_UNINTERRUPTIBLE != null && channel.getClass() == CLS_FILE_CHANNEL_IMPL )
        {
            try
            {
                MAKE_CHANNEL_UNINTERRUPTIBLE.invoke( channel );
            }
            catch ( Throwable t )
            {
                throw new LinkageError( "No setter for uninterruptible flag", t );
            }
        }
    }

    @Override
    public StoreFileChannel position( long newPosition ) throws IOException
    {
        channel.position( newPosition );
        return this;
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        return channel.read( dst, position );
    }

    @Override
    public void readAll( ByteBuffer dst ) throws IOException
    {
        while ( dst.hasRemaining() )
        {
            int bytesRead = channel.read( dst );
            if ( bytesRead < 0 )
            {
                throw new IllegalStateException( "Channel has reached end-of-stream." );
            }
        }
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        channel.force( metaData );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        return channel.read( dst );
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        return channel.read( dsts, offset, length );
    }

    @Override
    public long position() throws IOException
    {
        return channel.position();
    }

    @Override
    public FileLock tryLock() throws IOException
    {
        return channel.tryLock();
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        return channel.read( dsts );
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        return channel.write( src );
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @Override
    public long size() throws IOException
    {
        return channel.size();
    }

    @Override
    public void flush() throws IOException
    {
        force( false );
    }
}
