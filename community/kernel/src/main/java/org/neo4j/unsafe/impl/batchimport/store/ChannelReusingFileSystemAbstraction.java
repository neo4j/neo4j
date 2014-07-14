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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.Function;
import org.neo4j.io.fs.FileLock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Wraps another {@link FileSystemAbstraction} and keeps opened {@link StoreChannel} opened even after
 * {@link StoreChannel#close()} has been called, so that other consumers can reuse those channels.
 *
 * Implements {@link Lifecycle} where the channels will be finally closed on {@link Lifecycle#shutdown() shutdown}.
 */
public class ChannelReusingFileSystemAbstraction extends LifecycleAdapter implements FileSystemAbstraction
{
    private final FileSystemAbstraction delegate;
    private final Map<File, KeepAliveStoreChannel> openChannels = new HashMap<>();

    public ChannelReusingFileSystemAbstraction( FileSystemAbstraction delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public StoreChannel open( File fileName, String mode ) throws IOException
    {
        KeepAliveStoreChannel channel = openChannels.get( fileName );
        if ( channel == null )
        {
            openChannels.put( fileName, channel = new KeepAliveStoreChannel( delegate.open( fileName, mode ) ) );
        }
        return channel;
    }

    @Override
    public void shutdown() throws Throwable
    {
        for ( KeepAliveStoreChannel channel : openChannels.values() )
        {
            channel.closeForReal();
        }
        openChannels.clear();
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        throw new UnsupportedOperationException( "Just checking if this is used" );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        throw new UnsupportedOperationException( "Just checking if this is used" );
    }

    @Override
    public Reader openAsReader( File fileName, String encoding ) throws IOException
    {
        throw new UnsupportedOperationException( "Just checking if this is used" );
    }

    @Override
    public Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException
    {
        throw new UnsupportedOperationException( "Just checking if this is used" );
    }

    @Override
    public FileLock tryLock( File fileName, StoreChannel channel ) throws IOException
    {
        return delegate.tryLock( fileName, channel );
    }

    @Override
    public StoreChannel create( File fileName ) throws IOException
    {
        return delegate.create( fileName );
    }

    @Override
    public boolean fileExists( File fileName )
    {
        return delegate.fileExists( fileName );
    }

    @Override
    public boolean mkdir( File fileName )
    {
        return delegate.mkdir( fileName );
    }

    @Override
    public void mkdirs( File fileName ) throws IOException
    {
        delegate.mkdirs( fileName );
    }

    @Override
    public long getFileSize( File fileName )
    {
        return delegate.getFileSize( fileName );
    }

    @Override
    public boolean deleteFile( File fileName )
    {
        return delegate.deleteFile( fileName );
    }

    @Override
    public void deleteRecursively( File directory ) throws IOException
    {
        delegate.deleteRecursively( directory );
    }

    @Override
    public boolean renameFile( File from, File to ) throws IOException
    {
        return delegate.renameFile( from, to );
    }

    @Override
    public File[] listFiles( File directory )
    {
        return delegate.listFiles( directory );
    }

    @Override
    public File[] listFiles( File directory, FilenameFilter filter )
    {
        return delegate.listFiles( directory, filter );
    }

    @Override
    public boolean isDirectory( File file )
    {
        return delegate.isDirectory( file );
    }

    @Override
    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        delegate.moveToDirectory( file, toDirectory );
    }

    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        delegate.copyFile( from, to );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        delegate.copyRecursively( fromDirectory, toDirectory );
    }

    @Override
    public <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem( Class<K> clazz,
            Function<Class<K>, K> creator )
    {
        return delegate.getOrCreateThirdPartyFileSystem( clazz, creator );
    }

    public static class KeepAliveStoreChannel implements StoreChannel
    {
        private final StoreChannel delegate;

        public KeepAliveStoreChannel( StoreChannel delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
        {
            return delegate.read( dsts, offset, length );
        }

        @Override
        public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
        {
            return delegate.write( srcs, offset, length );
        }

        @Override
        public java.nio.channels.FileLock tryLock() throws IOException
        {
            return delegate.tryLock();
        }

        @Override
        public boolean isOpen()
        {
            return delegate.isOpen();
        }

        @Override
        public void close() throws IOException
        {   // Don't actually close it here
        }

        private void closeForReal() throws IOException
        {
            delegate.close();
        }

        @Override
        public int read( ByteBuffer dst ) throws IOException
        {
            return delegate.read( dst );
        }

        @Override
        public int write( ByteBuffer src, long position ) throws IOException
        {
            return delegate.write( src, position );
        }

        @Override
        public void writeAll( ByteBuffer src, long position ) throws IOException
        {
            write( src, position );
        }

        @Override
        public void writeAll( ByteBuffer src ) throws IOException
        {
            write( src );
        }

        @Override
        public MappedByteBuffer map( MapMode mode, long position, long size ) throws IOException
        {
            return delegate.map( mode, position, size );
        }

        @Override
        public int read( ByteBuffer dst, long position ) throws IOException
        {
            return delegate.read( dst, position );
        }

        @Override
        public void force( boolean metaData ) throws IOException
        {
            // In the assumed environment, there's no need for forcing a channel, other than when it's automatically
            // done so, at the time of closing the channel.
        }

        @Override
        public StoreChannel position( long newPosition ) throws IOException
        {
            return delegate.position( newPosition );
        }

        @Override
        public int write( ByteBuffer src ) throws IOException
        {
            return delegate.write( src );
        }

        @Override
        public StoreChannel truncate( long size ) throws IOException
        {
            return delegate.truncate( size );
        }

        @Override
        public long position() throws IOException
        {
            return delegate.position();
        }

        @Override
        public long read( ByteBuffer[] dsts ) throws IOException
        {
            return delegate.read( dsts );
        }

        @Override
        public long size() throws IOException
        {
            return delegate.size();
        }

        @Override
        public long write( ByteBuffer[] srcs ) throws IOException
        {
            return delegate.write( srcs );
        }
    }
}
