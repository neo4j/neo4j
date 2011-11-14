/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

public class EphemeralFileSystemAbstraction implements FileSystemAbstraction
{
    private static final Queue<ByteBuffer> memoryPool = new ConcurrentLinkedQueue<ByteBuffer>();
    private final Map<String, EphemeralFileChannel> files = new HashMap<String, EphemeralFileChannel>();
    
    public void dispose()
    {
        for ( EphemeralFileChannel file : files.values() ) free( file );
    }
    
    @Override
    public synchronized FileChannel open( String fileName, String mode ) throws IOException
    {
        EphemeralFileChannel file = files.get( fileName );
        return file != null ? file : create( fileName );
    }

    @Override
    public FileLock tryLock( String fileName, FileChannel channel ) throws IOException
    {
        return FileLock.getOsSpecificFileLock( fileName, channel );
    }
    
    @Override
    public synchronized FileChannel create( String fileName ) throws IOException
    {
        EphemeralFileChannel file = new EphemeralFileChannel();
        free( files.put( fileName, file ) );
        return file;
    }
    
    private void free( EphemeralFileChannel fileChannel )
    {
        if ( fileChannel != null ) freeBuffer( fileChannel.fileAsBuffer );
    }

    @Override
    public long getFileSize( String fileName )
    {
        EphemeralFileChannel file = files.get( fileName );
        return file == null ? 0 : file.size();
    }
    
    @Override
    public boolean fileExists( String fileName )
    {
        return files.containsKey( fileName );
    }
    
    @Override
    public boolean deleteFile( String fileName )
    {
        files.remove( fileName );
        return true;
    }
    
    @Override
    public boolean renameFile( String from, String to ) throws IOException
    {
        EphemeralFileChannel file = files.remove( from );
        if ( file == null ) throw new IOException( "'" + from + "' doesn't exist" );
        if ( files.containsKey( to ) ) throw new IOException( "'" + to + "' already exists" );
        files.put( to, file );
        return true;
    }

    private static ByteBuffer allocateBuffer()
    {
        ByteBuffer buffer = memoryPool.poll();
        if ( buffer != null ) return buffer;
        // TODO dynamic size
        return ByteBuffer.allocateDirect( 50 * 1024 * 1024 );
    }

    private static void freeBuffer( ByteBuffer buffer )
    {
        buffer.clear();
        memoryPool.add( buffer );
    }
    
    private static class EphemeralFileChannel extends FileChannel
    {
        private final ByteBuffer fileAsBuffer = allocateBuffer();
        private final byte[] scratchPad = new byte[1024];
        private int size;
        private int locked;
        
        @Override
        public int read( ByteBuffer dst ) throws IOException
        {
            return transfer( fileAsBuffer, dst );
        }
        
        private int transfer( ByteBuffer from, ByteBuffer to )
        {
            int howMuchToRead = min( to.limit(), from.remaining() );
            int leftToRead = howMuchToRead;
            while ( leftToRead > 0 )
            {
                int howMuchToReadThisTime = min( leftToRead, scratchPad.length );
                from.get( scratchPad, 0, howMuchToReadThisTime );
                to.put( scratchPad, 0, howMuchToReadThisTime );
                leftToRead -= howMuchToReadThisTime;
            }
            return howMuchToRead;
        }

        @Override
        public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int write( ByteBuffer src ) throws IOException
        {
            int result = transfer( src, fileAsBuffer );
            size = max( size, (int) position() );
            return result;
        }

        @Override
        public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long position() throws IOException
        {
            return fileAsBuffer.position();
        }

        @Override
        public FileChannel position( long newPosition ) throws IOException
        {
            fileAsBuffer.position( (int) newPosition );
            return this;
        }

        @Override
        public long size()
        {
            return size;
        }

        @Override
        public FileChannel truncate( long size ) throws IOException
        {
            this.size = (int) size;
            return this;
        }

        @Override
        public void force( boolean metaData ) throws IOException
        {
        }

        @Override
        public long transferTo( long position, long count, WritableByteChannel target ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferFrom( ReadableByteChannel src, long position, long count ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read( ByteBuffer dst, long position ) throws IOException
        {
            long previous = position();
            position( position );
            try
            {
                return read( dst );
            }
            finally
            {
                position( previous );
            }
        }

        @Override
        public int write( ByteBuffer src, long position ) throws IOException
        {
            long previous = position();
            position( position );
            try
            {
                return write( src );
            }
            finally
            {
                position( previous );
            }
        }

        @Override
        public MappedByteBuffer map( MapMode mode, long position, long size ) throws IOException
        {
            throw new IOException( "Not supported" );
        }

        @Override
        public java.nio.channels.FileLock lock( long position, long size, boolean shared ) throws IOException
        {
            if ( locked > 0 ) return null;
            return new EphemeralFileLock( this );
        }

        @Override
        public java.nio.channels.FileLock tryLock( long position, long size, boolean shared ) throws IOException
        {
            if ( locked > 0 ) throw new IOException( "Locked" );
            return new EphemeralFileLock( this );
        }

        @Override
        protected void implCloseChannel() throws IOException
        {
        }
    }
    
    private static class EphemeralFileLock extends java.nio.channels.FileLock
    {
        private final EphemeralFileChannel channel;
        private boolean released;

        EphemeralFileLock( EphemeralFileChannel channel )
        {
            super( channel, 0, Long.MAX_VALUE, false );
            this.channel = channel;
            channel.locked++;
        }
        
        @Override
        public boolean isValid()
        {
            return !released;
        }

        @Override
        public void release() throws IOException
        {
            if ( released ) return;
            channel.locked--;
            released = true;
        }
    }
}
