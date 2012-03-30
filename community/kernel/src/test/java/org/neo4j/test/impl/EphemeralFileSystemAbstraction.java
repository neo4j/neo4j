/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.test.impl;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class EphemeralFileSystemAbstraction implements FileSystemAbstraction, Lifecycle
{
    private final Map<String, EphemeralFileData> files = new HashMap<String, EphemeralFileData>();

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
    {
        for (EphemeralFileData file : files.values()) free(file);
        files.clear();

        DynamicByteBuffer.dispose();
    }

    @Override
    protected void finalize() throws Throwable
    {
        shutdown();
        super.finalize();
    }

    @SuppressWarnings( "deprecation" )
    public void assertNoOpenFiles() throws Exception
    {
        List<Throwable> open = new ArrayList<Throwable>();
        for ( EphemeralFileData file : files.values() )
        {
            for ( EphemeralFileChannel channel : loop( file.getOpenChannels() ) )
            {
                open.add( channel.openedAt );
            }
        }
        if (!open.isEmpty())
        {
            if (open.size() == 1) throw (FileStillOpenException) open.get( 0 );
            throw new org.junit.internal.runners.model.MultipleFailureException( open );
        }
    }

    @SuppressWarnings( "serial" )
    private static class FileStillOpenException extends Exception
    {
        FileStillOpenException( String filename )
        {
            super( "File still open: [" + filename + "]" );
        }
    }

    private void free(EphemeralFileData file)
    {
        if (file != null) file.fileAsBuffer.free();
    }

    @Override
    public synchronized FileChannel open( String fileName, String mode ) throws IOException
    {
        EphemeralFileData data = files.get( fileName );
        return data != null ? new EphemeralFileChannel( data, new FileStillOpenException( fileName ) ) : create( fileName );
    }

    @Override
    public FileLock tryLock(String fileName, FileChannel channel) throws IOException
    {
        if ( channel instanceof EphemeralFileChannel )
        {
            EphemeralFileChannel efc = (EphemeralFileChannel) channel;
            final java.nio.channels.FileLock lock = efc.tryLock();
            return new FileLock()
            {
                @Override
                public void release() throws IOException
                {
                    lock.release();
                }
            };
        }
        System.err.println("WARNING: locking non-ephemeral FileChannel[" + channel + "] through EphemeralFileSystem, for: " + fileName);
        return FileLock.getOsSpecificFileLock(fileName, channel);
    }

    @Override
    public synchronized FileChannel create(String fileName) throws IOException
    {
        EphemeralFileData data = new EphemeralFileData();
        free(files.put(fileName, data));
        return new EphemeralFileChannel( data, new FileStillOpenException( fileName ) );
    }

    @Override
    public long getFileSize(String fileName)
    {
        EphemeralFileData file = files.get(fileName);
        return file == null ? 0 : file.size();
    }

    @Override
    public boolean fileExists(String fileName)
    {
        return files.containsKey(fileName);
    }

    @Override
    public boolean deleteFile(String fileName)
    {
        free(files.remove(fileName));
        return true;
    }

    @Override
    public boolean renameFile(String from, String to) throws IOException
    {
        if (!files.containsKey( from )) throw new IOException("'" + from + "' doesn't exist");
        if (files.containsKey(to)) throw new IOException("'" + to + "' already exists");
        files.put(to, files.remove(from));
        return true;
    }

    private static class EphemeralFileChannel extends FileChannel
    {
        final FileStillOpenException openedAt;
        private final EphemeralFileData data;
        long position = 0;

        EphemeralFileChannel( EphemeralFileData data, FileStillOpenException opened )
        {
            this.data = data;
            this.openedAt = opened;
            data.open( this );
        }

        @Override
        public int read( ByteBuffer dst )
        {
            return data.read( this, dst );
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int write( ByteBuffer src ) throws IOException
        {
            return data.write( this, src );
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long position() throws IOException
        {
            return position;
        }

        @Override
        public FileChannel position( long newPosition ) throws IOException
        {
            this.position = newPosition;
            return this;
        }

        @Override
        public long size() throws IOException
        {
            return data.size();
        }

        @Override
        public FileChannel truncate( long size ) throws IOException
        {
            data.truncate( size );
            return this;
        }

        @Override
        public void force(boolean metaData)
        {
            // NO-OP
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read( ByteBuffer dst, long position ) throws IOException
        {
            long prev = this.position;
            this.position = position;
            try
            {
                return data.read( this, dst );
            }
            finally
            {
                this.position = prev;
            }
        }

        @Override
        public int write( ByteBuffer src, long position ) throws IOException
        {
            long prev = this.position;
            this.position = position;
            try
            {
                return data.write( this, src );
            }
            finally
            {
                this.position = prev;
            }
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException
        {
            throw new IOException("Not supported");
        }

        @Override
        public java.nio.channels.FileLock lock( long position, long size, boolean shared ) throws IOException
        {
            if ( !data.lock() ) return null;
            return new EphemeralFileLock( this, data );
        }

        @Override
        public java.nio.channels.FileLock tryLock( long position, long size, boolean shared ) throws IOException
        {
            if ( !data.lock() ) throw new IOException( "Locked" );
            return new EphemeralFileLock( this, data );
        }

        @Override
        protected void implCloseChannel() throws IOException
        {
            data.close( this );
        }
    }

    private static class EphemeralFileData
    {
        private final DynamicByteBuffer fileAsBuffer = new DynamicByteBuffer();
        private final byte[] scratchPad = new byte[1024];
        private final Collection<WeakReference<EphemeralFileChannel>> channels = new LinkedList<WeakReference<EphemeralFileChannel>>();
        private int size;
        private int locked;

        int read( EphemeralFileChannel fc, ByteBuffer dst )
        {
            int wanted = dst.limit();
            int available = min(wanted, (int) (size - fc.position));
            if ( available == 0 ) return -1; // EOF
            int pending = available;
            // Read up until our internal size
            while (pending > 0)
            {
                int howMuchToReadThisTime = min(pending, scratchPad.length);
                fileAsBuffer.get((int)fc.position, scratchPad, 0, howMuchToReadThisTime);
                fc.position += howMuchToReadThisTime;
                dst.put(scratchPad, 0, howMuchToReadThisTime);
                pending -= howMuchToReadThisTime;
            }
            return available; // return how much data was read
        }

        void open( EphemeralFileChannel channel )
        {
            channels.add( new WeakReference<EphemeralFileChannel>( channel ) );
        }

        void close( EphemeralFileChannel channel )
        {
            locked = 0; // Regular file systems seems to release all file locks when closed...
            for ( Iterator<EphemeralFileChannel> iter = getOpenChannels(); iter.hasNext(); )
            {
                if ( iter.next() == channel )
                {
                    iter.remove();
                }
            }
        }

        Iterator<EphemeralFileChannel> getOpenChannels()
        {
            final Iterator<WeakReference<EphemeralFileChannel>> refs = channels.iterator();
            return new PrefetchingIterator<EphemeralFileChannel>()
            {
                @Override
                protected EphemeralFileChannel fetchNextOrNull()
                {
                    while ( refs.hasNext() )
                    {
                        EphemeralFileChannel channel = refs.next().get();
                        if ( channel != null ) return channel;
                        refs.remove();
                    }
                    return null;
                }

                @Override
                public void remove()
                {
                    refs.remove();
                }
            };
        }

        boolean isOpen()
        {
            return getOpenChannels().hasNext();
        }

        int write(EphemeralFileChannel fc, ByteBuffer src)
        {
            int wanted = src.limit();
            int pending = wanted;
            while (pending > 0)
            {
                int howMuchToWriteThisTime = min(pending, scratchPad.length);
                src.get(scratchPad, 0, howMuchToWriteThisTime);
                fileAsBuffer.put((int)fc.position, scratchPad, 0, howMuchToWriteThisTime);
                fc.position += howMuchToWriteThisTime;
                pending -= howMuchToWriteThisTime;
            }

            // If we just made a jump in the file fill the rest of the gap with zeros
            int newSize = max(size, (int) fc.position);
            int intermediaryBytes = newSize-wanted-size;
            if ( intermediaryBytes > 0 )
            {
                fileAsBuffer.fillWithZeros(size, intermediaryBytes);
                fileAsBuffer.buf.position( size );
                //fillWithZeros( fileAsBuffer.buf, intermediaryBytes );
            }

            size = newSize;
            return wanted;
        }

        long size()
        {
            return size;
        }

        void truncate(long newSize)
        {
            this.size = (int) newSize;
        }

        boolean lock()
        {
            return locked == 0;
        }
    }

    private static class EphemeralFileLock extends java.nio.channels.FileLock
    {
        private EphemeralFileData file;

        EphemeralFileLock(EphemeralFileChannel channel, EphemeralFileData file)
        {
            super(channel, 0, Long.MAX_VALUE, false);
            this.file = file;
            file.locked++;
        }

        @Override
        public boolean isValid()
        {
            return file != null;
        }

        @Override
        public void release() throws IOException
        {
            if (file == null || file.locked == 0) return;
            file.locked--;
            file = null;
        }
    }

    /**
     * Dynamically expanding ByteBuffer substitute/wrapper. This will allocate ByteBuffers on the go
     * so that we don't have to allocate too big of a buffer up-front.
     */
    private static class DynamicByteBuffer
    {
        private static final int[] SIZES;
        private static volatile AtomicReferenceArray<Queue<Reference<ByteBuffer>>> POOL;
        private static final byte[] zeroBuffer = new byte[1024];

        static void dispose()
        {
            for (int i = POOL.length(); i < POOL.length(); i++)
            {
                for( Reference<ByteBuffer> byteBufferReference : POOL.get( i ) )
                {
                    ByteBuffer byteBuffer = byteBufferReference.get();
                    if ( byteBuffer != null)
                    {
                        try
                        {
                            destroyDirectByteBuffer( byteBuffer );
                        }
                        catch( Throwable e )
                        {
                            e.printStackTrace();
                        }
                    }
                }
            }

            init();
        }

        private static void destroyDirectByteBuffer(ByteBuffer toBeDestroyed)
            throws IllegalArgumentException, IllegalAccessException,
                   InvocationTargetException, SecurityException, NoSuchMethodException
        {
            Method cleanerMethod = toBeDestroyed.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(toBeDestroyed);
            Method cleanMethod = cleaner.getClass().getMethod("clean");
            cleanMethod.setAccessible(true);
            cleanMethod.invoke(cleaner);
        }

        private static void init()
        {
            AtomicReferenceArray<Queue<Reference<ByteBuffer>>> pool = POOL = new AtomicReferenceArray<Queue<Reference<ByteBuffer>>>( SIZES.length );
            for ( int i = 0; i < SIZES.length; i++ ) pool.set( i, new ConcurrentLinkedQueue<Reference<ByteBuffer>>() );
        }

        static
        {
            int K = 1024;
            SIZES = new int[] { 64 * K, 128 * K, 256 * K, 512 * K, 1024 * K };
            init();
        }

        private ByteBuffer buf;

        public DynamicByteBuffer()
        {
            buf = allocate( 0 );
        }

        /**
         * Tries to allocate a buffer of at least the specified size.
         * If no free buffers are available of the available capacity, we
         * check for buffers up to two sizes larger. If still no buffers
         * are found we allocate a new buffer of the specified size.
         */
        private ByteBuffer allocate( int sizeIndex )
        {
            for (int enlargement = 0; enlargement < 2; enlargement++) {
                AtomicReferenceArray<Queue<Reference<ByteBuffer>>> pool = POOL;
                if (sizeIndex + enlargement < pool.length()) {
                    Queue<Reference<ByteBuffer>> queue = pool.get( sizeIndex+enlargement );
                    if ( queue != null )
                    {
                        for (;;)
                        {
                            Reference<ByteBuffer> ref = queue.poll();
                            if ( ref == null ) break;
                            ByteBuffer buffer = ref.get();
                            if ( buffer != null ) return buffer;
                        }
                    }
                }
            }
            return ByteBuffer.allocateDirect( ( sizeIndex < SIZES.length ) ? SIZES[sizeIndex]
                    : ( ( sizeIndex - SIZES.length + 1 ) * SIZES[SIZES.length - 1] ) );
        }

        void free()
        {
            try
            {
                clear();
                int sizeIndex = buf.capacity() / SIZES[SIZES.length - 1];
                if (sizeIndex == 0) for ( ; sizeIndex < SIZES.length; sizeIndex++ )
                {
                    if (buf.capacity() == SIZES[sizeIndex]) break;
                }
                else
                {
                    sizeIndex += SIZES.length - 1;
                }
                AtomicReferenceArray<Queue<Reference<ByteBuffer>>> pool = POOL;
                // Use soft references to the buffers to allow the GC to reclaim
                // unused buffers if memory gets scarce.
                SoftReference<ByteBuffer> ref = new SoftReference<ByteBuffer>( buf );
                ( sizeIndex < pool.length() ? pool.get( sizeIndex ) : growPool( sizeIndex ) ).add( ref );
            }
            finally
            {
                buf = null;
            }
        }

        private static synchronized Queue<Reference<ByteBuffer>> growPool( int sizeIndex )
        {
            AtomicReferenceArray<Queue<Reference<ByteBuffer>>> pool = POOL;
            if ( sizeIndex >= pool.length()) {
                int newSize = pool.length();
                while ( sizeIndex >= newSize ) newSize <<= 1;
                AtomicReferenceArray<Queue<Reference<ByteBuffer>>> newPool = new AtomicReferenceArray<Queue<Reference<ByteBuffer>>>( newSize );
                for ( int i = 0; i < pool.length(); i++ )
                    newPool.set( i, pool.get( i ) );
                for ( int i = pool.length(); i < newPool.length(); i++ )
                    newPool.set( i, new ConcurrentLinkedQueue<Reference<ByteBuffer>>() );
                POOL = pool = newPool;
            }
            return pool.get( sizeIndex );
        }

        void put(int pos, byte[] bytes, int offset, int length)
        {
            buf.position( pos );
            verifySize(length);
            buf.put( bytes, offset, length );
        }

        void get(int pos, byte[] scratchPad, int i, int howMuchToReadThisTime)
        {
            buf.position( pos );
            buf.get(scratchPad, i, howMuchToReadThisTime);
        }

        void fillWithZeros( int pos, int bytes )
        {
            buf.position( pos );
            while ( bytes > 0 )
            {
                int howMuchToReadThisTime = min( bytes, zeroBuffer.length );
                buf.put( zeroBuffer, 0, howMuchToReadThisTime );
                bytes -= howMuchToReadThisTime;
            }
        }

        /**
         * Checks if more space needs to be allocated.
         */
        private void verifySize(int amount)
        {
            if (buf.remaining() >= amount)
            {
                return;
            }

            // Double size each time, but after 1M only increase by 1M at a time, until required amount is reached.
            int newSize = buf.capacity();
            int sizeIndex = newSize / SIZES[SIZES.length - 1];
            if (sizeIndex == 0) for ( ; sizeIndex < SIZES.length; sizeIndex++ )
            {
                if (newSize == SIZES[sizeIndex]) break;
            }
            else
            {
                sizeIndex += SIZES.length - 1;
            }
            for ( int required = newSize + amount - buf.remaining();
                  newSize < required;
                  newSize += Math.min( newSize, 1024 * 1024 ), sizeIndex++ );
            int oldPosition = this.buf.position();
            ByteBuffer buf = allocate( sizeIndex );
            this.buf.position(0);
            buf.put(this.buf);
            this.buf = buf;
            this.buf.position(oldPosition);
        }

        public void clear()
        {
            this.buf.clear();
        }
    }
}