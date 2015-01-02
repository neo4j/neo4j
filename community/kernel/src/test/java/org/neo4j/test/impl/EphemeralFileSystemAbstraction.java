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
package org.neo4j.test.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.StoreFileChannel;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Arrays.asList;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class EphemeralFileSystemAbstraction extends LifecycleAdapter implements FileSystemAbstraction
{
    private final Set<File> directories = Collections.newSetFromMap( new ConcurrentHashMap<File, Boolean>() );
    private final Map<File, EphemeralFileData> files;

    public EphemeralFileSystemAbstraction()
    {
        this.files = new ConcurrentHashMap<>();
    }

    private EphemeralFileSystemAbstraction( Set<File> directories, Map<File, EphemeralFileData> files )
    {
        this.files = new ConcurrentHashMap<>( files );
        this.directories.addAll( directories );
        this.files.putAll( files );
    }

    @Override
    public synchronized void shutdown()
    {
        for ( EphemeralFileData file : files.values() )
            free( file );
        files.clear();

        for ( ThirdPartyFileSystem thirdPartyFileSystem : thirdPartyFileSystems.values() )
        {
            thirdPartyFileSystem.close();
        }
        thirdPartyFileSystems.clear();
    }

    @Override
    protected void finalize() throws Throwable
    {
        shutdown();
        super.finalize();
    }

    public void assertNoOpenFiles() throws Exception
    {
        List<FileStillOpenException> open = new ArrayList<>();
        for ( EphemeralFileData file : files.values() )
        {
            for ( EphemeralFileChannel channel : loop( file.getOpenChannels() ) )
            {
                open.add( channel.openedAt );
            }
        }
        MultipleExceptionsStrategy.assertEmptyExceptions( open );
    }

    public void dumpZip( OutputStream output ) throws IOException
    {
        try ( ZipOutputStream zip = new ZipOutputStream( output ) )
        {
            String prefix = null;
            for ( Map.Entry<File, EphemeralFileData> entry : files.entrySet() )
            {
                File file = entry.getKey();
                String parent = file.getParentFile().getAbsolutePath();
                if ( prefix == null || prefix.startsWith( parent ) )
                {
                    prefix = parent;
                }
                zip.putNextEntry( new ZipEntry( file.getAbsolutePath() ) );
                entry.getValue().dumpTo( zip );
                zip.closeEntry();
            }
            for ( ThirdPartyFileSystem fs : thirdPartyFileSystems.values() )
            {
                fs.dumpToZip( zip, EphemeralFileData.SCRATCH_PAD.get() );
            }
            if ( prefix != null )
            {
                File directory = new File( prefix );
                if ( directory.exists() ) // things ended up on the file system...
                {
                    addRecursively( zip, directory );
                }
            }
        }
    }

    private void addRecursively( ZipOutputStream output, File input ) throws IOException
    {
        if ( input.isFile() )
        {
            output.putNextEntry( new ZipEntry( input.getAbsolutePath() ) );
            byte[] scratchPad = EphemeralFileData.SCRATCH_PAD.get();
            try ( FileInputStream source = new FileInputStream( input ) )
            {
                for ( int read; 0 <= (read = source.read( scratchPad )); )
                {
                    output.write( scratchPad, 0, read );
                }
            }
            output.closeEntry();
        }
        else
        {
            File[] children = input.listFiles();
            if ( children != null )
            {
                for ( File child : children )
                {
                    addRecursively( output, child );
                }
            }
        }
    }

    @SuppressWarnings( "serial" )
    private static class FileStillOpenException extends Exception
    {
        private final String filename;

        FileStillOpenException( String filename )
        {
            super( "File still open: [" + filename + "]" );
            this.filename = filename;
        }
    }

    private void free(EphemeralFileData file)
    {
        if (file != null) file.fileAsBuffer.free();
    }

    @Override
    public synchronized StoreChannel open( File fileName, String mode ) throws IOException
    {
        EphemeralFileData data = files.get( fileName );
        if ( data != null )
        {
            return new StoreFileChannel( new EphemeralFileChannel(
                    data, new FileStillOpenException( fileName.getPath() ) ) );
        }
        return create( fileName );
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return new ChannelOutputStream( open( fileName, "rw" ), append );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return new ChannelInputStream( open( fileName, "r" ) );
    }

    @Override
    public Reader openAsReader( File fileName, String encoding ) throws IOException
    {
        return new InputStreamReader( openAsInputStream( fileName ), encoding );
    }

    @Override
    public Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException
    {
        return new OutputStreamWriter( openAsOutputStream( fileName, append ), encoding );
    }

    @Override
    public FileLock tryLock(File fileName, StoreChannel channel) throws IOException
    {
        final java.nio.channels.FileLock lock = channel.tryLock();
        return new FileLock()
        {
            @Override
            public void release() throws IOException
            {
                lock.release();
            }
        };
    }

    @Override
    public synchronized StoreChannel create( File fileName ) throws IOException
    {
        File parentFile = fileName.getParentFile();
        if ( parentFile != null /*means that this is the 'default location'*/ && !fileExists( parentFile ) )
        {
            throw new FileNotFoundException( "'" + fileName
                    + "' (The system cannot find the path specified)" );
        }

        EphemeralFileData data = new EphemeralFileData();
        free( files.put( fileName, data ) );
        return new StoreFileChannel(
                new EphemeralFileChannel( data, new FileStillOpenException( fileName.getPath() ) ) );
    }

    @Override
    public long getFileSize(File fileName)
    {
        EphemeralFileData file = files.get(fileName);
        return file == null ? 0 : file.size();
    }

    @Override
    public boolean fileExists( File file )
    {
        return directories.contains( file.getAbsoluteFile() ) || files.containsKey( file );
    }

    @Override
    public boolean isDirectory( File file )
    {
        return directories.contains( file.getAbsoluteFile() );
    }

    @Override
    public boolean mkdir( File directory )
    {
        if ( fileExists( directory ) )
        {
            return false;
        }

        directories.add( directory.getAbsoluteFile() );
        return true;
    }

    @Override
    public void mkdirs( File directory )
    {
        File currentDirectory = directory.getAbsoluteFile();

        while ( currentDirectory != null )
        {
            mkdir( currentDirectory );
            currentDirectory = currentDirectory.getParentFile();
        }
    }

    @Override
    public boolean deleteFile(File fileName)
    {
        EphemeralFileData removed = files.remove( fileName );
        free( removed );
        return removed != null;
    }

    @Override
    public void deleteRecursively( File directory ) throws IOException
    {
        List<String> directoryPathItems = splitPath( directory );
        for ( Map.Entry<File, EphemeralFileData> file : files.entrySet() )
        {
            File fileName = file.getKey();
            List<String> fileNamePathItems = splitPath( fileName );
            if ( directoryMatches( directoryPathItems, fileNamePathItems ) )
                deleteFile( fileName );
        }
    }

    @Override
    public boolean renameFile(File from, File to) throws IOException
    {
        if (!files.containsKey( from )) throw new IOException("'" + from + "' doesn't exist");
        if (files.containsKey(to)) throw new IOException("'" + to + "' already exists");
        files.put(to, files.remove(from));
        return true;
    }

    @Override
    public File[] listFiles( File directory )
    {
        if ( files.containsKey( directory ) )
            // This means that you're trying to list files on a file, not a directory.
            return null;

        List<String> directoryPathItems = splitPath( directory );
        List<File> found = new ArrayList<>();
        for ( Map.Entry<File, EphemeralFileData> file : files.entrySet() )
        {
            File fileName = file.getKey();
            List<String> fileNamePathItems = splitPath( fileName );
            if ( directoryMatches( directoryPathItems, fileNamePathItems ) )
                found.add( constructPath( fileNamePathItems, directoryPathItems.size()+1 ) );
        }
        return found.toArray( new File[found.size()] );
    }

    @Override
    public File[] listFiles( File directory, FilenameFilter filter )
    {
        if ( files.containsKey( directory ) )
            // This means that you're trying to list files on a file, not a directory.
            return null;

        List<String> directoryPathItems = splitPath( directory );
        List<File> found = new ArrayList<>();
        for ( Map.Entry<File, EphemeralFileData> file : files.entrySet() )
        {
            File fileName = file.getKey();
            List<String> fileNamePathItems = splitPath( fileName );
            if ( directoryMatches( directoryPathItems, fileNamePathItems ) )
            {
                File path = constructPath( fileNamePathItems, directoryPathItems.size() + 1 );
                if ( filter.accept( path.getParentFile(), path.getName() ) )
                {
                    found.add( path );
                }
            }
        }
        return found.toArray( new File[found.size()] );
    }

    private File constructPath( List<String> pathItems, int count )
    {
        File file = null;
        for ( String pathItem : pathItems.subList( 0, count ) )
            file = file == null ? new File( pathItem ) : new File( file, pathItem );
        return file;
    }

    private boolean directoryMatches( List<String> directoryPathItems, List<String> fileNamePathItems )
    {
        return fileNamePathItems.size() > directoryPathItems.size() &&
               fileNamePathItems.subList( 0, directoryPathItems.size() ).equals( directoryPathItems );
    }

    private List<String> splitPath( File path )
    {
        return asList( path.getPath().replaceAll( "\\\\", "/" ).split( "/" ) );
    }

    @Override
    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        EphemeralFileData fileToMove = files.remove( file );
        if ( fileToMove == null )
            throw new FileNotFoundException( file.getPath() );
        files.put( new File( toDirectory, file.getName() ), fileToMove );
    }

    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        EphemeralFileData data = files.get( from );
        if ( data == null )
            throw new FileNotFoundException( "File " + from + " not found" );
        copyFile( from, this, to, newCopyBuffer() );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        copyRecursivelyFromOtherFs( fromDirectory, this, toDirectory, newCopyBuffer() );
    }

    interface Positionable
    {
        long pos();

        void pos( long position );
    }

    static class LocalPosition implements Positionable
    {
        private long position;

        public LocalPosition( long position )
        {
            this.position = position;
        }

        @Override
        public long pos()
        {
            return position;
        }

        @Override
        public void pos( long position )
        {
            this.position = position;
        }
    }

    private static class EphemeralFileChannel extends FileChannel implements Positionable
    {
        final FileStillOpenException openedAt;
        private final EphemeralFileData data;
        private long position = 0;

        EphemeralFileChannel( EphemeralFileData data, FileStillOpenException opened )
        {
            this.data = data;
            this.openedAt = opened;
            data.open( this );
        }

        @Override
        public String toString()
        {
            return String.format( "%s[%s]", getClass().getSimpleName(), openedAt.filename );
        }

        @Override
        public int read( ByteBuffer dst )
        {
            return data.read( this, dst );
        }

        @Override
        public long read( ByteBuffer[] dsts, int offset, int length )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int write( ByteBuffer src ) throws IOException
        {
            return data.write( this, src );
        }

        @Override
        public long write( ByteBuffer[] srcs, int offset, int length )
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
        public long transferFrom( ReadableByteChannel src, long position, long count ) throws IOException
        {
            long previousPos = position();
            position( position );
            try
            {
                long transferred = 0;
                ByteBuffer intermediary = ByteBuffer.allocateDirect( 8096 );
                while ( transferred < count )
                {
                    intermediary.clear();
                    intermediary.limit( (int) min( intermediary.capacity(), count-transferred ) );
                    int read = src.read( intermediary );
                    if ( read == -1 )
                        break;
                    transferred += read;
                    intermediary.flip();
                }
                return transferred;
            }
            finally
            {
                position( previousPos );
            }
        }

        @Override
        public int read( ByteBuffer dst, long position ) throws IOException
        {
            return data.read( new LocalPosition( position ), dst );
        }

        @Override
        public int write( ByteBuffer src, long position ) throws IOException
        {
            return data.write( new LocalPosition( position ), src );
        }

        @Override
        public MappedByteBuffer map( FileChannel.MapMode mode, long position, long size ) throws IOException
        {
            throw new IOException("Not supported");
        }

        @Override
        public java.nio.channels.FileLock lock( long position, long size, boolean shared ) throws IOException
        {
            synchronized ( data.channels )
            {
                if ( !data.lock() )
                {
                    return null;
                }
                return new EphemeralFileLock( this, data );
            }
        }

        @Override
        public java.nio.channels.FileLock tryLock( long position, long size, boolean shared ) throws IOException
        {
            synchronized ( data.channels )
            {
                if ( !data.lock() )
                {
                    throw new IOException( "Locked" );
                }
                return new EphemeralFileLock( this, data );
            }
        }

        @Override
        protected void implCloseChannel() throws IOException
        {
            data.close( this );
        }

        @Override
        public long pos()
        {
            return position;
        }

        @Override
        public void pos( long position )
        {
            this.position = position;
        }
    }

    private static class EphemeralFileData
    {
        private static final ThreadLocal<byte[]> SCRATCH_PAD = new ThreadLocal<byte[]>()
        {
            @Override
            protected byte[] initialValue()
            {
                return new byte[1024];
            }
        };
        private final DynamicByteBuffer fileAsBuffer;
        private final Collection<WeakReference<EphemeralFileChannel>> channels = new LinkedList<>();
        private int size;
        private int locked;

        public EphemeralFileData()
        {
            this( new DynamicByteBuffer() );
        }

        private EphemeralFileData( DynamicByteBuffer data )
        {
            this.fileAsBuffer = data;
        }

        int read( Positionable fc, ByteBuffer dst )
        {
            int wanted = dst.limit();
            int available = min(wanted, (int) (size() - fc.pos()));
            if ( available == 0 ) return -1; // EOF
            int pending = available;
            // Read up until our internal size
            byte[] scratchPad = SCRATCH_PAD.get();
            while (pending > 0)
            {
                int howMuchToReadThisTime = min(pending, scratchPad.length);
                long pos = fc.pos();
                fileAsBuffer.get((int) pos, scratchPad, 0, howMuchToReadThisTime);
                fc.pos( pos + howMuchToReadThisTime );
                dst.put(scratchPad, 0, howMuchToReadThisTime);
                pending -= howMuchToReadThisTime;
            }
            return available; // return how much data was read
        }

        int write( Positionable fc, ByteBuffer src )
        {
            int wanted = src.limit();
            int pending = wanted;
            byte[] scratchPad = SCRATCH_PAD.get();
            while ( pending > 0 )
            {
                int howMuchToWriteThisTime = min( pending, scratchPad.length );
                src.get( scratchPad, 0, howMuchToWriteThisTime );
                long pos = fc.pos();
                fileAsBuffer.put( (int) pos, scratchPad, 0, howMuchToWriteThisTime );
                fc.pos( pos += howMuchToWriteThisTime );
                pending -= howMuchToWriteThisTime;
            }

            synchronized ( fileAsBuffer )
            {
                // If we just made a jump in the file fill the rest of the gap with zeros
                int newSize = max( size, (int) fc.pos() );
                int intermediaryBytes = newSize - wanted - size;
                if ( intermediaryBytes > 0 )
                {
                    fileAsBuffer.fillWithZeros( size, intermediaryBytes );
                }

                size = newSize;
            }
            return wanted;
        }

        EphemeralFileData copy()
        {
            synchronized ( fileAsBuffer )
            {
                EphemeralFileData copy = new EphemeralFileData( fileAsBuffer.copy() );
                copy.size = size;
                return copy;
            }
        }

        void open( EphemeralFileChannel channel )
        {
            synchronized ( channels )
            {
                channels.add( new WeakReference<>( channel ) );
            }
        }

        void close( EphemeralFileChannel channel )
        {
            synchronized ( channels )
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

        long size()
        {
            synchronized ( fileAsBuffer )
            {
                return size;
            }
        }

        void truncate( long newSize )
        {
            synchronized ( fileAsBuffer )
            {
                this.size = (int) newSize;
            }
        }

        boolean lock()
        {
            return locked == 0;
        }

        void dumpTo( OutputStream target ) throws IOException
        {
            byte[] scratchPad = SCRATCH_PAD.get();
            synchronized ( fileAsBuffer )
            {
                fileAsBuffer.dump( target, scratchPad, size );
            }
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
            synchronized ( file.channels )
            {
                if ( file == null || file.locked == 0 )
                {
                    return;
                }
                file.locked--;
                file = null;
            }
        }
    }

    /**
     * Dynamically expanding ByteBuffer substitute/wrapper. This will allocate ByteBuffers on the go
     * so that we don't have to allocate too big of a buffer up-front.
     */
    private static class DynamicByteBuffer
    {
        private static final int[] SIZES;

        /**
         * Holds a set of pools of unused BytBuffers, where pools are implemented by {@link Queue}s.
         * Each pool contains only {@link ByteBuffer} of the same size. This way, we have pools for
         * different sized {@link ByteBuffer}, and can pick an available byte buffer that suits what
         * we want to store quickly.
         */
        private static volatile AtomicReferenceArray<Queue<Reference<ByteBuffer>>> POOLS;
        private static final byte[] zeroBuffer = new byte[1024];

        synchronized DynamicByteBuffer copy()
        {
            return new DynamicByteBuffer( buf ); // invoke "copy constructor"
        }

        static
        {
            int K = 1024;
            SIZES = new int[] { 64 * K, 128 * K, 256 * K, 512 * K, 1024 * K };

            POOLS = new AtomicReferenceArray<>( SIZES.length );
            for ( int sizeIndex = 0; sizeIndex < SIZES.length; sizeIndex++ )
                POOLS.set( sizeIndex, new ConcurrentLinkedQueue<Reference<ByteBuffer>>() );
        }

        private ByteBuffer buf;

        public DynamicByteBuffer()
        {
            buf = allocate( 0 );
        }

        /** This is a copying constructor, the input buffer is just read from, never stored in 'this'. */
        private DynamicByteBuffer( ByteBuffer toClone )
        {
            int sizeIndex = sizeIndexFor( toClone.capacity() );
            buf = allocate( sizeIndex );
            copyByteBufferContents( toClone, buf );
        }

        private void copyByteBufferContents( ByteBuffer from, ByteBuffer to )
        {
            int positionBefore = from.position();
            try
            {
                from.position( 0 );
                to.put( from );
            }
            finally
            {
                from.position( positionBefore );
                to.position( 0 );
            }
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
                AtomicReferenceArray<Queue<Reference<ByteBuffer>>> pools = POOLS;
                if (sizeIndex + enlargement < pools.length()) {
                    Queue<Reference<ByteBuffer>> queue = pools.get( sizeIndex+enlargement );
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
                AtomicReferenceArray<Queue<Reference<ByteBuffer>>> pools = POOLS;
                // Use soft references to the buffers to allow the GC to reclaim
                // unused buffers if memory gets scarce.
                SoftReference<ByteBuffer> ref = new SoftReference<>( buf );

                // Put our buffer into a pool, create a pool for the buffer size if one does not exist
                ( sizeIndex < pools.length() ? pools.get( sizeIndex ) : getOrCreatePoolForSize( sizeIndex ) ).add( ref );
            }
            finally
            {
                buf = null;
            }
        }

        private static synchronized Queue<Reference<ByteBuffer>> getOrCreatePoolForSize( int sizeIndex )
        {
            AtomicReferenceArray<Queue<Reference<ByteBuffer>>> pools = POOLS;
            if ( sizeIndex >= pools.length() )
            {
                int newSize = pools.length();
                while ( sizeIndex >= newSize )
                    newSize <<= 1;
                AtomicReferenceArray<Queue<Reference<ByteBuffer>>> newPool = new AtomicReferenceArray<>( newSize );
                for ( int i = 0; i < pools.length(); i++ )
                    newPool.set( i, pools.get( i ) );
                for ( int i = pools.length(); i < newPool.length(); i++ )
                    newPool.set( i, new ConcurrentLinkedQueue<Reference<ByteBuffer>>() );
                POOLS = pools = newPool;
            }
            return pools.get( sizeIndex );
        }

        synchronized void put( int pos, byte[] bytes, int offset, int length )
        {
            verifySize( pos+length );
            try
            {
                buf.position( pos );
            }
            catch ( IllegalArgumentException e )
            {
                throw new IllegalArgumentException( buf + ", " + pos, e );
            }
            buf.put( bytes, offset, length );
        }

        synchronized void get( int pos, byte[] scratchPad, int i, int howMuchToReadThisTime )
        {
            buf.position( pos );
            buf.get( scratchPad, i, howMuchToReadThisTime );
        }

        synchronized void fillWithZeros( int pos, int bytes )
        {
            buf.position( pos );
            while ( bytes > 0 )
            {
                int howMuchToReadThisTime = min( bytes, zeroBuffer.length );
                buf.put( zeroBuffer, 0, howMuchToReadThisTime );
                bytes -= howMuchToReadThisTime;
            }
            buf.position( pos );
        }

        /**
         * Checks if more space needs to be allocated.
         */
        private void verifySize( int totalAmount )
        {
            if ( buf.capacity() >= totalAmount )
            {
                return;
            }

            // Double size each time, but after 1M only increase by 1M at a time, until required amount is reached.
            int newSize = buf.capacity();
            int sizeIndex = sizeIndexFor( newSize );
            while ( newSize < totalAmount )
            {
                newSize += Math.min( newSize, 1024 * 1024 );
                sizeIndex++;
            }
            int oldPosition = this.buf.position();
            ByteBuffer buf = allocate( sizeIndex );
            this.buf.position( 0 );
            buf.put( this.buf );
            this.buf = buf;
            this.buf.position( oldPosition );
        }

        private static int sizeIndexFor( int capacity )
        {
            // Double size each time, but after 1M only increase by 1M at a time, until required amount is reached.
            int sizeIndex = capacity / SIZES[SIZES.length - 1];
            if (sizeIndex == 0) for ( ; sizeIndex < SIZES.length; sizeIndex++ )
            {
                if ( capacity == SIZES[sizeIndex] )
                    break;
            }
            else
            {
                sizeIndex += SIZES.length - 1;
            }
            return sizeIndex;
        }

        public void clear()
        {
            this.buf.clear();
        }

        void dump( OutputStream target, byte[] scratchPad, int size ) throws IOException
        {
            buf.position( 0 );
            while ( size > 0 )
            {
                int read = min( size, scratchPad.length );
                buf.get( scratchPad, 0, read );
                size -= read;
                target.write( scratchPad, 0, read );
            }
        }
    }

    public EphemeralFileSystemAbstraction snapshot()
    {
        Map<File, EphemeralFileData> copiedFiles = new HashMap<>();
        for ( Map.Entry<File, EphemeralFileData> file : files.entrySet() )
        {
            copiedFiles.put( file.getKey(), file.getValue().copy() );
        }
        return new EphemeralFileSystemAbstraction( directories, copiedFiles );
    }

    public void copyRecursivelyFromOtherFs( File from, FileSystemAbstraction fromFs, File to ) throws IOException
    {
        copyRecursivelyFromOtherFs( from, fromFs, to, newCopyBuffer() );
    }

    private ByteBuffer newCopyBuffer()
    {
        return ByteBuffer.allocate( 1024*1024 );
    }

    private void copyRecursivelyFromOtherFs( File from, FileSystemAbstraction fromFs, File to, ByteBuffer buffer )
            throws IOException
    {
        this.mkdirs( to );
        for ( File fromFile : fromFs.listFiles( from ) )
        {
            File toFile = new File( to, fromFile.getName() );
            if ( fromFs.isDirectory( fromFile ) )
                copyRecursivelyFromOtherFs( fromFile, fromFs, toFile );
            else
                copyFile( fromFile, fromFs, toFile, buffer );
        }
    }

    private void copyFile( File from, FileSystemAbstraction fromFs, File to, ByteBuffer buffer ) throws IOException
    {
        StoreChannel source = fromFs.open( from, "r" );
        StoreChannel sink = this.open( to, "rw" );
        try
        {
            for ( int available; (available = (int) (source.size() - source.position())) > 0; )
            {
                buffer.clear();
                buffer.limit( min( available, buffer.capacity() ) );
                source.read( buffer );
                buffer.flip();
                sink.write( buffer );
            }
        }
        finally
        {
            if ( source != null )
                source.close();
            if ( sink != null )
                sink.close();
        }
    }

    private final Map<Class<? extends ThirdPartyFileSystem>, ThirdPartyFileSystem> thirdPartyFileSystems =
            new HashMap<>();

    @Override
    public synchronized <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem(
            Class<K> clazz, Function<Class<K>, K> creator )
    {
        ThirdPartyFileSystem fileSystem = thirdPartyFileSystems.get( clazz );
        if (fileSystem == null)
        {
            thirdPartyFileSystems.put( clazz, fileSystem = creator.apply( clazz ) );
        }
        return clazz.cast( fileSystem );
    }
}
