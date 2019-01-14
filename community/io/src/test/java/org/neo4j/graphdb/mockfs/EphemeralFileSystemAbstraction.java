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
package org.neo4j.graphdb.mockfs;

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
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.fs.StreamFilesRecursive;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.test.impl.ChannelInputStream;
import org.neo4j.test.impl.ChannelOutputStream;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Arrays.asList;

public class EphemeralFileSystemAbstraction implements FileSystemAbstraction
{
    private final Clock clock;
    private volatile boolean closed;

    interface Positionable
    {
        long pos();

        void pos( long position );
    }

    private final Set<File> directories = Collections.newSetFromMap( new ConcurrentHashMap<>() );
    private final Map<File,EphemeralFileData> files;
    private final Map<Class<? extends ThirdPartyFileSystem>,ThirdPartyFileSystem> thirdPartyFileSystems =
            new HashMap<>();

    public EphemeralFileSystemAbstraction()
    {
        this( Clock.systemUTC() );
    }

    public EphemeralFileSystemAbstraction( Clock clock )
    {
        this.clock = clock;
        this.files = new ConcurrentHashMap<>();
        initCurrentWorkingDirectory();
    }

    private void initCurrentWorkingDirectory()
    {
        try
        {
            mkdirs( new File( "." ).getCanonicalFile() );
        }
        catch ( IOException e )
        {
            System.err.println(
                    "WARNING: EphemeralFileSystemAbstraction could not initialise current working directory" );
            e.printStackTrace();
        }
    }

    private EphemeralFileSystemAbstraction( Set<File> directories, Map<File,EphemeralFileData> files, Clock clock )
    {
        this.clock = clock;
        this.files = new ConcurrentHashMap<>( files );
        this.directories.addAll( directories );
        initCurrentWorkingDirectory();
    }

    /**
     * Simulate a filesystem crash, in which any changes that have not been {@link StoreChannel#force}d
     * will be lost. Practically, all files revert to the state when they are last {@link StoreChannel#force}d.
     */
    public void crash()
    {
        files.values().forEach( EphemeralFileSystemAbstraction.EphemeralFileData::crash );
    }

    @Override
    public synchronized void close() throws IOException
    {
        closeFiles();
        closeFileSystems();
        closed = true;
    }

    public boolean isClosed()
    {
        return closed;
    }

    private void closeFileSystems() throws IOException
    {
        IOUtils.closeAll( thirdPartyFileSystems.values() );
        thirdPartyFileSystems.clear();
    }

    private void closeFiles()
    {
        for ( EphemeralFileData file : files.values() )
        {
            file.free();
        }
        files.clear();
    }

    public void assertNoOpenFiles() throws Exception
    {
        FileStillOpenException exception = null;
        for ( EphemeralFileData file : files.values() )
        {
            Iterator<EphemeralFileChannel> channels = file.getOpenChannels();
            while ( channels.hasNext() )
            {
                EphemeralFileChannel channel = channels.next();
                if ( exception == null )
                {
                    exception = channel.openedAt;
                }
                else
                {
                    exception.addSuppressed( channel.openedAt );
                }
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    public void dumpZip( OutputStream output ) throws IOException
    {
        try ( ZipOutputStream zip = new ZipOutputStream( output ) )
        {
            String prefix = null;
            for ( Map.Entry<File,EphemeralFileData> entry : files.entrySet() )
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

    @Override
    public FileWatcher fileWatcher()
    {
        return FileWatcher.SILENT_WATCHER;
    }

    @Override
    public synchronized StoreChannel open( File fileName, OpenMode openMode ) throws IOException
    {
        EphemeralFileData data = files.get( canonicalFile( fileName ) );
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
        return new ChannelOutputStream( open( fileName, OpenMode.READ_WRITE ), append );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return new ChannelInputStream( open( fileName, OpenMode.READ ) );
    }

    @Override
    public Reader openAsReader( File fileName, Charset charset ) throws IOException
    {
        return new InputStreamReader( openAsInputStream( fileName ), charset );
    }

    @Override
    public Writer openAsWriter( File fileName, Charset charset, boolean append ) throws IOException
    {
        return new OutputStreamWriter( openAsOutputStream( fileName, append ), charset );
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

        EphemeralFileData data = files.computeIfAbsent( canonicalFile( fileName ), key -> new EphemeralFileData( clock ) );
        return new StoreFileChannel(
                new EphemeralFileChannel( data, new FileStillOpenException( fileName.getPath() ) ) );
    }

    @Override
    public long getFileSize( File fileName )
    {
        EphemeralFileData file = files.get( canonicalFile( fileName ) );
        return file == null ? 0 : file.size();
    }

    @Override
    public boolean fileExists( File file )
    {
        file = canonicalFile( file );
        return directories.contains( file ) || files.containsKey( file );
    }

    private File canonicalFile( File file )
    {
        try
        {
            return file.getCanonicalFile();
        }
        catch ( IOException e )
        {
            System.err.println( "WARNING: EphemeralFileSystemAbstraction could not canonicalise file: " + file );
            e.printStackTrace();
        }
        // Ugly fallback
        return file.getAbsoluteFile();
    }

    @Override
    public boolean isDirectory( File file )
    {
        return directories.contains( canonicalFile( file ) );
    }

    @Override
    public boolean mkdir( File directory )
    {
        if ( fileExists( directory ) )
        {
            return false;
        }

        directories.add( canonicalFile( directory ) );
        return true;
    }

    @Override
    public void mkdirs( File directory )
    {
        File currentDirectory = canonicalFile( directory );

        while ( currentDirectory != null )
        {
            mkdir( currentDirectory );
            currentDirectory = currentDirectory.getParentFile();
        }
    }

    @Override
    public boolean deleteFile( File fileName )
    {
        fileName = canonicalFile( fileName );
        EphemeralFileData removed = files.remove( fileName );
        if ( removed != null )
        {
            removed.free();
            return true;
        }
        else
        {
            File[] files = listFiles( fileName );
            return files != null && files.length == 0 && directories.remove( fileName );
        }
    }

    @Override
    public void deleteRecursively( File path )
    {
        if ( isDirectory( path ) )
        {
            List<String> directoryPathItems = splitPath( canonicalFile( path ) );
            for ( Map.Entry<File,EphemeralFileData> file : files.entrySet() )
            {
                File fileName = file.getKey();
                List<String> fileNamePathItems = splitPath( fileName );
                if ( directoryMatches( directoryPathItems, fileNamePathItems ) )
                {
                    deleteFile( fileName );
                }
            }
        }
        deleteFile( path );
    }

    @Override
    public void renameFile( File from, File to, CopyOption... copyOptions ) throws IOException
    {
        from = canonicalFile( from );
        to = canonicalFile( to );

        if ( !files.containsKey( from ) )
        {
            throw new NoSuchFileException( "'" + from + "' doesn't exist" );
        }

        boolean replaceExisting = false;
        for ( CopyOption copyOption : copyOptions )
        {
            replaceExisting |= copyOption == REPLACE_EXISTING;
        }
        if ( files.containsKey( to ) && !replaceExisting )
        {
            throw new FileAlreadyExistsException( "'" + to + "' already exists" );
        }
        if ( !isDirectory( to.getParentFile() ) )
        {
            throw new NoSuchFileException( "Target directory[" + to.getParent() + "] does not exists" );
        }
        files.put( to, files.remove( from ) );
    }

    @Override
    public File[] listFiles( File directory )
    {
        directory = canonicalFile( directory );
        if ( files.containsKey( directory ) || !directories.contains( directory ) )
        {
            // This means that you're trying to list files on a file, not a directory.
            return null;
        }

        List<String> directoryPathItems = splitPath( directory );
        Set<File> found = new HashSet<>();
        Iterator<File> files = new CombiningIterator<>( asList( this.files.keySet().iterator(), directories.iterator() ) );
        while ( files.hasNext() )
        {
            File file = files.next();
            List<String> fileNamePathItems = splitPath( file );
            if ( directoryMatches( directoryPathItems, fileNamePathItems ) )
            {
                found.add( constructPath( fileNamePathItems, directoryPathItems ) );
            }
        }

        return found.toArray( new File[found.size()] );
    }

    @Override
    public File[] listFiles( File directory, FilenameFilter filter )
    {
        directory = canonicalFile( directory );
        if ( files.containsKey( directory ) )
        // This means that you're trying to list files on a file, not a directory.
        {
            return null;
        }

        List<String> directoryPathItems = splitPath( directory );
        Set<File> found = new HashSet<>();
        Iterator<File> files = new CombiningIterator<>( asList( this.files.keySet().iterator(), directories.iterator() ) );
        while ( files.hasNext() )
        {
            File file = files.next();
            List<String> fileNamePathItems = splitPath( file );
            if ( directoryMatches( directoryPathItems, fileNamePathItems ) )
            {
                File path = constructPath( fileNamePathItems, directoryPathItems );
                if ( filter.accept( path.getParentFile(), path.getName() ) )
                {
                    found.add( path );
                }
            }
        }
        return found.toArray( new File[found.size()] );
    }

    private File constructPath( List<String> pathItems, List<String> base )
    {
        File file = null;
        if ( base.size() > 0 )
        {
            // We're not directly basing off the root directory
            pathItems = pathItems.subList( 0, base.size() + 1 );
        }
        for ( String pathItem : pathItems )
        {
            String pathItemName = pathItem + File.separator;
            file = file == null ? new File( pathItemName ) : new File( file, pathItemName );
        }
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
        if ( isDirectory( file ) )
        {
            File inner = new File( toDirectory, file.getName() );
            mkdir( inner );
            for ( File f : listFiles( file ) )
            {
                moveToDirectory( f, inner );
            }
            deleteFile( file );
        }
        else
        {
            EphemeralFileData fileToMove = files.remove( canonicalFile( file ) );
            if ( fileToMove == null )
            {
                throw new FileNotFoundException( file.getPath() );
            }
            files.put( canonicalFile( new File( toDirectory, file.getName() ) ), fileToMove );
        }
    }

    @Override
    public void copyToDirectory( File file, File toDirectory ) throws IOException
    {
        File targetFile = new File( toDirectory, file.getName() );
        copyFile( file, targetFile );
    }

    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        EphemeralFileData data = files.get( canonicalFile( from ) );
        if ( data == null )
        {
            throw new FileNotFoundException( "File " + from + " not found" );
        }
        copyFile( from, this, to, newCopyBuffer() );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        copyRecursivelyFromOtherFs( fromDirectory, this, toDirectory, newCopyBuffer() );
    }

    public EphemeralFileSystemAbstraction snapshot()
    {
        Map<File,EphemeralFileData> copiedFiles = new HashMap<>();
        for ( Map.Entry<File,EphemeralFileData> file : files.entrySet() )
        {
            copiedFiles.put( file.getKey(), file.getValue().copy() );
        }
        return new EphemeralFileSystemAbstraction( directories, copiedFiles, clock );
    }

    public void copyRecursivelyFromOtherFs( File from, FileSystemAbstraction fromFs, File to ) throws IOException
    {
        copyRecursivelyFromOtherFs( from, fromFs, to, newCopyBuffer() );
    }

    public long checksum()
    {
        Checksum checksum = new CRC32();
        byte[] data = new byte[(int) ByteUnit.kibiBytes( 1 )];

        // Go through file name list in sorted order, so that checksum is consistent
        List<File> names = new ArrayList<>( files.size() );
        names.addAll( files.keySet() );

        names.sort( Comparator.comparing( File::getAbsolutePath ) );

        for ( File name : names )
        {
            EphemeralFileData file = files.get( name );
            ByteBuffer buf = file.fileAsBuffer.buf();
            buf.position( 0 );
            while ( buf.position() < buf.limit() )
            {
                int len = Math.min( data.length, buf.limit() - buf.position() );
                buf.get( data );
                checksum.update( data, 0, len );
            }
        }
        return checksum.getValue();
    }

    private ByteBuffer newCopyBuffer()
    {
        return ByteBuffer.allocate( (int) ByteUnit.mebiBytes( 1 ) );
    }

    private void copyRecursivelyFromOtherFs( File from, FileSystemAbstraction fromFs, File to, ByteBuffer buffer )
            throws IOException
    {
        this.mkdirs( to );
        for ( File fromFile : fromFs.listFiles( from ) )
        {
            File toFile = new File( to, fromFile.getName() );
            if ( fromFs.isDirectory( fromFile ) )
            {
                copyRecursivelyFromOtherFs( fromFile, fromFs, toFile );
            }
            else
            {
                copyFile( fromFile, fromFs, toFile, buffer );
            }
        }
    }

    private void copyFile( File from, FileSystemAbstraction fromFs, File to, ByteBuffer buffer ) throws IOException
    {
        try ( StoreChannel source = fromFs.open( from, OpenMode.READ );
              StoreChannel sink = this.open( to, OpenMode.READ_WRITE ) )
        {
            sink.truncate( 0 );
            for ( int available; (available = (int) (source.size() - source.position())) > 0; )
            {
                buffer.clear();
                buffer.limit( min( available, buffer.capacity() ) );
                source.read( buffer );
                buffer.flip();
                sink.write( buffer );
            }
        }
    }

    @Override
    public synchronized <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem(
            Class<K> clazz, Function<Class<K>,K> creator )
    {
        ThirdPartyFileSystem fileSystem = thirdPartyFileSystems.computeIfAbsent( clazz, k -> creator.apply( clazz ) );
        return clazz.cast( fileSystem );
    }

    @Override
    public void truncate( File file, long size ) throws IOException
    {
        EphemeralFileData data = files.get( canonicalFile( file ) );
        if ( data == null )
        {
            throw new FileNotFoundException( "File " + file + " not found" );
        }
        data.truncate( size );
    }

    @Override
    public long lastModifiedTime( File file )
    {
        EphemeralFileData data = files.get( canonicalFile( file ) );
        if ( data == null )
        {
            return 0;
        }
        return data.lastModified;
    }

    @Override
    public void deleteFileOrThrow( File file ) throws IOException
    {
        file = canonicalFile( file );
        if ( !fileExists( file ) )
        {
            throw new NoSuchFileException( file.getAbsolutePath() );
        }
        if ( !deleteFile( file ) )
        {
            throw new IOException( "Could not delete file: " + file );
        }
    }

    @Override
    public Stream<FileHandle> streamFilesRecursive( File directory ) throws IOException
    {
        return StreamFilesRecursive.streamFilesRecursive( directory, this );
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

    static class LocalPosition implements Positionable
    {
        private long position;

        LocalPosition( long position )
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
        private long position;

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

        private void checkIfClosedOrInterrupted() throws IOException
        {
            if ( !isOpen() )
            {
                throw new ClosedChannelException();
            }

            if ( Thread.currentThread().isInterrupted() )
            {
                close();
                throw new ClosedByInterruptException();
            }
        }

        @Override
        public int read( ByteBuffer dst ) throws IOException
        {
            checkIfClosedOrInterrupted();
            return data.read( this, dst );
        }

        @Override
        public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
        {
            checkIfClosedOrInterrupted();
            throw new UnsupportedOperationException();
        }

        @Override
        public int write( ByteBuffer src ) throws IOException
        {
            checkIfClosedOrInterrupted();
            return data.write( this, src );
        }

        @Override
        public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
        {
            checkIfClosedOrInterrupted();
            throw new UnsupportedOperationException();
        }

        @Override
        public long position() throws IOException
        {
            checkIfClosedOrInterrupted();
            return position;
        }

        @Override
        public FileChannel position( long newPosition ) throws IOException
        {
            checkIfClosedOrInterrupted();
            this.position = newPosition;
            return this;
        }

        @Override
        public long size() throws IOException
        {
            checkIfClosedOrInterrupted();
            return data.size();
        }

        @Override
        public FileChannel truncate( long size ) throws IOException
        {
            checkIfClosedOrInterrupted();
            data.truncate( size );
            return this;
        }

        @Override
        public void force( boolean metaData ) throws IOException
        {
            checkIfClosedOrInterrupted();
            // Otherwise no forcing of an in-memory file
            data.force();
        }

        @Override
        public long transferTo( long position, long count, WritableByteChannel target )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferFrom( ReadableByteChannel src, long position, long count ) throws IOException
        {
            checkIfClosedOrInterrupted();
            long previousPos = position();
            position( position );
            try
            {
                long transferred = 0;
                ByteBuffer intermediary = ByteBuffer.allocate( (int) ByteUnit.mebiBytes( 8 ) );
                while ( transferred < count )
                {
                    intermediary.clear();
                    intermediary.limit( (int) min( intermediary.capacity(), count - transferred ) );
                    int read = src.read( intermediary );
                    if ( read == -1 )
                    {
                        break;
                    }
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
            checkIfClosedOrInterrupted();
            return data.read( new LocalPosition( position ), dst );
        }

        @Override
        public int write( ByteBuffer src, long position ) throws IOException
        {
            checkIfClosedOrInterrupted();
            return data.write( new LocalPosition( position ), src );
        }

        @Override
        public MappedByteBuffer map( FileChannel.MapMode mode, long position, long size ) throws IOException
        {
            checkIfClosedOrInterrupted();
            throw new IOException( "Not supported" );
        }

        @Override
        public java.nio.channels.FileLock lock( long position, long size, boolean shared ) throws IOException
        {
            checkIfClosedOrInterrupted();
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
        public java.nio.channels.FileLock tryLock( long position, long size, boolean shared )
        {
            synchronized ( data.channels )
            {
                if ( !data.lock() )
                {
                    throw new OverlappingFileLockException();
                }
                return new EphemeralFileLock( this, data );
            }
        }

        @Override
        protected void implCloseChannel()
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
        private static final ThreadLocal<byte[]> SCRATCH_PAD =
                ThreadLocal.withInitial( () -> new byte[(int) ByteUnit.kibiBytes( 1 )] );
        private DynamicByteBuffer fileAsBuffer;
        private DynamicByteBuffer forcedBuffer;
        private final Collection<WeakReference<EphemeralFileChannel>> channels = new LinkedList<>();
        private int size;
        private int forcedSize;
        private int locked;
        private final Clock clock;
        private long lastModified;

        EphemeralFileData( Clock clock )
        {
            this( new DynamicByteBuffer(), clock );
        }

        private EphemeralFileData( DynamicByteBuffer data, Clock clock )
        {
            this.fileAsBuffer = data;
            this.forcedBuffer = data.copy();
            this.clock = clock;
            this.lastModified = clock.millis();
        }

        int read( Positionable fc, ByteBuffer dst )
        {
            int wanted = dst.limit() - dst.position();
            long size = size();
            int available = min( wanted, (int) (size - fc.pos()) );
            if ( available <= 0 )
            {
                return -1; // EOF
            }
            int pending = available;
            // Read up until our internal size
            byte[] scratchPad = SCRATCH_PAD.get();
            while ( pending > 0 )
            {
                int howMuchToReadThisTime = min( pending, scratchPad.length );
                long pos = fc.pos();
                fileAsBuffer.get( (int) pos, scratchPad, 0, howMuchToReadThisTime );
                fc.pos( pos + howMuchToReadThisTime );
                dst.put( scratchPad, 0, howMuchToReadThisTime );
                pending -= howMuchToReadThisTime;
            }
            return available; // return how much data was read
        }

        synchronized int write( Positionable fc, ByteBuffer src )
        {
            int wanted = src.limit() - src.position();
            int pending = wanted;
            byte[] scratchPad = SCRATCH_PAD.get();

            while ( pending > 0 )
            {
                int howMuchToWriteThisTime = min( pending, scratchPad.length );
                src.get( scratchPad, 0, howMuchToWriteThisTime );
                long pos = fc.pos();
                fileAsBuffer.put( (int) pos, scratchPad, 0, howMuchToWriteThisTime );
                fc.pos( pos + howMuchToWriteThisTime );
                pending -= howMuchToWriteThisTime;
            }

            // If we just made a jump in the file fill the rest of the gap with zeros
            int newSize = max( size, (int) fc.pos() );
            int intermediaryBytes = newSize - wanted - size;
            if ( intermediaryBytes > 0 )
            {
                fileAsBuffer.fillWithZeros( size, intermediaryBytes );
            }

            size = newSize;
            lastModified = clock.millis();
            return wanted;
        }

        synchronized EphemeralFileData copy()
        {
            EphemeralFileData copy = new EphemeralFileData( fileAsBuffer.copy(), clock );
            copy.size = size;
            return copy;
        }

        void free()
        {
            fileAsBuffer.free();
        }

        void open( EphemeralFileChannel channel )
        {
            synchronized ( channels )
            {
                channels.add( new WeakReference<>( channel ) );
            }
        }

        synchronized void force()
        {
            forcedBuffer = fileAsBuffer.copy();
            forcedSize = size;
        }

        synchronized void crash()
        {
            fileAsBuffer = forcedBuffer.copy();
            size = forcedSize;
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
                        if ( channel != null )
                        {
                            return channel;
                        }
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

        synchronized long size()
        {
            return size;
        }

        synchronized void truncate( long newSize )
        {
            this.size = (int) newSize;
        }

        boolean lock()
        {
            return locked == 0;
        }

        synchronized void dumpTo( OutputStream target ) throws IOException
        {
            byte[] scratchPad = SCRATCH_PAD.get();
            fileAsBuffer.dump( target, scratchPad, size );
        }

        @Override
        public String toString()
        {
            return "size: " + size + ", locked:" + locked;
        }
    }

    private static class EphemeralFileLock extends java.nio.channels.FileLock
    {
        private EphemeralFileData file;

        EphemeralFileLock( EphemeralFileChannel channel, EphemeralFileData file )
        {
            super( channel, 0, Long.MAX_VALUE, false );
            this.file = file;
            file.locked++;
        }

        @Override
        public boolean isValid()
        {
            return file != null;
        }

        @Override
        public void release()
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
    static class DynamicByteBuffer
    {
        private static final byte[] zeroBuffer = new byte[(int) ByteUnit.kibiBytes( 1 )];
        private ByteBuffer buf;
        private Exception freeCall;

        DynamicByteBuffer()
        {
            buf = allocate( ByteUnit.kibiBytes( 1 ) );
        }

        public ByteBuffer buf()
        {
            assertNotFreed();
            return buf;
        }

        /** This is a copying constructor, the input buffer is just read from, never stored in 'this'. */
        private DynamicByteBuffer( ByteBuffer toClone )
        {
            buf = allocate( toClone.capacity() );
            copyByteBufferContents( toClone, buf );
        }

        synchronized DynamicByteBuffer copy()
        {
            return new DynamicByteBuffer( buf() ); // invoke "copy constructor"
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

        private ByteBuffer allocate( long capacity )
        {
            return ByteBuffer.allocate( Math.toIntExact( capacity ) );
        }

        void free()
        {
            assertNotFreed();
            try
            {
                clear();
            }
            finally
            {
                buf = null;
                freeCall = new Exception(
                        "You're most likely seeing this exception because there was an attempt to use this buffer " +
                        "after it was freed. This stack trace may help you figure out where and why it was freed" );
            }
        }

        synchronized void put( int pos, byte[] bytes, int offset, int length )
        {
            verifySize( pos + length );
            ByteBuffer buf = buf();
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
            ByteBuffer buf = buf();
            buf.position( pos );
            buf.get( scratchPad, i, howMuchToReadThisTime );
        }

        synchronized void fillWithZeros( int pos, int bytes )
        {
            ByteBuffer buf = buf();
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
            ByteBuffer buf = buf();
            if ( buf.capacity() >= totalAmount )
            {
                return;
            }

            int newSize = buf.capacity();
            long maxSize = ByteUnit.gibiBytes( 1 );
            checkAllowedSize( totalAmount, maxSize );
            while ( newSize < totalAmount )
            {
                newSize = newSize << 1;
                checkAllowedSize( newSize, maxSize );
            }
            int oldPosition = buf.position();

            // allocate new buffer
            ByteBuffer newBuf = allocate( newSize );

            // copy contents of current buffer into new buffer
            buf.position( 0 );
            newBuf.put( buf );

            // re-assign buffer to new buffer
            newBuf.position( oldPosition );
            this.buf = newBuf;
        }

        private void checkAllowedSize( long size, long maxSize )
        {
            if ( size > maxSize )
            {
                throw new RuntimeException( "Requested file size is too big for ephemeral file system." );
            }
        }

        public void clear()
        {
            buf().clear();
        }

        private void assertNotFreed()
        {
            if ( this.buf == null )
            {
                throw new IllegalStateException( "This buffer have been freed", freeCall );
            }
        }

        void dump( OutputStream target, byte[] scratchPad, int size ) throws IOException
        {
            ByteBuffer buf = buf();
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

    // Copied from kernel since we don't want to depend on that module here
    private abstract static class PrefetchingIterator<T> implements Iterator<T>
    {
        boolean hasFetchedNext;
        T nextObject;

        /**
         * @return {@code true} if there is a next item to be returned from the next
         * call to {@link #next()}.
         */
        @Override
        public boolean hasNext()
        {
            return peek() != null;
        }

        /**
         * @return the next element that will be returned from {@link #next()} without
         * actually advancing the iterator
         */
        public T peek()
        {
            if ( hasFetchedNext )
            {
                return nextObject;
            }

            nextObject = fetchNextOrNull();
            hasFetchedNext = true;
            return nextObject;
        }

        /**
         * Uses {@link #hasNext()} to try to fetch the next item and returns it
         * if found, otherwise it throws a {@link java.util.NoSuchElementException}.
         *
         * @return the next item in the iteration, or throws
         * {@link java.util.NoSuchElementException} if there's no more items to return.
         */
        @Override
        public T next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException();
            }
            T result = nextObject;
            nextObject = null;
            hasFetchedNext = false;
            return result;
        }

        protected abstract T fetchNextOrNull();

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class CombiningIterator<T> extends PrefetchingIterator<T>
    {
        private Iterator<? extends Iterator<T>> iterators;
        private Iterator<T> currentIterator;

        CombiningIterator( Iterable<? extends Iterator<T>> iterators )
        {
            this( iterators.iterator() );
        }

        CombiningIterator( Iterator<? extends Iterator<T>> iterators )
        {
            this.iterators = iterators;
        }

        @Override
        protected T fetchNextOrNull()
        {
            if ( currentIterator == null || !currentIterator.hasNext() )
            {
                while ( (currentIterator = nextIteratorOrNull()) != null )
                {
                    if ( currentIterator.hasNext() )
                    {
                        break;
                    }
                }
            }
            return currentIterator != null && currentIterator.hasNext() ? currentIterator.next() : null;
        }

        protected Iterator<T> nextIteratorOrNull()
        {
            if ( iterators.hasNext() )
            {
                return iterators.next();
            }
            return null;
        }
    }
}
