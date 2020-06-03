/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.neo4j.graphdb.Resource;
import org.neo4j.internal.helpers.collection.CombiningIterator;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.test.impl.ChannelInputStream;
import org.neo4j.test.impl.ChannelOutputStream;

import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Arrays.asList;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.UNABLE_TO_CREATE_DIRECTORY_FORMAT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public class EphemeralFileSystemAbstraction implements FileSystemAbstraction
{
    private final Clock clock;
    private final AtomicInteger keepFiles = new AtomicInteger();
    private final Set<File> directories = ConcurrentHashMap.newKeySet();
    private final Map<File,EphemeralFileData> files;
    private volatile boolean closed;

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
            throw new UncheckedIOException( "EphemeralFileSystemAbstraction could not initialise current working directory", e );
        }
    }

    private EphemeralFileSystemAbstraction( Set<File> directories, Map<File,EphemeralFileData> files, Clock clock )
    {
        this.clock = clock;
        this.files = new ConcurrentHashMap<>( files );
        this.directories.addAll( directories );
        initCurrentWorkingDirectory();
    }

    public void clear()
    {
        closeFiles();
    }

    /**
     * Simulate a filesystem crash, in which any changes that have not been {@link StoreChannel#force}d
     * will be lost. Practically, all files revert to the state when they are last {@link StoreChannel#force}d.
     */
    public void crash()
    {
        files.values().forEach( EphemeralFileData::crash );
    }

    public Resource keepFiles()
    {
        keepFiles.getAndIncrement();
        return keepFiles::decrementAndGet;
    }

    @Override
    public synchronized void close() throws IOException
    {
        if ( keepFiles.get() > 0 )
        {
            return;
        }
        closeFiles();
        closed = true;
    }

    public boolean isClosed()
    {
        return closed;
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
        Exception exception = null;
        for ( EphemeralFileData file : files.values() )
        {
            Iterator<EphemeralFileChannel> channels = file.getOpenChannels();
            while ( channels.hasNext() )
            {
                EphemeralFileChannel channel = channels.next();
                if ( exception == null )
                {
                    exception = new IOException( "Expected no open files. " +
                            "The stack traces of the currently open files are attached as suppressed exceptions." );
                }
                exception.addSuppressed( channel.openedAt );
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
    }

    @Override
    public FileWatcher fileWatcher()
    {
        return FileWatcher.SILENT_WATCHER;
    }

    @Override
    public synchronized StoreChannel open( File fileName, Set<OpenOption> options ) throws IOException
    {
        return getStoreChannel( fileName );
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return new ChannelOutputStream( write( fileName ), append, INSTANCE );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return new ChannelInputStream( read( fileName), INSTANCE );
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
    public synchronized StoreChannel write( File fileName ) throws IOException
    {
        File parentFile = fileName.getParentFile();
        if ( parentFile != null /*means that this is the 'default location'*/ && !fileExists( parentFile ) )
        {
            throw new FileNotFoundException( "'" + fileName + "' (The system cannot find the path specified)" );
        }

        EphemeralFileData data = files.computeIfAbsent( canonicalFile( fileName ), key -> new EphemeralFileData( key, clock ) );
        return new StoreFileChannel( new EphemeralFileChannel( data, new EphemeralFileStillOpenException( fileName.getPath() ) ) );
    }

    @Override
    public synchronized StoreChannel read( File fileName ) throws IOException
    {
        return getStoreChannel( fileName );
    }

    @Override
    public long getFileSize( File fileName )
    {
        EphemeralFileData file = files.get( canonicalFile( fileName ) );
        return file == null ? 0 : file.size();
    }

    @Override
    public long getBlockSize( File file )
    {
        return 512;
    }

    @Override
    public boolean fileExists( File file )
    {
        file = canonicalFile( file );
        return directories.contains( file ) || files.containsKey( file );
    }

    private static File canonicalFile( File file )
    {
        try
        {
            return file.getCanonicalFile();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "EphemeralFileSystemAbstraction could not canonicalise file: " + file, e );
        }
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
    public void mkdirs( File directory ) throws IOException
    {
        File currentDirectory = canonicalFile( directory );

        while ( currentDirectory != null )
        {
            if ( files.containsKey( currentDirectory ) )
            {
                throw new IOException( format( UNABLE_TO_CREATE_DIRECTORY_FORMAT, currentDirectory ) );
            }
            else
            {
                mkdir( currentDirectory );
            }
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
            File[] fileList = listFiles( fileName );
            return fileList != null && fileList.length == 0 && directories.remove( fileName );
        }
    }

    @Override
    public void deleteRecursively( File path )
    {
        if ( isDirectory( path ) )
        {
            // Delete all files in directory and sub-directory
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

            // Delete all sub-directories
            for ( File subDirectory : directories )
            {
                List<String> subDirectoryPathItems = splitPath( subDirectory );
                if ( directoryMatches( directoryPathItems, subDirectoryPathItems ) )
                {
                    deleteFile( subDirectory );
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
        Iterator<File> filesAndFolders = new CombiningIterator<>( asList( this.files.keySet().iterator(), directories.iterator() ) );
        while ( filesAndFolders.hasNext() )
        {
            File file = filesAndFolders.next();
            List<String> fileNamePathItems = splitPath( file );
            if ( directoryMatches( directoryPathItems, fileNamePathItems ) )
            {
                found.add( constructPath( fileNamePathItems, directoryPathItems ) );
            }
        }

        return found.toArray( new File[0] );
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
        return found.toArray( new File[0] );
    }

    private static File constructPath( List<String> pathItems, List<String> base )
    {
        File file = null;
        if ( !base.isEmpty() )
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

    private StoreChannel getStoreChannel( File fileName ) throws IOException
    {
        EphemeralFileData data = files.get( canonicalFile( fileName ) );
        if ( data != null )
        {
            return new StoreFileChannel( new EphemeralFileChannel( data, new EphemeralFileStillOpenException( fileName.getPath() ) ) );
        }
        return write( fileName );
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
    public void copyFile( File from, File to, CopyOption... copyOptions ) throws IOException
    {
        EphemeralFileData data = files.get( canonicalFile( from ) );
        if ( data == null )
        {
            throw new FileNotFoundException( "File " + from + " not found" );
        }
        if ( !ArrayUtils.contains( copyOptions, REPLACE_EXISTING ) && files.get( canonicalFile( from ) ) != null )
        {
            throw new FileAlreadyExistsException( to.getAbsolutePath() );
        }
        copyFile( from, this, to, newCopyBuffer() );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        copyRecursivelyFromOtherFs( fromDirectory, this, toDirectory, newCopyBuffer() );
    }

    public synchronized EphemeralFileSystemAbstraction snapshot()
    {
        Map<File,EphemeralFileData> copiedFiles = new HashMap<>();
        for ( Map.Entry<File,EphemeralFileData> file : files.entrySet() )
        {
            copiedFiles.put( file.getKey(), file.getValue().copy() );
        }
        return new EphemeralFileSystemAbstraction( directories, copiedFiles, clock );
    }

    private void copyRecursivelyFromOtherFs( File from, FileSystemAbstraction fromFs, File to ) throws IOException
    {
        copyRecursivelyFromOtherFs( from, fromFs, to, newCopyBuffer() );
    }

    private static ByteBuffer newCopyBuffer()
    {
        return ByteBuffers.allocate( 1, ByteUnit.MebiByte, INSTANCE );
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
        try ( StoreChannel source = fromFs.read( from );
              StoreChannel sink = this.write( to ) )
        {
            sink.truncate( 0 );
            long sourceSize = source.size();
            for ( int available; (available = (int) (sourceSize - source.position())) > 0; )
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
        return data.getLastModified();
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

    @Override
    public int getFileDescriptor( StoreChannel channel )
    {
        return INVALID_FILE_DESCRIPTOR;
    }
}
