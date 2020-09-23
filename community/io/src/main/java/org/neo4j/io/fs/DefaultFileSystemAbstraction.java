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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.io.fs.watcher.DefaultFileSystemWatcher;
import org.neo4j.io.fs.watcher.FileWatcher;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Default file system abstraction that creates files using the underlying file system.
 */
public class DefaultFileSystemAbstraction implements FileSystemAbstraction
{
    static final String UNABLE_TO_CREATE_DIRECTORY_FORMAT = "Unable to write directory path [%s] for Neo4j store.";
    public static final Set<OpenOption> WRITE_OPTIONS = Set.of( READ, WRITE, CREATE );
    private static final Set<OpenOption> READ_OPTIONS = Set.of( READ );
    private static final OpenOption[] APPEND_OPTIONS = new OpenOption[]{CREATE, APPEND};
    private static final OpenOption[] DEFAULT_OUTPUT_OPTIONS = new OpenOption[0];

    @Override
    public FileWatcher fileWatcher() throws IOException
    {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        return new DefaultFileSystemWatcher( watchService );
    }

    @Override
    public StoreFileChannel open( Path fileName, Set<OpenOption> options ) throws IOException
    {
        FileChannel channel = FileChannel.open( fileName, options );
        return getStoreFileChannel( channel );
    }

    @Override
    public OutputStream openAsOutputStream( Path fileName, boolean append ) throws IOException
    {
        return new BufferedOutputStream( openFileOutputStream( fileName, append ) );
    }

    @Override
    public InputStream openAsInputStream( Path fileName ) throws IOException
    {
        return new BufferedInputStream( openFileInputStream( fileName ) );
    }

    @Override
    public Reader openAsReader( Path fileName, Charset charset ) throws IOException
    {
        return new BufferedReader( new InputStreamReader( openFileInputStream( fileName ), charset ) );
    }

    @Override
    public Writer openAsWriter( Path fileName, Charset charset, boolean append ) throws IOException
    {
        return new BufferedWriter( new OutputStreamWriter( openFileOutputStream( fileName, append ), charset ) );
    }

    @Override
    public StoreFileChannel write( Path fileName ) throws IOException
    {
        return open( fileName, WRITE_OPTIONS );
    }

    @Override
    public StoreFileChannel read( Path fileName ) throws IOException
    {
        return open( fileName, READ_OPTIONS );
    }

    @Override
    public boolean mkdir( Path fileName )
    {
        try
        {
            Files.createDirectories( fileName );
        }
        catch ( IOException e )
        {
            return false;
        }
        return true;
    }

    @Override
    public void mkdirs( Path file ) throws IOException
    {
        if ( Files.exists( file ) && Files.isDirectory( file ) )
        {
            return;
        }

        try
        {
            Files.createDirectories( file );
        }
        catch ( IOException e )
        {
            throw new IOException( format( UNABLE_TO_CREATE_DIRECTORY_FORMAT, file ), e );
        }
    }

    @Override
    public boolean fileExists( Path file )
    {
        return Files.exists( file );
    }

    /**
     * @return 0 on IO exception to preserve behaviour of {@link File#length()}
     */
    @Override
    public long getFileSize( Path file )
    {
        try
        {
            return Files.size( file );
        }
        catch ( IOException e )
        {
            return 0;
        }
    }

    @Override
    public long getBlockSize( Path file ) throws IOException
    {
        return FileUtils.blockSize( file );
    }

    @Override
    public boolean deleteFile( Path fileName )
    {
        try
        {
            FileUtils.deleteFile( fileName );
            return true;
        }
        catch ( IOException e )
        {
            return false;
        }
    }

    @Override
    public void deleteRecursively( Path directory ) throws IOException
    {
        FileUtils.deleteDirectory( directory );
    }

    @Override
    public void renameFile( Path from, Path to, CopyOption... copyOptions ) throws IOException
    {
        Files.move( from, to, copyOptions );
    }

    @Override
    public Path[] listFiles( Path directory )
    {
        try
        {
            try ( Stream<Path> list = Files.list( directory ) )
            {
                return list.toArray( Path[]::new );
            }
        }
        catch ( IOException ignored )
        {
            return null; // Preserve behaviour of File.listFiles()
        }
    }

    @Override
    public Path[] listFiles( Path directory, DirectoryStream.Filter<Path> filter )
    {
        try
        {
            try ( DirectoryStream<Path> paths = Files.newDirectoryStream( directory, filter ) )
            {
                return StreamSupport.stream( paths.spliterator(), false ).toArray( Path[]::new );
            }
        }
        catch ( IOException ignored )
        {
            return null; // Preserve behaviour of File.listFiles()
        }
    }

    @Override
    public boolean isDirectory( Path file )
    {
        return Files.isDirectory( file );
    }

    @Override
    public void moveToDirectory( Path file, Path toDirectory ) throws IOException
    {
        FileUtils.moveFileToDirectory( file, toDirectory );
    }

    @Override
    public void copyToDirectory( Path file, Path toDirectory ) throws IOException
    {
        FileUtils.copyFileToDirectory( file, toDirectory );
    }

    @Override
    public void copyFile( Path from, Path to ) throws IOException
    {
        FileUtils.copyFile( from, to );
    }

    @Override
    public void copyFile( Path from, Path to, CopyOption... copyOptions ) throws IOException
    {
        FileUtils.copyFile( from, to, copyOptions );
    }

    @Override
    public void copyRecursively( Path fromDirectory, Path toDirectory ) throws IOException
    {
        FileUtils.copyDirectory( fromDirectory, toDirectory );
    }

    @Override
    public void truncate( Path path, long size ) throws IOException
    {
        FileUtils.truncateFile( path, size );
    }

    /**
     * @return 0 on IO exception to preserve behaviour of {@link File#lastModified()}
     */
    @Override
    public long lastModifiedTime( Path file )
    {
        try
        {
            return Files.getLastModifiedTime( file ).toMillis();
        }
        catch ( IOException e )
        {
            return 0;
        }
    }

    @Override
    public void deleteFileOrThrow( Path file ) throws IOException
    {
        Files.delete( file );
    }

    @Override
    public Stream<FileHandle> streamFilesRecursive( Path directory ) throws IOException
    {
        return StreamFilesRecursive.streamFilesRecursive( directory, this );
    }

    @Override
    public int getFileDescriptor( StoreChannel channel )
    {
        return channel.getFileDescriptor();
    }

    @Override
    public void close() throws IOException
    {
        // nothing
    }

    protected StoreFileChannel getStoreFileChannel( FileChannel channel )
    {
        return new StoreFileChannel( channel );
    }

    private static InputStream openFileInputStream( Path fileName ) throws IOException
    {
        return Files.newInputStream( fileName );
    }

    private OutputStream openFileOutputStream( Path fileName, boolean append ) throws IOException
    {
        return Files.newOutputStream( fileName, append ? APPEND_OPTIONS : DEFAULT_OUTPUT_OPTIONS );
    }
}
