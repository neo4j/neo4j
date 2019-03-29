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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
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
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.WatchService;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.io.fs.watcher.DefaultFileSystemWatcher;
import org.neo4j.io.fs.watcher.FileWatcher;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Default file system abstraction that creates files using the underlying file system.
 */
public class DefaultFileSystemAbstraction implements FileSystemAbstraction
{
    static final String UNABLE_TO_CREATE_DIRECTORY_FORMAT = "Unable to write directory path [%s] for Neo4j store.";
    private static final Set<OpenOption> WRITE_OPTIONS = Set.of( READ, WRITE, CREATE );
    private static final Set<OpenOption> READ_OPTIONS = Set.of( READ );

    @Override
    public FileWatcher fileWatcher() throws IOException
    {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        return new DefaultFileSystemWatcher( watchService );
    }

    @Override
    public StoreFileChannel open( File fileName, Set<OpenOption> options ) throws IOException
    {
        FileChannel channel = FileChannel.open( fileName.toPath(), options );
        return getStoreFileChannel( channel );
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return new FileOutputStream( fileName, append );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return new FileInputStream( fileName );
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
    public StoreFileChannel write( File fileName ) throws IOException
    {
        return open( fileName, WRITE_OPTIONS );
    }

    @Override
    public StoreFileChannel read( File fileName ) throws IOException
    {
        return open( fileName, READ_OPTIONS );
    }

    @Override
    public boolean mkdir( File fileName )
    {
        return fileName.mkdir();
    }

    @Override
    public void mkdirs( File path ) throws IOException
    {
        if ( path.exists() )
        {
            return;
        }

        path.mkdirs();

        if ( path.exists() )
        {
            return;
        }

        throw new IOException( format( UNABLE_TO_CREATE_DIRECTORY_FORMAT, path ) );
    }

    @Override
    public boolean fileExists( File file )
    {
        return file.exists();
    }

    @Override
    public long getFileSize( File file )
    {
        return file.length();
    }

    @Override
    public boolean deleteFile( File fileName )
    {
        return FileUtils.deleteFile( fileName );
    }

    @Override
    public void deleteRecursively( File directory ) throws IOException
    {
        FileUtils.deleteRecursively( directory );
    }

    @Override
    public void renameFile( File from, File to, CopyOption... copyOptions ) throws IOException
    {
        Files.move( from.toPath(), to.toPath(), copyOptions );
    }

    @Override
    public File[] listFiles( File directory )
    {
        return directory.listFiles();
    }

    @Override
    public File[] listFiles( File directory, FilenameFilter filter )
    {
        return directory.listFiles( filter );
    }

    @Override
    public boolean isDirectory( File file )
    {
        return file.isDirectory();
    }

    @Override
    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        FileUtils.moveFileToDirectory( file, toDirectory );
    }

    @Override
    public void copyToDirectory( File file, File toDirectory ) throws IOException
    {
        FileUtils.copyFileToDirectory( file, toDirectory );
    }

    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        FileUtils.copyFile( from, to );
    }

    @Override
    public void copyFile( File from, File to, CopyOption... copyOptions ) throws IOException
    {
        FileUtils.copyFile( from, to, copyOptions );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        FileUtils.copyRecursively( fromDirectory, toDirectory );
    }

    @Override
    public void truncate( File path, long size ) throws IOException
    {
        FileUtils.truncateFile( path, size );
    }

    @Override
    public long lastModifiedTime( File file )
    {
        return file.lastModified();
    }

    @Override
    public void deleteFileOrThrow( File file ) throws IOException
    {
        Files.delete( file.toPath() );
    }

    @Override
    public Stream<FileHandle> streamFilesRecursive( File directory ) throws IOException
    {
        return StreamFilesRecursive.streamFilesRecursive( directory, this );
    }

    protected StoreFileChannel getStoreFileChannel( FileChannel channel )
    {
        return new StoreFileChannel( channel );
    }

    @Override
    public void close() throws IOException
    {
        // nothing
    }
}
