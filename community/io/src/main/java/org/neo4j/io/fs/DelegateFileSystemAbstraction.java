/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.io.fs;

import java.io.File;
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
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

import org.neo4j.io.fs.watcher.DefaultFileSystemWatcher;
import org.neo4j.io.fs.watcher.FileWatcher;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * This FileSystemAbstract implementation delegates all calls to a given {@link FileSystem} implementation.
 * This is useful for testing with arbitrary 3rd party file systems, such as Jimfs.
 */
public class DelegateFileSystemAbstraction implements FileSystemAbstraction
{
    private final FileSystem fs;

    public DelegateFileSystemAbstraction( FileSystem fs )
    {
        this.fs = fs;
    }

    @Override
    public FileWatcher fileWatcher() throws IOException
    {
        return new DefaultFileSystemWatcher( fs.newWatchService() );
    }

    @Override
    public StoreChannel open( File fileName, OpenMode openMode ) throws IOException
    {
        return new StoreFileChannel( FileUtils.open( path( fileName ), openMode ) );
    }

    private Path path( File fileName )
    {
        return path( fileName.getPath() );
    }

    private Path path( String fileName )
    {
        return fs.getPath( fileName );
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return FileUtils.openAsOutputStream( path( fileName ), append );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return FileUtils.openAsInputStream( path( fileName ) );
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
    public StoreChannel create( File fileName ) throws IOException
    {
        return open( fileName, OpenMode.READ_WRITE );
    }

    @Override
    public boolean fileExists( File fileName )
    {
        return Files.exists( path( fileName ) );
    }

    @Override
    public boolean mkdir( File fileName )
    {
        if ( !fileExists( fileName ) )
        {
            try
            {
                Files.createDirectory( path( fileName ) );
                return true;
            }
            catch ( IOException ignore )
            {
            }
        }
        return false;
    }

    @Override
    public void mkdirs( File fileName ) throws IOException
    {
        Files.createDirectories( path( fileName ) );
    }

    @Override
    public long getFileSize( File fileName )
    {
        try
        {
            return Files.size( path( fileName ) );
        }
        catch ( IOException e )
        {
            return 0;
        }
    }

    @Override
    public boolean deleteFile( File fileName )
    {
        try
        {
            Files.delete( path( fileName ) );
            return true;
        }
        catch ( IOException e )
        {
            return false;
        }
    }

    @Override
    public void deleteRecursively( File directory ) throws IOException
    {
        if ( fileExists( directory ) )
        {
            FileUtils.deletePathRecursively( path( directory ) );
        }
    }

    @Override
    public void renameFile( File from, File to, CopyOption... copyOptions ) throws IOException
    {
        Files.move( path( from ), path( to ), copyOptions );
    }

    @Override
    public File[] listFiles( File directory )
    {
        try ( Stream<Path> listing = Files.list( path( directory ) ) )
        {
            return listing.map( Path::toFile ).toArray( File[]::new );
        }
        catch ( IOException e )
        {
            return null;
        }
    }

    @Override
    public File[] listFiles( File directory, final FilenameFilter filter )
    {
        try ( Stream<Path> listing = Files.list( path( directory ) ) )
        {
            return listing
                    .filter( entry -> filter.accept( entry.getParent().toFile(), entry.getFileName().toString() ) )
                    .map( Path::toFile ).toArray( File[]::new );
        }
        catch ( IOException e )
        {
            return null;
        }
    }

    @Override
    public boolean isDirectory( File file )
    {
        return Files.isDirectory( path( file ) );
    }

    @Override
    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        Files.move( path( file ), path( toDirectory ).resolve( path( file.getName() ) ) );
    }

    @Override
    public void copyToDirectory( File file, File toDirectory ) throws IOException
    {
        Files.copy( path( file ), path( toDirectory ).resolve( file.getName() ), REPLACE_EXISTING );
    }

    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        Files.copy( path( from ), path( to ) );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        Path target = path( toDirectory );
        Path source = path( fromDirectory );
        copyRecursively( source, target );
    }

    private void copyRecursively( Path source, Path target ) throws IOException
    {
        try ( DirectoryStream<Path> directoryStream = Files.newDirectoryStream( source ) )
        {
            for ( Path sourcePath : directoryStream )
            {
                Path targetPath = target.resolve( sourcePath.getFileName() );
                if ( Files.isDirectory( sourcePath ) )
                {
                    Files.createDirectories( targetPath );
                    copyRecursively( sourcePath, targetPath );
                }
                else
                {
                    Files.copy( sourcePath, targetPath, REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES );
                }
            }
        }
    }

    @Override
    public void truncate( File path, long size ) throws IOException
    {
        try ( FileChannel channel = FileChannel.open( path( path ) ) )
        {
            channel.truncate( size );
        }
    }

    @Override
    public long lastModifiedTime( File file ) throws IOException
    {
        return Files.getLastModifiedTime( path( file ) ).toMillis();
    }

    @Override
    public void deleteFileOrThrow( File file ) throws IOException
    {
        Files.delete( path( file ) );
    }

    @Override
    public Stream<FileHandle> streamFilesRecursive( File directory ) throws IOException
    {
        return StreamFilesRecursive.streamFilesRecursive( directory, this );
    }

    @Override
    public void close() throws IOException
    {
        fs.close();
    }

    @Override
    public void force( File path ) throws IOException
    {
        OpenMode openMode = path.isDirectory() ? OpenMode.READ : OpenMode.READ_WRITE;
        try ( StoreChannel ch = this.open( path, openMode ) )
        {
            ch.force( true );
        }
    }
}
