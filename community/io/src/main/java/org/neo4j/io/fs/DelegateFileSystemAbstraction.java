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
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.function.Function;

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
    public StoreChannel open( File fileName, String mode ) throws IOException
    {
        return new StoreFileChannel( FileUtils.open( path( fileName ), mode ) );
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
    public StoreChannel create( File fileName ) throws IOException
    {
        return open( fileName, "rw" );
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
    public boolean renameFile( File from, File to ) throws IOException
    {
        Files.move( path( from ), path( to ) );
        return true;
    }

    @Override
    public File[] listFiles( File directory )
    {
        List<File> files = new ArrayList<>();
        try
        {
            for ( Path path : Files.newDirectoryStream( path( directory ) ) )
            {
                files.add( path.toFile() );
            }
        }
        catch ( IOException e )
        {
            return null;
        }
        return files.toArray( new File[files.size()] );
    }

    @Override
    public File[] listFiles( File directory, final FilenameFilter filter )
    {
        List<File> files = new ArrayList<>();
        try
        {
            DirectoryStream.Filter<Path> dirfilter = new DirectoryStream.Filter<Path>()
            {
                @Override
                public boolean accept( Path entry ) throws IOException
                {
                    return filter.accept( entry.getParent().toFile(), entry.getFileName().toString() );
                }
            };
            for ( Path path : Files.newDirectoryStream( path( directory ), dirfilter ) )
            {
                files.add( path.toFile() );
            }
        }
        catch ( IOException e )
        {
            return null;
        }
        return files.toArray( new File[files.size()] );
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
        for ( Path sourcePath : Files.newDirectoryStream( source ) )
        {
            Path targetPath = target.resolve( sourcePath.getFileName() );
            if ( Files.isDirectory( sourcePath ) )
            {
                Files.createDirectories( targetPath );
                copyRecursively( sourcePath, targetPath );
            }
            else
            {
                Files.copy( sourcePath, targetPath,
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES );
            }
        }
    }

    private final Map<Class<?>,Object> thirdPartyFs = new HashMap<>();

    @Override
    public synchronized <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem(
            Class<K> clazz, Function<Class<K>,K> creator )
    {
        // what in the ever-loving mother of the lake is this!?
        K otherFs = (K) thirdPartyFs.get( clazz );
        if ( otherFs == null )
        {
            otherFs = creator.apply( clazz );
            thirdPartyFs.put( clazz, otherFs );
        }
        return otherFs;
    }

    @Override
    public void truncate( File path, long size ) throws IOException
    {
        try ( FileChannel channel = FileChannel.open( path( path ) ) )
        {
            channel.truncate( size );
        }
    }
}
