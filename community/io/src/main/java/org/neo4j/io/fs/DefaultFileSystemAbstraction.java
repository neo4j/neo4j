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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.Function;

import static java.lang.String.format;

/**
 * Default file system abstraction that creates files using the underlying file system.
 */
public class DefaultFileSystemAbstraction
        implements FileSystemAbstraction
{
    static final String UNABLE_TO_CREATE_DIRECTORY_FORMAT = "Unable to create directory path [%s] for Neo4j store.";

    @Override
    public StoreFileChannel open( File fileName, String mode ) throws IOException
    {
        // Returning only the channel is ok, because the channel, when close()d will close its parent File.
        FileChannel channel = new RandomAccessFile( fileName, mode ).getChannel();
        return new StoreFileChannel( channel );
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
    public Reader openAsReader( File fileName, String encoding ) throws IOException
    {
        return new InputStreamReader( new FileInputStream( fileName ), encoding );
    }

    @Override
    public Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException
    {
        return new OutputStreamWriter( new FileOutputStream( fileName, append ), encoding );
    }

    @Override
    public StoreFileChannel create( File fileName ) throws IOException
    {
        return open( fileName, "rw" );
    }

    @Override
    public boolean mkdir( File fileName )
    {
        return fileName.mkdir();
    }

    @Override
    public void mkdirs( File path ) throws IOException
    {
        if (path.exists())
        {
            return;
        }

        boolean directoriesWereCreated = path.mkdirs();

        if (directoriesWereCreated)
        {
            return;
        }

        throw new IOException( format( UNABLE_TO_CREATE_DIRECTORY_FORMAT, path ) );
    }

    @Override
    public boolean fileExists( File fileName )
    {
        return fileName.exists();
    }

    @Override
    public long getFileSize( File fileName )
    {
        return fileName.length();
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
    public boolean renameFile( File from, File to ) throws IOException
    {
        return FileUtils.renameFile( from, to );
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
    public void copyFile( File from, File to ) throws IOException
    {
        FileUtils.copyFile( from, to );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        FileUtils.copyRecursively( fromDirectory, toDirectory );
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

    @Override
    public void truncate( File path, long size ) throws IOException
    {
        FileUtils.truncateFile( path, size );
    }
}
