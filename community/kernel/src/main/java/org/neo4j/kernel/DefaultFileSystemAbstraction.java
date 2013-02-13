/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.nio.channels.FileChannel;

import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.FileUtils;

/**
* Default file system abstraction that creates files using the underlying file system.
*/
public class DefaultFileSystemAbstraction
    implements FileSystemAbstraction
{
    @Override
    public FileChannel open( File fileName, String mode ) throws IOException
    {
        // Returning only the channel is ok, because the channel, when close()d will close its parent File.
        return new RandomAccessFile( fileName, mode ).getChannel();
    }
    
    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return new BufferedOutputStream( new FileOutputStream( fileName, append ) );
    }
    
    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return new BufferedInputStream( new FileInputStream( fileName ) );
    }
    
    @Override
    public Reader openAsReader( File fileName, String encoding ) throws IOException
    {
        return new InputStreamReader( new FileInputStream( fileName ), encoding );
    }

    @Override
    public FileLock tryLock( File fileName, FileChannel channel ) throws IOException
    {
        return FileLock.getOsSpecificFileLock( fileName, channel );
    }

    @Override
    public FileChannel create( File fileName ) throws IOException
    {
        return open( fileName, "rw" );
    }
    
    @Override
    public boolean mkdir( File fileName )
    {
        return fileName.mkdir();
    }
    
    @Override
    public boolean mkdirs( File fileName )
    {
        return fileName.mkdirs();
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
    public void autoCreatePath( File path ) throws IOException
    {
        if (!path.isDirectory())
            path = path.getParentFile();

        if ( !path.exists() )
        {
            if ( !path.mkdirs() )
            {
                throw new IOException( "Unable to create directory path["
                        + path + "] for Neo4j store." );
            }
        }
    }
    
    @Override
    public File[] listFiles( File directory )
    {
        return directory.listFiles();
    }
    
    @Override
    public boolean isDirectory( File file )
    {
        return file.isDirectory();
    }
}
