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

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.FileUtils;

/**
* TODO
*/
public class DefaultFileSystemAbstraction implements FileSystemAbstraction
{
    static String UNABLE_TO_CREATE_DIRECTORY_FORMAT = "Unable to create directory path [%s] for Neo4j store.";

    @Override
    public FileChannel open( String fileName, String mode ) throws IOException
    {
        // Returning only the channel is ok, because the channel, when close()d will close its parent File.
        return new RandomAccessFile( fileName, mode ).getChannel();
    }

    @Override
    public FileLock tryLock( String fileName, FileChannel channel ) throws IOException
    {
        return FileLock.getOsSpecificFileLock( fileName, channel );
    }

    @Override
    public FileChannel create( String fileName ) throws IOException
    {
        return open( fileName, "rw" );
    }

    @Override
    public boolean fileExists( String fileName )
    {
        return new File( fileName ).exists();
    }

    @Override
    public long getFileSize( String fileName )
    {
        return new File( fileName ).length();
    }

    @Override
    public boolean deleteFile( String fileName )
    {
        return FileUtils.deleteFile( new File( fileName ) );
    }

    @Override
    public boolean renameFile( String from, String to ) throws IOException
    {
        return FileUtils.renameFile( new File( from ), new File( to ) );
    }
    
    @Override
    public void copyFile( String from, String to ) throws IOException
    {
        FileUtils.copyRecursively( new File( from ), new File( to ) );
    }

    @Override
    public void autoCreatePath( File path ) throws IOException
    {
        if (path.exists()) return;

        boolean directoriesWereCreated = path.mkdirs();

        if (directoriesWereCreated) return;

        throw new IOException( format( UNABLE_TO_CREATE_DIRECTORY_FORMAT, path ) );
    }
}
