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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;

import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

public class CannedFileSystemAbstraction implements FileSystemAbstraction
{
    private final boolean fileExists;
    private final IOException cannotCreateStoreDir;
    private final IOException cannotOpenLockFile;
    private final boolean lockSuccess;

    public CannedFileSystemAbstraction( boolean fileExists,
                                        IOException cannotCreateStoreDir,
                                        IOException cannotOpenLockFile,
                                        boolean lockSuccess )
    {
        this.fileExists = fileExists;
        this.cannotCreateStoreDir = cannotCreateStoreDir;
        this.cannotOpenLockFile = cannotOpenLockFile;
        this.lockSuccess = lockSuccess;
    }

    @Override
    public FileChannel open( File fileName, String mode ) throws IOException
    {
        if ( cannotOpenLockFile != null )
        {
            throw cannotOpenLockFile;
        }

        return null;
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public Reader openAsReader( File fileName, String encoding ) throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }
    
    @Override
    public Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public FileLock tryLock( File fileName, FileChannel channel ) throws IOException
    {
        return lockSuccess ? SYMBOLIC_FILE_LOCK : null;
    }

    @Override
    public FileChannel create( File fileName ) throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public boolean fileExists( File fileName )
    {
        return fileExists;
    }

    @Override
    public boolean mkdir( File fileName )
    {
        return false;
    }

    @Override
    public boolean mkdirs( File fileName )
    {
        return false;
    }

    @Override
    public long getFileSize( File fileName )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public boolean deleteFile( File fileName )
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void deleteRecursively( File directory ) throws IOException
    {
    }

    @Override
    public boolean renameFile( File from, File to ) throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public boolean isDirectory( File file )
    {
        return false;
    }

    @Override
    public void autoCreatePath( File path ) throws IOException
    {
        if ( cannotCreateStoreDir != null )
        {
            throw cannotCreateStoreDir;
        }
    }

    @Override
    public File[] listFiles( File directory )
    {
        return new File[0];
    }
    
    @Override
    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public void copyFile( File file, File toDirectory ) throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }
    
    private static final FileLock SYMBOLIC_FILE_LOCK = new FileLock()
    {
        @Override
        public void release() throws IOException
        {

        }
    };
}