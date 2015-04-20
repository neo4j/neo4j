/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.function.Function;
import org.neo4j.io.fs.AbstractStoreChannel;
import org.neo4j.io.fs.FileLock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

public class CannedFileSystemAbstraction implements FileSystemAbstraction
{
    public static Runnable NOTHING = new Runnable()
    {
        @Override
        public void run()
        {
        }
    };
    
    public static Runnable callCounter( final AtomicInteger count )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                count.incrementAndGet();
            }
        };
    }
    
    private final boolean fileExists;
    private final IOException cannotCreateStoreDir;
    private final IOException cannotOpenLockFile;
    private final boolean lockSuccess;
    private final Runnable onClose;

    public CannedFileSystemAbstraction( boolean fileExists,
                                        IOException cannotCreateStoreDir,
                                        IOException cannotOpenLockFile,
                                        boolean lockSuccess,
                                        Runnable onClose )
    {
        this.fileExists = fileExists;
        this.cannotCreateStoreDir = cannotCreateStoreDir;
        this.cannotOpenLockFile = cannotOpenLockFile;
        this.lockSuccess = lockSuccess;
        this.onClose = onClose;
    }

    @Override
    public StoreChannel open( File fileName, String mode ) throws IOException
    {
        if ( cannotOpenLockFile != null )
        {
            throw cannotOpenLockFile;
        }

        return emptyFileChannel;
    }
    
    private final StoreChannel emptyFileChannel = new AbstractStoreChannel()
    {
        @Override
        public int read( ByteBuffer dst ) throws IOException
        {
            return 0;
        }

        @Override
        public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
        {
            return 0;
        }

        @Override
        public long position() throws IOException
        {
            return 0;
        }

        @Override
        public StoreChannel position( long newPosition ) throws IOException
        {
            if ( newPosition != 0 )
                throw unsupported();
            return this;
        }

        @Override
        public long size() throws IOException
        {
            return 0;
        }

        @Override
        public StoreChannel truncate( long size ) throws IOException
        {
            if ( size != 0 )
                throw unsupported();
            return this;
        }

        @Override
        public void force( boolean metaData ) throws IOException
        {
        }

        @Override
        public int read( ByteBuffer dst, long position ) throws IOException
        {
            return 0;
        }

        @Override
        public int write( ByteBuffer src, long position ) throws IOException
        {
            if ( position != 0 )
                throw unsupported();
            return 0;
        }

        @Override
        public void close() throws IOException
        {
            onClose.run();
        }
        
        private IOException unsupported()
        {
            return new IOException( "Unsupported" );
        }
    };

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
    public FileLock tryLock( File fileName, StoreChannel channel ) throws IOException
    {
        if ( !lockSuccess )
        {
            throw new IOException( "Unable to create lock file " + fileName );
        }
        
        return SYMBOLIC_FILE_LOCK;
    }

    @Override
    public StoreChannel create( File fileName ) throws IOException
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
    public void mkdirs( File fileName ) throws IOException
    {
        if ( cannotCreateStoreDir != null )
        {
            throw cannotCreateStoreDir;
        }
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
    public File[] listFiles( File directory )
    {
        return new File[0];
    }

    @Override
    public File[] listFiles( File directory, FilenameFilter filter )
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
    
    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        throw new UnsupportedOperationException( "TODO" );
    }

    @Override
    public <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem( Class<K> clazz, Function<Class<K>, K>
            creator )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    private static final FileLock SYMBOLIC_FILE_LOCK = new FileLock()
    {
        @Override
        public void release() throws IOException
        {

        }
    };
}
