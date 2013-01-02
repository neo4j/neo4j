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
package org.neo4j.graphdb.mockfs;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

public class LimitedFilesystemAbstraction implements FileSystemAbstraction
{
    private FileSystemAbstraction inner;
    private boolean outOfSpace;
    private Integer bytesAtATime = null;

    public LimitedFilesystemAbstraction(FileSystemAbstraction wrapped)
    {
        this.inner = wrapped;
    }

    @Override
    public FileChannel open( File fileName, String mode ) throws IOException
    {
        return new LimitedFileChannel( inner.open( fileName, mode ), this );
    }

    @Override
    public FileLock tryLock( File fileName, FileChannel channel ) throws IOException
    {
        return inner.tryLock( fileName, channel );
    }

    @Override
    public FileChannel create( File fileName ) throws IOException
    {
        ensureHasSpace();
        return new LimitedFileChannel( inner.create( fileName ), this );
    }

    @Override
    public boolean fileExists( File fileName )
    {
        return inner.fileExists( fileName );
    }

    @Override
    public long getFileSize( File fileName )
    {
        return inner.getFileSize( fileName );
    }

    @Override
    public boolean deleteFile( File fileName )
    {
        return inner.deleteFile( fileName );
    }

    @Override
    public boolean renameFile( File from, File to ) throws IOException
    {
        ensureHasSpace();
        return inner.renameFile( from, to );
    }

    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        ensureHasSpace();
        inner.copyFile( from, to );
    }

    @Override
    public void autoCreatePath( File path ) throws IOException
    {
        ensureHasSpace();
        inner.autoCreatePath( path );
    }

    public void runOutOfDiskSpace()
    {
        outOfSpace = true;
    }

    public void ensureHasSpace() throws IOException
    {
        if( outOfSpace )
        {
            throw new IOException( "No space left on device" );
        }
    }

    public void limitWritesTo( int bytesAtATime )
    {
        this.bytesAtATime = bytesAtATime;
    }
}
