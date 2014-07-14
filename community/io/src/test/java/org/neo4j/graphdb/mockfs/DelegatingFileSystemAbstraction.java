/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import org.neo4j.function.Function;
import org.neo4j.io.fs.FileLock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

public class DelegatingFileSystemAbstraction implements FileSystemAbstraction
{
    private final FileSystemAbstraction delegate;

    public DelegatingFileSystemAbstraction( FileSystemAbstraction delegate )
    {
        this.delegate = delegate;
    }

    public StoreChannel open( File fileName, String mode ) throws IOException
    {
        return delegate.open( fileName, mode );
    }

    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        delegate.moveToDirectory( file, toDirectory );
    }

    public boolean mkdir( File fileName )
    {
        return delegate.mkdir( fileName );
    }

    public void copyFile( File from, File to ) throws IOException
    {
        delegate.copyFile( from, to );
    }

    public <K extends FileSystemAbstraction.ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem( Class<K> clazz,
                                                                                                     Function<Class<K>, K> creator )
    {
        return delegate.getOrCreateThirdPartyFileSystem( clazz, creator );
    }

    public boolean renameFile( File from, File to ) throws IOException
    {
        return delegate.renameFile( from, to );
    }

    public FileLock tryLock( File fileName, StoreChannel channel ) throws IOException
    {
        return delegate.tryLock( fileName, channel );
    }

    public StoreChannel create( File fileName ) throws IOException
    {
        return delegate.create( fileName );
    }

    public void mkdirs( File fileName ) throws IOException
    {
        delegate.mkdirs( fileName );
    }

    public boolean deleteFile( File fileName )
    {
        return delegate.deleteFile( fileName );
    }

    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return delegate.openAsInputStream( fileName );
    }

    public boolean fileExists( File fileName )
    {
        return delegate.fileExists( fileName );
    }

    public boolean isDirectory( File file )
    {
        return delegate.isDirectory( file );
    }

    public long getFileSize( File fileName )
    {
        return delegate.getFileSize( fileName );
    }

    public Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException
    {
        return delegate.openAsWriter( fileName, encoding, append );
    }

    public File[] listFiles( File directory )
    {
        return delegate.listFiles( directory );
    }

    public void deleteRecursively( File directory ) throws IOException
    {
        delegate.deleteRecursively( directory );
    }

    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return delegate.openAsOutputStream( fileName, append );
    }

    public Reader openAsReader( File fileName, String encoding ) throws IOException
    {
        return delegate.openAsReader( fileName, encoding );
    }

    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        delegate.copyRecursively( fromDirectory, toDirectory );
    }
}
