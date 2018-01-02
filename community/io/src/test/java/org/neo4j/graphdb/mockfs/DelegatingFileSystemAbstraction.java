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
package org.neo4j.graphdb.mockfs;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import org.neo4j.function.Function;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

public class DelegatingFileSystemAbstraction implements FileSystemAbstraction
{
    private final FileSystemAbstraction delegate;

    public DelegatingFileSystemAbstraction( FileSystemAbstraction delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public StoreChannel open( File fileName, String mode ) throws IOException
    {
        return delegate.open( fileName, mode );
    }

    @Override
    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        delegate.moveToDirectory( file, toDirectory );
    }

    @Override
    public boolean mkdir( File fileName )
    {
        return delegate.mkdir( fileName );
    }

    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        delegate.copyFile( from, to );
    }

    @Override
    public <K extends FileSystemAbstraction.ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem( Class<K> clazz,
                                                                                                     Function<Class<K>, K> creator )
    {
        return delegate.getOrCreateThirdPartyFileSystem( clazz, creator );
    }

    @Override
    public void truncate( File path, long size ) throws IOException
    {
        delegate.truncate( path, size );
    }

    @Override
    public boolean renameFile( File from, File to ) throws IOException
    {
        return delegate.renameFile( from, to );
    }

    @Override
    public StoreChannel create( File fileName ) throws IOException
    {
        return delegate.create( fileName );
    }

    @Override
    public void mkdirs( File fileName ) throws IOException
    {
        delegate.mkdirs( fileName );
    }

    @Override
    public boolean deleteFile( File fileName )
    {
        return delegate.deleteFile( fileName );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return delegate.openAsInputStream( fileName );
    }

    @Override
    public boolean fileExists( File fileName )
    {
        return delegate.fileExists( fileName );
    }

    @Override
    public File[] listFiles( File directory, FilenameFilter filter )
    {
        return delegate.listFiles( directory, filter );
    }

    @Override
    public boolean isDirectory( File file )
    {
        return delegate.isDirectory( file );
    }

    @Override
    public long getFileSize( File fileName )
    {
        return delegate.getFileSize( fileName );
    }

    @Override
    public Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException
    {
        return delegate.openAsWriter( fileName, encoding, append );
    }

    @Override
    public File[] listFiles( File directory )
    {
        return delegate.listFiles( directory );
    }

    @Override
    public void deleteRecursively( File directory ) throws IOException
    {
        delegate.deleteRecursively( directory );
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return delegate.openAsOutputStream( fileName, append );
    }

    @Override
    public Reader openAsReader( File fileName, String encoding ) throws IOException
    {
        return delegate.openAsReader( fileName, encoding );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        delegate.copyRecursively( fromDirectory, toDirectory );
    }
}
