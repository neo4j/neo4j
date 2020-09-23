/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.io.fs.watcher.FileWatcher;

public class DelegatingFileSystemAbstraction implements FileSystemAbstraction
{
    private final FileSystemAbstraction delegate;

    public DelegatingFileSystemAbstraction( FileSystemAbstraction delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public FileWatcher fileWatcher() throws IOException
    {
        return delegate.fileWatcher();
    }

    @Override
    public StoreChannel open( Path fileName, Set<OpenOption> options ) throws IOException
    {
        return delegate.open( fileName, options );
    }

    @Override
    public void moveToDirectory( Path file, Path toDirectory ) throws IOException
    {
        delegate.moveToDirectory( file, toDirectory );
    }

    @Override
    public void copyToDirectory( Path file, Path toDirectory ) throws IOException
    {
        delegate.copyToDirectory( file, toDirectory );
    }

    @Override
    public boolean mkdir( Path fileName )
    {
        return delegate.mkdir( fileName );
    }

    @Override
    public void copyFile( Path from, Path to ) throws IOException
    {
        delegate.copyFile( from, to );
    }

    @Override
    public void copyFile( Path from, Path to, CopyOption... copyOptions ) throws IOException
    {
        delegate.copyFile( from, to, copyOptions );
    }

    @Override
    public void truncate( Path path, long size ) throws IOException
    {
        delegate.truncate( path, size );
    }

    @Override
    public long lastModifiedTime( Path file )
    {
        return delegate.lastModifiedTime( file );
    }

    @Override
    public void deleteFileOrThrow( Path file ) throws IOException
    {
        delegate.deleteFileOrThrow( file );
    }

    @Override
    public Stream<FileHandle> streamFilesRecursive( Path directory ) throws IOException
    {
        return StreamFilesRecursive.streamFilesRecursive( directory, this );
    }

    @Override
    public int getFileDescriptor( StoreChannel channel )
    {
        return delegate.getFileDescriptor( channel );
    }

    @Override
    public void renameFile( Path from, Path to, CopyOption... copyOptions ) throws IOException
    {
        delegate.renameFile( from, to, copyOptions );
    }

    @Override
    public StoreChannel read( Path fileName ) throws IOException
    {
        return delegate.read( fileName );
    }

    @Override
    public StoreChannel write( Path fileName ) throws IOException
    {
        return delegate.write( fileName );
    }

    @Override
    public void mkdirs( Path fileName ) throws IOException
    {
        delegate.mkdirs( fileName );
    }

    @Override
    public boolean deleteFile( Path fileName )
    {
        return delegate.deleteFile( fileName );
    }

    @Override
    public InputStream openAsInputStream( Path fileName ) throws IOException
    {
        return delegate.openAsInputStream( fileName );
    }

    @Override
    public boolean fileExists( Path file )
    {
        return delegate.fileExists( file );
    }

    @Override
    public Path[] listFiles( Path directory, DirectoryStream.Filter<Path> filter )
    {
        return delegate.listFiles( directory, filter );
    }

    @Override
    public boolean isDirectory( Path file )
    {
        return delegate.isDirectory( file );
    }

    @Override
    public long getFileSize( Path fileName )
    {
        return delegate.getFileSize( fileName );
    }

    @Override
    public long getBlockSize( Path file ) throws IOException
    {
        return delegate.getBlockSize( file );
    }

    @Override
    public Writer openAsWriter( Path fileName, Charset charset, boolean append ) throws IOException
    {
        return delegate.openAsWriter( fileName, charset, append );
    }

    @Override
    public Path[] listFiles( Path directory )
    {
        return delegate.listFiles( directory );
    }

    @Override
    public void deleteRecursively( Path directory ) throws IOException
    {
        delegate.deleteRecursively( directory );
    }

    @Override
    public OutputStream openAsOutputStream( Path fileName, boolean append ) throws IOException
    {
        return delegate.openAsOutputStream( fileName, append );
    }

    @Override
    public Reader openAsReader( Path fileName, Charset charset ) throws IOException
    {
        return delegate.openAsReader( fileName, charset );
    }

    @Override
    public void copyRecursively( Path fromDirectory, Path toDirectory ) throws IOException
    {
        delegate.copyRecursively( fromDirectory, toDirectory );
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }
}
