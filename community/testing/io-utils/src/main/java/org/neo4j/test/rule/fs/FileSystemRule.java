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
package org.neo4j.test.rule.fs;

import org.junit.rules.ExternalResource;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.watcher.FileWatcher;

public abstract class FileSystemRule<FS extends FileSystemAbstraction> extends ExternalResource
        implements FileSystemAbstraction, Supplier<FileSystemAbstraction>
{
    protected volatile FS fs;

    protected FileSystemRule( FS fs )
    {
        this.fs = fs;
    }

    @Override
    protected void after()
    {
        try
        {
            fs.close();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        super.after();
    }

    @Override
    public FS get()
    {
        return fs;
    }

    @Override
    public void close() throws IOException
    {
        fs.close();
    }

    @Override
    public FileWatcher fileWatcher() throws IOException
    {
        return fs.fileWatcher();
    }

    @Override
    public StoreChannel open( Path fileName, Set<OpenOption> options ) throws IOException
    {
        return fs.open( fileName, options );
    }

    @Override
    public OutputStream openAsOutputStream( Path fileName, boolean append ) throws IOException
    {
        return fs.openAsOutputStream( fileName, append );
    }

    @Override
    public InputStream openAsInputStream( Path fileName ) throws IOException
    {
        return fs.openAsInputStream( fileName );
    }

    @Override
    public Reader openAsReader( Path fileName, Charset charset ) throws IOException
    {
        return fs.openAsReader( fileName, charset );
    }

    @Override
    public Writer openAsWriter( Path fileName, Charset charset, boolean append ) throws IOException
    {
        return fs.openAsWriter( fileName, charset, append );
    }

    @Override
    public StoreChannel write( Path fileName ) throws IOException
    {
        return fs.write( fileName );
    }

    @Override
    public StoreChannel read( Path fileName ) throws IOException
    {
        return fs.read( fileName );
    }

    @Override
    public boolean fileExists( Path file )
    {
        return fs.fileExists( file );
    }

    @Override
    public boolean mkdir( Path fileName )
    {
        return fs.mkdir( fileName );
    }

    @Override
    public void mkdirs( Path fileName ) throws IOException
    {
        fs.mkdirs( fileName );
    }

    @Override
    public long getFileSize( Path fileName )
    {
        return fs.getFileSize( fileName );
    }

    @Override
    public boolean deleteFile( Path fileName )
    {
        return fs.deleteFile( fileName );
    }

    @Override
    public void deleteRecursively( Path directory ) throws IOException
    {
        fs.deleteRecursively( directory );
    }

    @Override
    public void renameFile( Path from, Path to, CopyOption... copyOptions ) throws IOException
    {
        fs.renameFile( from, to, copyOptions );
    }

    @Override
    public Path[] listFiles( Path directory )
    {
        return fs.listFiles( directory );
    }

    @Override
    public Path[] listFiles( Path directory, FilenameFilter filter )
    {
        return fs.listFiles( directory, filter );
    }

    @Override
    public boolean isDirectory( Path file )
    {
        return fs.isDirectory( file );
    }

    @Override
    public void moveToDirectory( Path file, Path toDirectory ) throws IOException
    {
        fs.moveToDirectory( file, toDirectory );
    }

    @Override
    public void copyToDirectory( Path file, Path toDirectory ) throws IOException
    {
        fs.copyToDirectory( file, toDirectory );
    }

    @Override
    public void copyFile( Path from, Path to ) throws IOException
    {
        fs.copyFile( from, to );
    }

    @Override
    public void copyFile( Path from, Path to, CopyOption... copyOptions ) throws IOException
    {
        fs.copyFile( from, to, copyOptions );
    }

    @Override
    public void copyRecursively( Path fromDirectory, Path toDirectory ) throws IOException
    {
        fs.copyRecursively( fromDirectory, toDirectory );
    }

    @Override
    public void truncate( Path path, long size ) throws IOException
    {
        fs.truncate( path, size );
    }

    @Override
    public long lastModifiedTime( Path file )
    {
        return fs.lastModifiedTime( file );
    }

    @Override
    public void deleteFileOrThrow( Path file ) throws IOException
    {
        fs.deleteFileOrThrow( file );
    }

    @Override
    public Stream<FileHandle> streamFilesRecursive( Path directory ) throws IOException
    {
        return fs.streamFilesRecursive( directory );
    }

    @Override
    public int getFileDescriptor( StoreChannel channel )
    {
        return fs.getFileDescriptor( channel );
    }

    @Override
    public int hashCode()
    {
        return fs.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        return fs.equals( obj );
    }

    @Override
    public String toString()
    {
        return fs.toString();
    }

    @Override
    public long getBlockSize( Path file ) throws IOException
    {
        return fs.getBlockSize( file );
    }
}
