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
package org.neo4j.graphdb.mockfs;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import org.neo4j.function.Function;
import org.neo4j.io.fs.FileLock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.impl.ChannelInputStream;
import org.neo4j.test.impl.ChannelOutputStream;

// TODO: Move this to org.neo4j.io
/**
 * This is a FileSystemAbstraction that can break in interesting ways during various operations. It is provided
 * with a FileSystemGuard which accepts a callback on each operation and depending on the state it can trigger
 * behaviour. It wraps a filesystem which will accept the final operation.
 */
public class BreakableFileSystemAbstraction implements FileSystemAbstraction, FileSystemGuard
{
    private final FileSystemGuard guard;

    public void checkOperation( OperationType operationType, File onFile, int bytesWrittenTotal, int bytesWrittenThisCall, long channelPosition ) throws IOException
    {
        guard.checkOperation( operationType, onFile, bytesWrittenTotal, bytesWrittenThisCall, channelPosition );
    }

    private final FileSystemAbstraction inner;

    public BreakableFileSystemAbstraction( FileSystemAbstraction inner, FileSystemGuard guard )
    {
        this.inner = inner;
        this.guard = guard;
    }

    @Override
    public StoreChannel open( File fileName, String mode ) throws IOException
    {
        return new BreakableFileChannel( inner.open( fileName, mode ), fileName, this );
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return new ChannelOutputStream( open( fileName, "rw" ), append );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return new ChannelInputStream( open( fileName, "r" ) );
    }

    @Override
    public Reader openAsReader( File fileName, String encoding ) throws IOException
    {
        return new InputStreamReader( openAsInputStream( fileName ), encoding );
    }

    @Override
    public Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException
    {
        return new OutputStreamWriter( openAsOutputStream( fileName, append ) );
    }

    @Override
    public FileLock tryLock( File fileName, StoreChannel channel ) throws IOException
    {
        return inner.tryLock( fileName, channel );
    }

    @Override
    public StoreChannel create( File fileName ) throws IOException
    {
        return new BreakableFileChannel( inner.create( fileName ), fileName, this );
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
    public void deleteRecursively( File directory ) throws IOException
    {
        inner.deleteRecursively( directory );
    }

    @Override
    public boolean mkdir( File fileName )
    {
        return inner.mkdir( fileName );
    }

    @Override
    public void mkdirs( File fileName ) throws IOException
    {
        inner.mkdirs( fileName );
    }

    @Override
    public boolean renameFile( File from, File to ) throws IOException
    {
        return inner.renameFile( from, to );
    }

    @Override
    public File[] listFiles( File directory )
    {
        return inner.listFiles( directory );
    }

    @Override
    public File[] listFiles( File directory, FilenameFilter filter )
    {
        return inner.listFiles( directory, filter );
    }

    @Override
    public boolean isDirectory( File file )
    {
        return inner.isDirectory( file );
    }

    @Override
    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        inner.moveToDirectory( file, toDirectory );
    }

    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        inner.copyFile( from, to );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        inner.copyRecursively( fromDirectory, toDirectory );
    }

    @Override
    public <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem( Class<K> clazz, Function<Class<K>, K>
            creator )
    {
        return inner.getOrCreateThirdPartyFileSystem( clazz, creator );
    }

}
