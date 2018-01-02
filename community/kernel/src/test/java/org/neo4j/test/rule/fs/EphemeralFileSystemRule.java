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
package org.neo4j.test.rule.fs;

import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

public class EphemeralFileSystemRule extends ExternalResource implements Supplier<FileSystemAbstraction>,
        FileSystemAbstraction
{
    private EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();

    @Override
    protected void after()
    {
        fs.shutdown();
    }

    @Override
    public final EphemeralFileSystemAbstraction get()
    {
        return fs;
    }

    public EphemeralFileSystemAbstraction snapshot( Runnable action )
    {
        EphemeralFileSystemAbstraction snapshot = fs.snapshot();
        try
        {
            action.run();
        }
        finally
        {
            fs.shutdown();
            fs = snapshot;
        }
        return fs;
    }

    public void clear()
    {
        fs.shutdown();
        fs = new EphemeralFileSystemAbstraction();
    }

    public static Runnable shutdownDbAction( final GraphDatabaseService db )
    {
        return () -> db.shutdown();
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

    public void crash()
    {
        fs.crash();
    }

    public void shutdown()
    {
        fs.shutdown();
    }

    public void assertNoOpenFiles() throws Exception
    {
        fs.assertNoOpenFiles();
    }

    public void dumpZip( OutputStream output ) throws IOException
    {
        fs.dumpZip( output );
    }

    @Override
    public StoreChannel open( File fileName, String mode ) throws IOException
    {
        return fs.open( fileName, mode );
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return fs.openAsOutputStream( fileName, append );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return fs.openAsInputStream( fileName );
    }

    @Override
    public Reader openAsReader( File fileName, Charset charset ) throws IOException
    {
        return fs.openAsReader( fileName, charset );
    }

    @Override
    public String toString()
    {
        return fs.toString();
    }

    @Override
    public Writer openAsWriter( File fileName, Charset charset, boolean append ) throws IOException
    {
        return fs.openAsWriter( fileName, charset, append );
    }

    @Override
    public StoreChannel create( File fileName ) throws IOException
    {
        return fs.create( fileName );
    }

    @Override
    public long getFileSize( File fileName )
    {
        return fs.getFileSize( fileName );
    }

    @Override
    public boolean fileExists( File file )
    {
        return fs.fileExists( file );
    }

    @Override
    public boolean isDirectory( File file )
    {
        return fs.isDirectory( file );
    }

    @Override
    public boolean mkdir( File directory )
    {
        return fs.mkdir( directory );
    }

    @Override
    public void mkdirs( File directory )
    {
        fs.mkdirs( directory );
    }

    @Override
    public boolean deleteFile( File fileName )
    {
        return fs.deleteFile( fileName );
    }

    @Override
    public void deleteRecursively( File directory ) throws IOException
    {
        fs.deleteRecursively( directory );
    }

    @Override
    public void renameFile( File from, File to, CopyOption... copyOptions ) throws IOException
    {
        fs.renameFile( from, to, copyOptions );
    }

    @Override
    public File[] listFiles( File directory )
    {
        return fs.listFiles( directory );
    }

    @Override
    public File[] listFiles( File directory, FilenameFilter filter )
    {
        return fs.listFiles( directory, filter );
    }

    @Override
    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        fs.moveToDirectory( file, toDirectory );
    }

    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        fs.copyFile( from, to );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        fs.copyRecursively( fromDirectory, toDirectory );
    }

    public EphemeralFileSystemAbstraction snapshot()
    {
        return fs.snapshot();
    }

    public void copyRecursivelyFromOtherFs( File from, FileSystemAbstraction fromFs, File to ) throws IOException
    {
        fs.copyRecursivelyFromOtherFs( from, fromFs, to );
    }

    public long checksum()
    {
        return fs.checksum();
    }

    @Override
    public <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem( Class<K> clazz,
            Function<Class<K>,K> creator )
    {
        return fs.getOrCreateThirdPartyFileSystem( clazz, creator );
    }

    @Override
    public void truncate( File file, long size ) throws IOException
    {
        fs.truncate( file, size );
    }

    @Override
    public long lastModifiedTime( File file ) throws IOException
    {
        return fs.lastModifiedTime( file );
    }

    @Override
    public void deleteFileOrThrow( File file ) throws IOException
    {
        fs.deleteFileOrThrow( file );
    }
}
