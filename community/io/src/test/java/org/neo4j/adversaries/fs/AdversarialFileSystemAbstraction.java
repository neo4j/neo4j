/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.adversaries.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.watcher.AdversarialFileWatcher;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.fs.StreamFilesRecursive;
import org.neo4j.io.fs.watcher.FileWatcher;

/**
 * Used by the robustness suite to check for partial failures.
 */
@SuppressWarnings( "unchecked" )
public class AdversarialFileSystemAbstraction implements FileSystemAbstraction
{
    private final FileSystemAbstraction delegate;
    private final Adversary adversary;

    public AdversarialFileSystemAbstraction( Adversary adversary )
    {
        this( adversary, new DefaultFileSystemAbstraction() );
    }

    public AdversarialFileSystemAbstraction( Adversary adversary, FileSystemAbstraction delegate )
    {
        this.adversary = adversary;
        this.delegate = delegate;
    }

    @Override
    public FileWatcher fileWatcher() throws IOException
    {
        adversary.injectFailure( UnsupportedOperationException.class, IOException.class );
        return new AdversarialFileWatcher( delegate.fileWatcher(), adversary );
    }

    @Override
    public StoreChannel open( File fileName, OpenMode openMode ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class );
        return AdversarialFileChannel.wrap( (StoreFileChannel) delegate.open( fileName, openMode ), adversary );
    }

    @Override
    public void renameFile( File from, File to, CopyOption... copyOptions ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, SecurityException.class );
        delegate.renameFile( from, to, copyOptions );
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, SecurityException.class );
        return new AdversarialOutputStream( delegate.openAsOutputStream( fileName, append ), adversary );
    }

    @Override
    public StoreChannel create( File fileName ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class );
        return AdversarialFileChannel.wrap( (StoreFileChannel) delegate.create( fileName ), adversary );
    }

    @Override
    public boolean mkdir( File fileName )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.mkdir( fileName );
    }

    @Override
    public File[] listFiles( File directory )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.listFiles( directory );
    }

    @Override
    public File[] listFiles( File directory, FilenameFilter filter )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.listFiles( directory, filter );
    }

    @Override
    public Writer openAsWriter( File fileName, Charset charset, boolean append ) throws IOException
    {
        adversary.injectFailure(
                UnsupportedEncodingException.class, FileNotFoundException.class, SecurityException.class );
        return new AdversarialWriter( delegate.openAsWriter( fileName, charset, append ), adversary );
    }

    @Override
    public Reader openAsReader( File fileName, Charset charset ) throws IOException
    {
        adversary.injectFailure(
                UnsupportedEncodingException.class, FileNotFoundException.class, SecurityException.class );
        return new AdversarialReader( delegate.openAsReader( fileName, charset ), adversary );
    }

    @Override
    public long getFileSize( File fileName )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.getFileSize( fileName );
    }

    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        adversary.injectFailure( SecurityException.class, FileNotFoundException.class, IOException.class );
        delegate.copyFile( from, to );
    }

    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        adversary.injectFailure( SecurityException.class, IOException.class, NullPointerException.class );
        delegate.copyRecursively( fromDirectory, toDirectory );
    }

    @Override
    public boolean deleteFile( File fileName )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.deleteFile( fileName );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, SecurityException.class );
        return new AdversarialInputStream( delegate.openAsInputStream( fileName ), adversary );
    }

    @Override
    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        adversary.injectFailure(
                SecurityException.class, IllegalArgumentException.class, FileNotFoundException.class,
                NullPointerException.class, IOException.class );
        delegate.moveToDirectory( file, toDirectory );
    }

    @Override
    public void copyToDirectory( File file, File toDirectory ) throws IOException
    {
        adversary.injectFailure(
                SecurityException.class, IllegalArgumentException.class, FileNotFoundException.class,
                NullPointerException.class, IOException.class );
        delegate.copyToDirectory( file, toDirectory );
    }

    @Override
    public boolean isDirectory( File file )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.isDirectory( file );
    }

    @Override
    public boolean fileExists( File fileName )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.fileExists( fileName );
    }

    @Override
    public void mkdirs( File fileName ) throws IOException
    {
        adversary.injectFailure( SecurityException.class, IOException.class );
        delegate.mkdirs( fileName );
    }

    @Override
    public void deleteRecursively( File directory ) throws IOException
    {
        adversary.injectFailure( SecurityException.class, NullPointerException.class, IOException.class );
        delegate.deleteRecursively( directory );
    }

    private final Map<Class<? extends ThirdPartyFileSystem>, ThirdPartyFileSystem> thirdPartyFileSystems =
            new HashMap<>();

    @Override
    public synchronized <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem(
            Class<K> clazz,
            Function<Class<K>, K> creator )
    {
        ThirdPartyFileSystem fileSystem = thirdPartyFileSystems.get( clazz );
        if ( fileSystem == null )
        {
            fileSystem = creator.apply( clazz );
            fileSystem = adversarialProxy( fileSystem, clazz );
            thirdPartyFileSystems.put( clazz, fileSystem );
        }
        return (K) fileSystem;
    }

    @Override
    public void truncate( File path, long size ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, IllegalArgumentException.class,
                SecurityException.class, NullPointerException.class );
        delegate.truncate( path, size );
    }

    @Override
    public long lastModifiedTime( File file )
    {
        adversary.injectFailure( SecurityException.class, NullPointerException.class );
        return delegate.lastModifiedTime( file );
    }

    @Override
    public void deleteFileOrThrow( File file ) throws IOException
    {
        adversary.injectFailure( NoSuchFileException.class, IOException.class, SecurityException.class );
        delegate.deleteFileOrThrow( file );
    }

    @Override
    public Stream<FileHandle> streamFilesRecursive( File directory ) throws IOException
    {
        return StreamFilesRecursive.streamFilesRecursive( directory, this );
    }

    private <K extends ThirdPartyFileSystem> ThirdPartyFileSystem adversarialProxy(
            final ThirdPartyFileSystem fileSystem,
            Class<K> clazz )
    {
        InvocationHandler handler = ( proxy, method, args ) ->
        {
            adversary.injectFailure( (Class<? extends Throwable>[]) method.getExceptionTypes() );
            return method.invoke( fileSystem, args );
        };
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        return (ThirdPartyFileSystem) Proxy.newProxyInstance( loader, new Class[] { clazz }, handler );
    }

    @Override
    public void close() throws IOException
    {
        adversary.injectFailure( IOException.class, SecurityException.class );
        delegate.close();
    }
}
