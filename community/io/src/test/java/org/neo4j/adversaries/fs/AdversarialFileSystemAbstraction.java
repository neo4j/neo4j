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
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.adversaries.Adversary;
import org.neo4j.function.Function;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

/**
 * Used by the robustness suite to check for partial failures.
 */
@SuppressWarnings("unchecked")
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

    public StoreChannel open( File fileName, String mode ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class );
        return AdversarialFileChannel.wrap( delegate.open( fileName, mode ), adversary );
    }

    public boolean renameFile( File from, File to ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, SecurityException.class );
        return delegate.renameFile( from, to );
    }

    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, SecurityException.class );
        return new AdversarialOutputStream( delegate.openAsOutputStream( fileName, append ), adversary );
    }

    public StoreChannel create( File fileName ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class );
        return AdversarialFileChannel.wrap( delegate.create( fileName ), adversary );
    }

    public boolean mkdir( File fileName )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.mkdir( fileName );
    }

    public File[] listFiles( File directory )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.listFiles( directory );
    }

    public File[] listFiles( File directory, FilenameFilter filter )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.listFiles( directory, filter );
    }

    public Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException
    {
        adversary.injectFailure(
                UnsupportedEncodingException.class, FileNotFoundException.class, SecurityException.class );
        return new AdversarialWriter( delegate.openAsWriter( fileName, encoding, append ), adversary );
    }

    public Reader openAsReader( File fileName, String encoding ) throws IOException
    {
        adversary.injectFailure(
                UnsupportedEncodingException.class, FileNotFoundException.class, SecurityException.class );
        return new AdversarialReader( delegate.openAsReader( fileName, encoding ), adversary );
    }

    public long getFileSize( File fileName )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.getFileSize( fileName );
    }

    public void copyFile( File from, File to ) throws IOException
    {
        adversary.injectFailure( SecurityException.class, FileNotFoundException.class, IOException.class );
        delegate.copyFile( from, to );
    }

    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        adversary.injectFailure( SecurityException.class, IOException.class, NullPointerException.class );
        delegate.copyRecursively( fromDirectory, toDirectory );
    }

    public boolean deleteFile( File fileName )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.deleteFile( fileName );
    }

    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, SecurityException.class );
        return new AdversarialInputStream( delegate.openAsInputStream( fileName ), adversary );
    }

    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        adversary.injectFailure(
                SecurityException.class, IllegalArgumentException.class, FileNotFoundException.class,
                NullPointerException.class, IOException.class );
        delegate.moveToDirectory( file, toDirectory );
    }

    public boolean isDirectory( File file )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.isDirectory( file );
    }

    public boolean fileExists( File fileName )
    {
        adversary.injectFailure( SecurityException.class );
        return delegate.fileExists( fileName );
    }

    public void mkdirs( File fileName ) throws IOException
    {
        adversary.injectFailure( SecurityException.class, IOException.class );
        delegate.mkdirs( fileName );
    }

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
        if (fileSystem == null)
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

    private <K extends ThirdPartyFileSystem> ThirdPartyFileSystem adversarialProxy(
            final ThirdPartyFileSystem fileSystem,
            Class<K> clazz )
    {
        InvocationHandler handler = new InvocationHandler()
        {
            @Override
            public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
            {
                adversary.injectFailure( (Class<? extends Throwable>[]) method.getExceptionTypes() );
                return method.invoke( fileSystem, args );
            }
        };
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        return (ThirdPartyFileSystem) Proxy.newProxyInstance( loader, new Class[] { clazz }, handler );
    }
}
