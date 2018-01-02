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
package org.neo4j.io.fs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;

public abstract class DelegatingPath implements Path
{
    private final Path delegate;
    private final FileSystem filesystem;

    public DelegatingPath( Path delegate )
    {
        this( delegate, delegate.getFileSystem() );
    }

    public DelegatingPath( Path delegate, FileSystem fs )
    {
        this.delegate = delegate;
        this.filesystem = fs;
    }

    public Path getDelegate()
    {
        return delegate;
    }

    @Override
    public FileSystem getFileSystem()
    {
        return filesystem;
    }

    @Override
    public boolean isAbsolute()
    {
        return delegate.isAbsolute();
    }

    @Override
    public Path getRoot()
    {
        return wrapPath( delegate.getRoot() );
    }

    @Override
    public Path getFileName()
    {
        return wrapPath( delegate.getFileName() );
    }

    @Override
    public Path getParent()
    {
        return wrapPath( delegate.getParent() );
    }

    @Override
    public int getNameCount()
    {
        return delegate.getNameCount();
    }

    @Override
    public Path getName( int index )
    {
        return wrapPath( delegate.getName( index ) );
    }

    @Override
    public Path subpath( int beginIndex, int endIndex )
    {
        return wrapPath( delegate.subpath( beginIndex, endIndex ) );
    }

    @Override
    public boolean startsWith( Path other )
    {
        return delegate.startsWith( getDelegate( other ) );
    }

    @Override
    public boolean startsWith( String other )
    {
        return delegate.startsWith( other );
    }

    @Override
    public boolean endsWith( Path other )
    {
        return delegate.endsWith( getDelegate( other ) );
    }

    @Override
    public boolean endsWith( String other )
    {
        return delegate.endsWith( other );
    }

    @Override
    public Path normalize()
    {
        return wrapPath( delegate.normalize() );
    }

    @Override
    public Path resolve( Path other )
    {
        return wrapPath( delegate.resolve( getDelegate( other ) ) );
    }

    @Override
    public Path resolve( String other )
    {
        return wrapPath( delegate.resolve( other ) );
    }

    @Override
    public Path resolveSibling( Path other )
    {
        return wrapPath( delegate.resolveSibling( getDelegate( other ) ) );
    }

    @Override
    public Path resolveSibling( String other )
    {
        return wrapPath( delegate.resolveSibling( other ) );
    }

    @Override
    public Path relativize( Path other )
    {
        return wrapPath( delegate.relativize( getDelegate( other ) ) );
    }

    @Override
    public URI toUri()
    {
        return delegate.toUri();
    }

    @Override
    public Path toAbsolutePath()
    {
        return wrapPath( delegate.toAbsolutePath() );
    }

    @Override
    public Path toRealPath( LinkOption... options ) throws IOException
    {
        return wrapPath( delegate.toRealPath( options ) );
    }

    @Override
    public File toFile()
    {
        return delegate.toFile();
    }

    @Override
    public WatchKey register( WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register( WatchService watcher, WatchEvent.Kind<?>... events ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Path> iterator()
    {
        final Iterator<Path> delegateIterator = delegate.iterator();
        return new Iterator<Path>()
        {
            @Override
            public boolean hasNext()
            {
                return delegateIterator.hasNext();
            }

            @Override
            public Path next()
            {
                return wrapPath( delegateIterator.next() );
            }

            @Override
            public void remove()
            {
                delegateIterator.remove();
            }
        };
    }

    @Override
    public int compareTo( Path other )
    {
        return delegate.compareTo( other );
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    private Path wrapPath( Path path )
    {
        return ( path == null ) ? null : createDelegate( path );
    }

    protected abstract Path createDelegate( Path path );

    public static Path getDelegate( Path path )
    {
        if ( path == null )
        {
            throw new NullPointerException();
        }

        if ( !( path instanceof DelegatingPath ) )
        {
            throw new ProviderMismatchException();
        }
        return ( (DelegatingPath) path ).getDelegate();
    }
}
