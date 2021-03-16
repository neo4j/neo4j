/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.dbms.database;

import org.eclipse.collections.api.set.ImmutableSet;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.buffer.IOBufferFactory;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;

import static java.util.Objects.requireNonNull;

/**
 * Wrapper around global page cache for an individual database. Abstracts the knowledge that database can have about other databases mapped files
 * by restricting access to files that mapped by other databases.
 * Any lookup or attempts to flush/close page file or cache itself will influence only files that were mapped by particular database over this wrapper.
 * Database specific page cache lifecycle tight to a individual database and it will be closed as soon as the particular database will be closed.
 */
public class DatabasePageCache implements PageCache
{
    private final PageCache globalPageCache;
    private final CopyOnWriteArrayList<PagedFile> databasePagedFiles = new CopyOnWriteArrayList<>();
    private final VersionContextSupplier versionContextSupplier;
    private final IOController ioController;
    private boolean closed;

    public DatabasePageCache( PageCache globalPageCache, VersionContextSupplier versionContextSupplier, IOController ioController )
    {
        this.globalPageCache = requireNonNull( globalPageCache );
        this.versionContextSupplier = requireNonNull( versionContextSupplier );
        this.ioController = requireNonNull( ioController );
    }

    @Override
    public PagedFile map( Path path, VersionContextSupplier versionContextSupplier, int pageSize, String databaseName,
            ImmutableSet<OpenOption> openOptions, IOController ignoredController ) throws IOException
    {
        // no one should call this version of map method with emptyDatabaseName != null,
        // since it is this class that is decorating map calls with the name of the database
        PagedFile pagedFile = globalPageCache.map( path, versionContextSupplier, pageSize, databaseName, openOptions, ioController );
        DatabasePageFile databasePageFile = new DatabasePageFile( pagedFile, databasePagedFiles );
        databasePagedFiles.add( databasePageFile );
        return databasePageFile;
    }

    @Override
    public Optional<PagedFile> getExistingMapping( Path path )
    {
        Path canonicalFile = path.normalize();

        return databasePagedFiles.stream().filter( pagedFile -> pagedFile.path().equals( canonicalFile ) ).findFirst();
    }

    @Override
    public List<PagedFile> listExistingMappings()
    {
        return new ArrayList<>( databasePagedFiles );
    }

    @Override
    public void flushAndForce() throws IOException
    {
        for ( PagedFile pagedFile : databasePagedFiles )
        {
            pagedFile.flushAndForce();
        }
    }

    @Override
    public synchronized void close()
    {
        //TODO: this called on shutdown of the db?
        if ( closed )
        {
            throw new IllegalStateException( "Database page cache was already closed" );
        }
        for ( PagedFile pagedFile : databasePagedFiles )
        {
            pagedFile.close();
        }
        databasePagedFiles.clear();
        closed = true;
    }

    @Override
    public int pageSize()
    {
        return globalPageCache.pageSize();
    }

    @Override
    public long maxCachedPages()
    {
        return globalPageCache.maxCachedPages();
    }

    @Override
    public VersionContextSupplier versionContextSupplier()
    {
        return versionContextSupplier;
    }

    @Override
    public IOBufferFactory getBufferFactory()
    {
        return globalPageCache.getBufferFactory();
    }

    private static class DatabasePageFile implements PagedFile
    {
        private final PagedFile delegate;
        private final List<PagedFile> databaseFiles;

        DatabasePageFile( PagedFile delegate, List<PagedFile> databaseFiles )
        {
            this.delegate = delegate;
            this.databaseFiles = databaseFiles;
        }

        @Override
        public PageCursor io( long pageId, int pf_flags, PageCursorTracer tracer ) throws IOException
        {
            return delegate.io( pageId, pf_flags, tracer );
        }

        @Override
        public int pageSize()
        {
            return delegate.pageSize();
        }

        @Override
        public long fileSize() throws IOException
        {
            return delegate.fileSize();
        }

        @Override
        public Path path()
        {
            return delegate.path();
        }

        @Override
        public void flushAndForce() throws IOException
        {
            delegate.flushAndForce();
        }

        @Override
        public long getLastPageId() throws IOException
        {
            return delegate.getLastPageId();
        }

        @Override
        public void close()
        {
            delegate.close();
            databaseFiles.remove( this );
        }

        @Override
        public void setDeleteOnClose( boolean deleteOnClose )
        {
            delegate.setDeleteOnClose( deleteOnClose );
        }

        @Override
        public boolean isDeleteOnClose()
        {
            return delegate.isDeleteOnClose();
        }

        @Override
        public String getDatabaseName()
        {
            return delegate.getDatabaseName();
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            DatabasePageFile that = (DatabasePageFile) o;
            return delegate.equals( that.delegate );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( delegate );
        }
    }
}
