/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.adversaries.pagecache;

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.neo4j.adversaries.Adversary;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

/**
 * A {@linkplain PageCache page cache} that wraps another page cache and an {@linkplain Adversary adversary} to provide
 * a misbehaving page cache implementation for testing.
 * <p>
 * Depending on the adversary each operation can throw either {@link RuntimeException} like {@link SecurityException}
 * or {@link IOException} like {@link FileNotFoundException}.
 */
@SuppressWarnings( "unchecked" )
public class AdversarialPageCache implements PageCache
{
    private final PageCache delegate;
    private final Adversary adversary;

    public AdversarialPageCache( PageCache delegate, Adversary adversary )
    {
        this.delegate = Objects.requireNonNull( delegate );
        this.adversary = Objects.requireNonNull( adversary );
    }

    @Override
    public PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException
    {
        if ( ArrayUtils.contains( openOptions, StandardOpenOption.CREATE ) )
        {
            adversary.injectFailure( IOException.class, SecurityException.class );
        }
        else
        {
            adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class );
        }
        PagedFile pagedFile = delegate.map( file, pageSize, openOptions );
        return new AdversarialPagedFile( pagedFile, adversary );
    }

    @Override
    public Optional<PagedFile> getExistingMapping( File file ) throws IOException
    {
        adversary.injectFailure( IOException.class, SecurityException.class );
        final Optional<PagedFile> optional = delegate.getExistingMapping( file );
        return optional.map( pagedFile -> new AdversarialPagedFile( pagedFile, adversary ) );
    }

    @Override
    public List<PagedFile> listExistingMappings() throws IOException
    {
        adversary.injectFailure( IOException.class, SecurityException.class );
        List<PagedFile> list = delegate.listExistingMappings();
        for ( int i = 0; i < list.size(); i++ )
        {
            list.set( i, new AdversarialPagedFile( list.get( i ), adversary ) );
        }
        return list;
    }

    @Override
    public void flushAndForce() throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class );
        delegate.flushAndForce();
    }

    @Override
    public void flushAndForce( IOLimiter limiter ) throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class );
        delegate.flushAndForce( limiter );
    }

    @Override
    public void close()
    {
        adversary.injectFailure( IllegalStateException.class );
        delegate.close();
    }

    @Override
    public int pageSize()
    {
        return delegate.pageSize();
    }

    @Override
    public long maxCachedPages()
    {
        return delegate.maxCachedPages();
    }

    @Override
    public void reportEvents()
    {
        delegate.reportEvents();
    }
}
