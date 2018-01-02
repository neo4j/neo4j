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
package org.neo4j.adversaries.pagecache;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;

import org.neo4j.adversaries.Adversary;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

/**
 * A {@linkplain PagedFile paged file} that wraps another paged file and an {@linkplain Adversary adversary} to provide
 * a misbehaving paged file implementation for testing.
 * <p>
 * Depending on the adversary each operation can throw either {@link RuntimeException} like {@link SecurityException}
 * or {@link IOException} like {@link FileNotFoundException}.
 */
@SuppressWarnings( "unchecked" )
class AdversarialPagedFile implements PagedFile
{
    private final PagedFile delegate;
    private final Adversary adversary;

    AdversarialPagedFile( PagedFile delegate, Adversary adversary )
    {
        this.delegate = Objects.requireNonNull( delegate );
        this.adversary = Objects.requireNonNull( adversary );
    }

    @Override
    public PageCursor io( long pageId, int pf_flags ) throws IOException
    {
        adversary.injectFailure( IllegalStateException.class );
        PageCursor pageCursor = delegate.io( pageId, pf_flags );
        if ( (pf_flags & PF_SHARED_LOCK) == PF_SHARED_LOCK )
        {
            return new AdversarialReadPageCursor( pageCursor, adversary );
        }
        return new AdversarialWritePageCursor( pageCursor, adversary );
    }

    @Override
    public int pageSize()
    {
        return delegate.pageSize();
    }

    @Override
    public void flushAndForce() throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class );
        delegate.flushAndForce();
    }

    @Override
    public long getLastPageId() throws IOException
    {
        adversary.injectFailure( IllegalStateException.class );
        return delegate.getLastPageId();
    }

    @Override
    public void close() throws IOException
    {
        adversary.injectFailure( FileNotFoundException.class, IOException.class, SecurityException.class );
        delegate.close();
    }
}
