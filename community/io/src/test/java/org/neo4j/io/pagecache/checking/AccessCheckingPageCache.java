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
package org.neo4j.io.pagecache.checking;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.Optional;

import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

/**
 * Wraps a {@link PageCache} and ensures that read {@link PageCursor} i.e. page cursors which are created
 * with {@link PagedFile#PF_SHARED_READ_LOCK}, only read data inside {@link PageCursor#shouldRetry() do-shouldRetry}
 * loop. It does so by raising a flag on the e.g. {@code getInt} methods, reseting that flag in
 * {@link PageCursor#shouldRetry()} and asserting that flag not being cleared with doing
 * {@link PageCursor#next()}, {@link PageCursor#next(long)} and {@link PageCursor#close()}.
 */
public class AccessCheckingPageCache extends DelegatingPageCache
{
    public AccessCheckingPageCache( PageCache delegate )
    {
        super( delegate );
    }

    @Override
    public PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException
    {
        return new AccessCheckingPagedFile( super.map( file, pageSize, openOptions ) );
    }

    @Override
    public Optional<PagedFile> getExistingMapping( File file ) throws IOException
    {
        return super.getExistingMapping( file );
    }
}
