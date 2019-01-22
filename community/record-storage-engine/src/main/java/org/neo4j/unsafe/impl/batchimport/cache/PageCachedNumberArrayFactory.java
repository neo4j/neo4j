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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;

/**
 * Factory of page cache backed number arrays.
 * @see NumberArrayFactory
 */
public class PageCachedNumberArrayFactory extends NumberArrayFactory.Adapter
{
    private final PageCache pageCache;
    private final File storeDir;

    public PageCachedNumberArrayFactory( PageCache pageCache, File storeDir )
    {
        Objects.requireNonNull( pageCache );
        this.pageCache = pageCache;
        this.storeDir = storeDir;
    }

    @Override
    public IntArray newIntArray( long length, int defaultValue, long base )
    {
        try
        {
            File tempFile = File.createTempFile( "intArray", ".tmp", storeDir );
            PagedFile pagedFile = pageCache.map( tempFile, pageCache.pageSize(), DELETE_ON_CLOSE, CREATE );
            return new PageCacheIntArray( pagedFile, length, defaultValue, base );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public LongArray newLongArray( long length, long defaultValue, long base )
    {
        try
        {
            File tempFile = File.createTempFile( "longArray", ".tmp", storeDir );
            PagedFile pagedFile = pageCache.map( tempFile, pageCache.pageSize(), DELETE_ON_CLOSE, CREATE );
            return new PageCacheLongArray( pagedFile, length, defaultValue, base );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public ByteArray newByteArray( long length, byte[] defaultValue, long base )
    {
        try
        {
            File tempFile = File.createTempFile( "byteArray", ".tmp", storeDir );
            PagedFile pagedFile = pageCache.map( tempFile, pageCache.pageSize(), DELETE_ON_CLOSE, CREATE );
            return new PageCacheByteArray( pagedFile, length, defaultValue, base );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
