/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.index.internal.gbptree;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@PageCacheExtension
public class FreeListIdProviderTracersTest
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;
    private final DefaultPageCacheTracer cacheTracer = new DefaultPageCacheTracer();

    @Test
    void trackPageCacheAccessOnInitialize() throws IOException
    {
        var cursorTracer = cacheTracer.createPageCursorTracer( "trackPageCacheAccessOnInitialize" );
        assertZeroCursor( cursorTracer );

        try ( var freeListFile = pageCache.map( testDirectory.createFilePath( "init" ), pageCache.pageSize() ) )
        {
            FreeListIdProvider listIdProvider = new FreeListIdProvider( freeListFile, 0 );
            listIdProvider.initializeAfterCreation( cursorTracer );
        }

        assertOneCursor( cursorTracer );
    }

    @Test
    void trackPageCacheAccessOnNewIdGeneration() throws IOException
    {
        var cursorTracer = cacheTracer.createPageCursorTracer( "trackPageCacheAccessOnNewIdGeneration" );
        assertZeroCursor( cursorTracer );

        try ( var freeListFile = pageCache.map( testDirectory.createFilePath( "newId" ), pageCache.pageSize() ) )
        {
            FreeListIdProvider listIdProvider = new FreeListIdProvider( freeListFile, 0 );
            listIdProvider.acquireNewId( 1, 1, cursorTracer );
        }

        assertOneCursor( cursorTracer );
    }

    @Test
    void trackPageCacheAccessOnIdReleaseOnTheSamePage() throws IOException
    {
        var cursorTracer = cacheTracer.createPageCursorTracer( "trackPageCacheAccessOnIdReleaseOnTheSamePage" );
        assertZeroCursor( cursorTracer );

        try ( var freeListFile = pageCache.map( testDirectory.createFilePath( "releaseId" ), pageCache.pageSize() ) )
        {
            FreeListIdProvider listIdProvider = new FreeListIdProvider( freeListFile, 0 );
            listIdProvider.releaseId( 1, 1,42,  cursorTracer );
        }

        assertOneCursor( cursorTracer );
    }

    @Test
    void trackPageCacheAccessOnIdReleaseOnDifferentPage() throws IOException
    {
        var cursorTracer = cacheTracer.createPageCursorTracer( "trackPageCacheAccessOnIdReleaseOnDifferentPage" );
        assertZeroCursor( cursorTracer );

        try ( var freeListFile = pageCache.map( testDirectory.createFilePath( "differentReleaseId" ), pageCache.pageSize() ) )
        {
            FreeListIdProvider listIdProvider = new FreeListIdProvider( freeListFile, 0 );
            listIdProvider.initialize( 0, 1, 0, listIdProvider.entriesPerPage() - 1, 0 );
            listIdProvider.releaseId( 1, 1,42,  cursorTracer );
            assertEquals( 0, listIdProvider.writePos() );
        }

        assertThat( cursorTracer.pins() ).isEqualTo( 3 );
        assertThat( cursorTracer.unpins() ).isEqualTo( 3 );
        assertThat( cursorTracer.hits() ).isEqualTo( 1 );
        assertThat( cursorTracer.faults() ).isEqualTo( 2 );
    }

    @Test
    void trackPageCacheAccessOnFreeListTraversal() throws IOException
    {
        var cursorTracer = cacheTracer.createPageCursorTracer( "trackPageCacheAccessOnFreeListTraversal" );
        assertZeroCursor( cursorTracer );

        try ( var freeListFile = pageCache.map( testDirectory.createFilePath( "traversal" ), pageCache.pageSize() ) )
        {
            FreeListIdProvider listIdProvider = new FreeListIdProvider( freeListFile, 0 );
            listIdProvider.initialize( 100, 0, 1, listIdProvider.entriesPerPage() - 1, 0 );
            listIdProvider.releaseId( 1, 1,42,  NULL );
            assertEquals( 0, listIdProvider.writePos() );

            listIdProvider.visitFreelist( new IdProvider.IdProviderVisitor.Adaptor(), cursorTracer );
        }

        assertThat( cursorTracer.pins() ).isEqualTo( 3 );
        assertThat( cursorTracer.unpins() ).isEqualTo( 3 );
        assertThat( cursorTracer.hits() ).isEqualTo( 3 );
    }

    private void assertOneCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.pins() ).isOne();
        assertThat( cursorTracer.unpins() ).isOne();
        assertThat( cursorTracer.faults() ).isOne();
    }

    private void assertZeroCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.hits() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
        assertThat( cursorTracer.faults() ).isZero();
    }
}
