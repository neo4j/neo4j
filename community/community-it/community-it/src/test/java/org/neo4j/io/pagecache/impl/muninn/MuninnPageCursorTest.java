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
package org.neo4j.io.pagecache.impl.muninn;

import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.DelegatingPageSwapper;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.tracing.cursor.CursorContext.NULL;

@TestDirectoryExtension
class MuninnPageCursorTest
{
    @Inject
    private TestDirectory directory;
    @Inject
    private FileSystemAbstraction fs;
    private JobScheduler jobScheduler;
    private final LifeSupport life = new LifeSupport();

    @BeforeEach
    private void start()
    {
        jobScheduler = JobSchedulerFactory.createScheduler();
        life.add( jobScheduler );
        life.start();
    }

    @AfterEach
    private void stop()
    {
        life.shutdown();
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldUnlockLatchOnPageFaultingWhenConcurrentlyCursorClosed( boolean alsoThrowOnPageFaultRead ) throws IOException
    {
        // given
        Path file = directory.file( "dude" );
        createSomeData( file );

        // when
        AtomicReference<PageCursor> cursorHolder = new AtomicReference<>();
        Runnable onReadAction = () ->
        {
            PageCursor cursor = cursorHolder.get();
            if ( cursor != null )
            {
                cursor.close();
                if ( alsoThrowOnPageFaultRead )
                {
                    // Alternatively also throw here to cause the problem happening in a slightly different place.
                    throw new RuntimeException();
                }
            }
        };
        try ( PageCache pageCache = startPageCache( customSwapper( defaultPageSwapperFactory(), onReadAction ) );
                PagedFile pagedFile = pageCache.map( file, PageCache.PAGE_SIZE, DEFAULT_DATABASE_NAME, Sets.immutable.of( StandardOpenOption.CREATE ) ) )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, NULL ) )
            {
                cursorHolder.set( cursor ); // enabling the failing behaviour
                assertThrows( NullPointerException.class, cursor::next );
                cursorHolder.set( null ); // disabling this failing behaviour
            }

            // then hopefully the latch is not jammed. Assert that we can read normally from this new cursor.
            try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, NULL ) )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    cursor.next( i );
                    for ( int j = 0; j < 100; j++ )
                    {
                        assertEquals( j, cursor.getLong() );
                    }
                }
            }
        }
    }

    private PageCache startPageCache( PageSwapperFactory pageSwapperFactory )
    {
        return new MuninnPageCache( pageSwapperFactory, jobScheduler, MuninnPageCache.config( 1_000 ) );
    }

    private void createSomeData( Path file ) throws IOException
    {
        try ( PageCache pageCache = startPageCache( defaultPageSwapperFactory() );
                PagedFile pagedFile = pageCache.map( file, PageCache.PAGE_SIZE, DEFAULT_DATABASE_NAME, Sets.immutable.of( StandardOpenOption.CREATE ) );
                PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, NULL ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                cursor.next( i );
                for ( int j = 0; j < 100; j++ )
                {
                    cursor.putLong( j );
                }
            }
        }
    }

    private PageSwapperFactory customSwapper( PageSwapperFactory actual, Runnable onRead )
    {
        return new PageSwapperFactory()
        {
            @Override
            public PageSwapper createPageSwapper( Path path, int filePageSize, PageEvictionCallback onEviction, boolean createIfNotExist, boolean useDirectIO,
                    IOController ioController, SwapperSet swappers ) throws IOException
            {
                PageSwapper actualSwapper = actual.createPageSwapper( path, filePageSize, onEviction, createIfNotExist, useDirectIO, ioController, swappers );
                return new DelegatingPageSwapper( actualSwapper )
                {
                    @Override
                    public long read( long filePageId, long bufferAddress ) throws IOException
                    {
                        onRead.run();
                        return super.read( filePageId, bufferAddress );
                    }
                };
            }
        };
    }

    private PageSwapperFactory defaultPageSwapperFactory()
    {
        return new SingleFilePageSwapperFactory( fs );
    }
}
