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
package org.neo4j.index.internal.gbptree;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.helpers.Exceptions;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.StubPagedFile;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.CrashGenerationCleaner.MAX_BATCH_SIZE;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;

public class CrashGenerationCleanerCrashTest
{
    @Rule
    public PageCacheAndDependenciesRule store = new PageCacheAndDependenciesRule();

    @Test
    public void mustNotLeakTasksOnCrash()
    {
        // Given
        String exceptionMessage = "When there's no more room in hell, the dead will walk the earth";
        CrashGenerationCleaner cleaner = newCrashingCrashGenerationCleaner( exceptionMessage );
        ExecutorService executorService = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );

        try
        {
            // When
            cleaner.clean( executorService );
            fail( "Expected to throw" );
        }
        catch ( Throwable e )
        {
            Throwable rootCause = Exceptions.rootCause( e );
            assertTrue( rootCause instanceof IOException );
            assertEquals( exceptionMessage, rootCause.getMessage() );
        }
        finally
        {
            // Then
            List<Runnable> tasks = executorService.shutdownNow();
            assertEquals( 0, tasks.size() );
        }
    }

    private CrashGenerationCleaner newCrashingCrashGenerationCleaner( String message )
    {
        int pageSize = 8192;
        PagedFile pagedFile = new StubPagedFile( pageSize )
        {
            AtomicBoolean first = new AtomicBoolean( true );

            @Override
            public PageCursor io( long pageId, int pf_flags ) throws IOException
            {
                try
                {
                    Thread.sleep( 1 );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                if ( first.getAndSet( false ) )
                {
                    throw new IOException( message );
                }
                return super.io( pageId, pf_flags );
            }
        };
        return new CrashGenerationCleaner( pagedFile, new TreeNodeFixedSize<>( pageSize, SimpleLongLayout.longLayout().build() ), 0,
                MAX_BATCH_SIZE * 1_000_000_000, 5, 7, NO_MONITOR );
    }
}
