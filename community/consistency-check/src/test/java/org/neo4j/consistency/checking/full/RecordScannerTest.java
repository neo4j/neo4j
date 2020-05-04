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
package org.neo4j.consistency.checking.full;

import org.junit.jupiter.api.Test;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RecordScannerTest
{
    @Test
    void shouldProcessRecordsSequentiallyAndUpdateProgress() throws Exception
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder progressBuilder = mock( ProgressMonitorFactory.MultiPartBuilder.class );
        ProgressListener progressListener = mock( ProgressListener.class );
        when( progressBuilder.progressForPart( anyString(), anyLong() ) ).thenReturn( progressListener );

        @SuppressWarnings( "unchecked" )
        BoundedIterable<Integer> store = mock( BoundedIterable.class );

        when( store.iterator() ).thenReturn( asList( 42, 75, 192 ).iterator() );

        @SuppressWarnings( "unchecked" )
        RecordProcessor<Integer> recordProcessor = mock( RecordProcessor.class );

        RecordScanner<Integer> scanner = new SequentialRecordScanner<>( "our test task", Statistics.NONE, 1, store,
                progressBuilder, recordProcessor, PageCacheTracer.NULL );

        // when
        scanner.run();

        // then
        verifyProcessCloseAndDone( recordProcessor, store, progressListener );
    }

    @Test
    void shouldProcessRecordsParallelAndUpdateProgress() throws Exception
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder progressBuilder = mock( ProgressMonitorFactory.MultiPartBuilder.class );
        ProgressListener progressListener = mock( ProgressListener.class );
        when( progressBuilder.progressForPart( anyString(), anyLong() ) ).thenReturn( progressListener );

        @SuppressWarnings( "unchecked" )
        BoundedIterable<Integer> store = mock( BoundedIterable.class );

        when( store.iterator() ).thenReturn( asList( 42, 75, 192 ).iterator() );

        @SuppressWarnings( "unchecked" )
        RecordProcessor<Integer> recordProcessor = mock( RecordProcessor.class );

        RecordScanner<Integer> scanner = new ParallelRecordScanner<>( "our test task", Statistics.NONE, 1, store,
                progressBuilder, recordProcessor, CacheAccess.EMPTY, QueueDistribution.ROUND_ROBIN, PageCacheTracer.NULL );

        // when
        scanner.run();

        // then
        verifyProcessCloseAndDone( recordProcessor, store, progressListener );
    }

    private static void verifyProcessCloseAndDone( RecordProcessor<Integer> recordProcessor, BoundedIterable<Integer> store, ProgressListener progressListener )
            throws Exception
    {
        verify( recordProcessor ).process( eq( 42 ), any() );
        verify( recordProcessor ).process( eq( 75 ), any() );
        verify( recordProcessor ).process( eq( 192 ), any() );
        verify( recordProcessor ).close();

        verify( store ).close();

        verify( progressListener, times( 3 ) ).add( 1 );
        verify( progressListener ).done();
    }
}
