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
package org.neo4j.legacy.consistency.checking.full;

import org.junit.Test;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.BoundedIterable;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static java.util.Arrays.asList;

public class RecordScannerTest
{
    @Test
    public void shouldProcessRecordsAndUpdateProgress() throws Exception
    {
        // given
        ProgressMonitorFactory.MultiPartBuilder progressBuilder = mock( ProgressMonitorFactory.MultiPartBuilder.class );
        ProgressListener progressListener = mock( ProgressListener.class );
        when( progressBuilder.progressForPart( anyString(), anyLong() ) ).thenReturn( progressListener );

        @SuppressWarnings("unchecked")
        BoundedIterable<Integer> store = mock( BoundedIterable.class );

        when( store.iterator() ).thenReturn( asList( 42, 75, 192 ).iterator() );

        @SuppressWarnings("unchecked")
        RecordProcessor<Integer> recordProcessor = mock( RecordProcessor.class );

        RecordScanner scanner = new RecordScanner<>( store, "our test task", progressBuilder, recordProcessor );

        // when
        scanner.run();

        // then
        verify( recordProcessor ).process( 42 );
        verify( recordProcessor ).process( 75 );
        verify( recordProcessor ).process( 192 );
        verify( recordProcessor ).close();

        verify( store ).close();

        verify( progressListener ).set( 0 );
        verify( progressListener ).set( 1 );
        verify( progressListener ).set( 2 );
        verify( progressListener ).done();
    }
}
