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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.legacy.consistency.checking.CheckDecorator;
import org.neo4j.legacy.consistency.report.ConsistencyReport;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StoreProcessorTest
{
    @SuppressWarnings("unchecked")
    @Test
    public void shouldProcessAllTheRecordsInAStore() throws Exception
    {
        // given
        StoreProcessor processor = new StoreProcessor( CheckDecorator.NONE, mock( ConsistencyReport.Reporter.class ) );
        RecordStore<NodeRecord> recordStore = mock( RecordStore.class );
        when( recordStore.getHighId() ).thenReturn( 3L );
        when( recordStore.forceGetRecord( any( Long.class ) ) ).thenReturn( new NodeRecord( 0, false, 0, 0 ) );

        // when
        processor.applyFiltered( recordStore );

        // then
        verify( recordStore ).forceGetRecord( 0 );
        verify( recordStore ).forceGetRecord( 1 );
        verify( recordStore ).forceGetRecord( 2 );
        verify( recordStore, never() ).forceGetRecord( 3 );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldStopProcessingRecordsWhenSignalledToStop() throws Exception
    {
        // given
        final StoreProcessor processor = new StoreProcessor( CheckDecorator.NONE, mock( ConsistencyReport.Reporter.class ) );
        RecordStore<NodeRecord> recordStore = mock( RecordStore.class );
        when( recordStore.getHighId() ).thenReturn( 4L );
        when( recordStore.forceGetRecord( 0L ) ).thenReturn( new NodeRecord( 0, false, 0, 0 ) );
        when( recordStore.forceGetRecord( 1L ) ).thenReturn( new NodeRecord( 0, false, 0, 0 ) );
        when( recordStore.forceGetRecord( 2L ) ).thenAnswer( new Answer<NodeRecord>()
        {
            @Override
            public NodeRecord answer( InvocationOnMock invocation ) throws Throwable
            {
                processor.stop();
                return new NodeRecord( 2, true, false, 0, 0, 0 );
            }
        } );

        // when
        processor.applyFiltered( recordStore );

        // then
        verify( recordStore ).forceGetRecord( 0 );
        verify( recordStore ).forceGetRecord( 1 );
        verify( recordStore ).forceGetRecord( 2 );
        verify( recordStore, never() ).forceGetRecord( 3 );
    }
}
