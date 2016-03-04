/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RecordStoreUtil.NewNodeRecordAnswer;
import org.neo4j.kernel.impl.store.RecordStoreUtil.ReadNodeAnswer;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StoreProcessorTest
{
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldProcessAllTheRecordsInAStore() throws Exception
    {
        // given
        StoreProcessor processor = new StoreProcessor( CheckDecorator.NONE,
                mock( ConsistencyReport.Reporter.class ), Stage.SEQUENTIAL_FORWARD, CacheAccess.EMPTY );
        RecordStore<NodeRecord> recordStore = mock( RecordStore.class );
        when( recordStore.newRecord() ).thenAnswer( new NewNodeRecordAnswer() );
        when( recordStore.getHighId() ).thenReturn( 3L );
        when( recordStore.getRecord( anyLong(), any( NodeRecord.class ), any( RecordLoad.class ) ) )
                .thenAnswer( new ReadNodeAnswer( false, 0, 0 ) );

        // when
        processor.applyFiltered( recordStore );

        // then
        verify( recordStore ).getRecord( eq( 0L ), any( NodeRecord.class ), any( RecordLoad.class ) );
        verify( recordStore ).getRecord( eq( 1L ), any( NodeRecord.class ), any( RecordLoad.class ) );
        verify( recordStore ).getRecord( eq( 2L ), any( NodeRecord.class ), any( RecordLoad.class ) );
        verify( recordStore, never() ).getRecord( eq( 3L ), any( NodeRecord.class ), any( RecordLoad.class ) );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldStopProcessingRecordsWhenSignalledToStop() throws Exception
    {
        // given
        final StoreProcessor processor = new StoreProcessor( CheckDecorator.NONE,
                mock( ConsistencyReport.Reporter.class ), Stage.SEQUENTIAL_FORWARD, CacheAccess.EMPTY );
        RecordStore<NodeRecord> recordStore = mock( RecordStore.class );
        when( recordStore.newRecord() ).thenAnswer( new NewNodeRecordAnswer() );
        when( recordStore.getHighId() ).thenReturn( 4L );
        when( recordStore.getRecord( eq( 0L ), any( NodeRecord.class ), any( RecordLoad.class ) ) )
                .thenAnswer( new ReadNodeAnswer( false, 0, 0 ) );
        when( recordStore.getRecord( eq( 1L ), any( NodeRecord.class ), any( RecordLoad.class ) ) )
                .thenAnswer( new ReadNodeAnswer( false, 0, 0 ) );
        when( recordStore.getRecord( eq( 2L ), any( NodeRecord.class ), any( RecordLoad.class ) ) ).thenAnswer(
                new ReadNodeAnswer( false, 0, 0 )
                {
                    @Override
                    public NodeRecord answer( InvocationOnMock invocation ) throws Throwable
                    {
                        processor.stop();
                        return super.answer( invocation );
                    }
                } );

        // when
        processor.applyFiltered( recordStore );

        // then
        verify( recordStore ).getRecord( eq( 0L ), any( NodeRecord.class ), any( RecordLoad.class ) );
        verify( recordStore ).getRecord( eq( 1L ), any( NodeRecord.class ), any( RecordLoad.class ) );
        verify( recordStore ).getRecord( eq( 2L ), any( NodeRecord.class ), any( RecordLoad.class ) );
        verify( recordStore, never() ).getRecord( eq( 3L ), any( NodeRecord.class ), any( RecordLoad.class ) );
    }
}
