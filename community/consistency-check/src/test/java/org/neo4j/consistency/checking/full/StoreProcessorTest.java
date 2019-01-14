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
package org.neo4j.consistency.checking.full;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.test.rule.NeoStoresRule;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StoreProcessorTest
{
    @Rule
    public final NeoStoresRule stores = new NeoStoresRule( getClass(), StoreType.NODE, StoreType.NODE_LABEL );

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldProcessAllTheRecordsInAStore() throws Exception
    {
        // given
        RecordStore<NodeRecord> nodeStore = stores.builder().build().getNodeStore();
        ConsistencyReport.Reporter reporter = mock( ConsistencyReport.Reporter.class );
        StoreProcessor processor = new StoreProcessor( CheckDecorator.NONE,
                reporter, Stage.SEQUENTIAL_FORWARD, CacheAccess.EMPTY );
        nodeStore.updateRecord( node( 0, false, 0, 0 ) );
        nodeStore.updateRecord( node( 1, false, 0, 0 ) );
        nodeStore.updateRecord( node( 2, false, 0, 0 ) );
        nodeStore.setHighestPossibleIdInUse( 2 );

        // when
        processor.applyFiltered( nodeStore );

        // then
        verify( reporter, times( 3 ) ).forNode( any( NodeRecord.class ), any( RecordCheck.class ) );
    }

    private NodeRecord node( long id, boolean dense, long nextRel, long nextProp )
    {
        return new NodeRecord( id ).initialize( true, nextProp, dense, nextRel, 0 );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldStopProcessingRecordsWhenSignalledToStop() throws Exception
    {
        // given
        ConsistencyReport.Reporter reporter = mock( ConsistencyReport.Reporter.class );
        StoreProcessor processor = new StoreProcessor( CheckDecorator.NONE,
                reporter, Stage.SEQUENTIAL_FORWARD, CacheAccess.EMPTY );
        RecordStore<NodeRecord> nodeStore = new RecordStore.Delegator<NodeRecord>(
                stores.builder().build().getNodeStore() )
        {
            @Override
            public RecordCursor<NodeRecord> newRecordCursor( NodeRecord record )
            {
                return new RecordCursor.Delegator<NodeRecord>( super.newRecordCursor( record ) )
                {
                    @Override
                    public boolean next( long id )
                    {
                        if ( id == 3 )
                        {
                            processor.stop();
                        }
                        return super.next( id );
                    }
                };
            }
        };
        nodeStore.updateRecord( node( 0, false, 0, 0 ) );
        nodeStore.updateRecord( node( 1, false, 0, 0 ) );
        nodeStore.updateRecord( node( 2, false, 0, 0 ) );
        nodeStore.updateRecord( node( 3, false, 0, 0 ) );
        nodeStore.updateRecord( node( 4, false, 0, 0 ) );
        nodeStore.setHighestPossibleIdInUse( 4 );

        // when
        processor.applyFiltered( nodeStore );

        // then
        verify( reporter, times( 3 ) ).forNode( any( NodeRecord.class ), any( RecordCheck.class ) );
    }
}
