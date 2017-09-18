/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import java.util.function.Supplier;

import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.state.storeview.NodeStoreScan;
import org.neo4j.storageengine.api.schema.PopulationProgress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeStoreScanTest
{
    private final LockService locks = mock( LockService.class, RETURNS_MOCKS );
    private final NodeStore nodeStore = mock( NodeStore.class );

    @Test
    public void shouldGiveBackCompletionPercentage() throws Throwable
    {
        // given
        final int total = 10;
        when( nodeStore.getHighId() ).thenReturn( (long) total );
        NodeRecord inUseRecord = new NodeRecord( 42 );
        inUseRecord.setInUse( true );
        when( nodeStore.getRecord( anyLong(), any( NodeRecord.class ), any( RecordLoad.class ) ) ).thenReturn(
                inUseRecord, inUseRecord, inUseRecord, inUseRecord,
                inUseRecord, inUseRecord, inUseRecord, inUseRecord, inUseRecord, inUseRecord );

        final PercentageSupplier percentageSupplier = new PercentageSupplier();

        final NodeStoreScan<RuntimeException> scan = new NodeStoreScan<RuntimeException>( nodeStore, locks,  total )
        {
            private int read;

            @Override
            public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, IndexEntryUpdate<?> update,
                    long currentlyIndexedNodeId )
            {
                // no-op
            }

            @Override
            public void process( NodeRecord node )
            {
                // then
                read++;
                float expected = (float) read / total;
                float actual = percentageSupplier.get();
                assertEquals( String.format( "%f==%f",  expected, actual ), expected, actual, 0.0 );
            }
        };
        percentageSupplier.setStoreScan( scan );

        // when
        scan.run();
    }

    private static class PercentageSupplier implements Supplier<Float>
    {
        private StoreScan<?> storeScan;

        @Override
        public Float get()
        {
            assertNotNull( storeScan );
            PopulationProgress progress = storeScan.getProgress();
            return (float) progress.getCompleted() / (float) progress.getTotal();
        }

        public void setStoreScan( StoreScan<?> storeScan )
        {
            this.storeScan = storeScan;
        }
    }
}
