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
package org.neo4j.kernel.impl.transaction.state;

import org.eclipse.collections.impl.block.factory.primitive.IntPredicates;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Supplier;

import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageReader;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.state.storeview.PropertyAwareEntityStoreScan;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PropertyAwareEntityStoreScanTest
{
    private final LockService locks = mock( LockService.class, RETURNS_MOCKS );
    private final NodeStore nodeStore = mock( NodeStore.class );
    private final PropertyStore propertyStore = mock( PropertyStore.class );
    private final NeoStores neoStores = mock( NeoStores.class );

    @Before
    public void before()
    {
        when( neoStores.getNodeStore() ).thenReturn( nodeStore );
        when( neoStores.getPropertyStore() ).thenReturn( propertyStore );
    }

    @Test
    public void shouldGiveBackCompletionPercentage()
    {
        // given
        long total = 10;
        when( nodeStore.getHighId() ).thenReturn( total );
        NodeRecord emptyRecord = new NodeRecord( 0 );
        NodeRecord inUseRecord = new NodeRecord( 42 );
        inUseRecord.setInUse( true );
        when( nodeStore.newRecord() ).thenReturn( emptyRecord );
        when( nodeStore.getRecord( anyLong(), any( NodeRecord.class ), any( RecordLoad.class ) ) ).thenReturn( inUseRecord, inUseRecord, inUseRecord,
                inUseRecord, inUseRecord, inUseRecord, inUseRecord, inUseRecord, inUseRecord, inUseRecord );

        final PercentageSupplier percentageSupplier = new PercentageSupplier();
        final PropertyAwareEntityStoreScan<StorageNodeCursor,RuntimeException> scan =
                new PropertyAwareEntityStoreScan<StorageNodeCursor,RuntimeException>( new RecordStorageReader( neoStores ), total, IntPredicates.alwaysTrue(),
                        id -> locks.acquireNodeLock( id, LockService.LockType.READ_LOCK ) )
                {
                    private int read;

                    @Override
                    public boolean process( StorageNodeCursor node )
                    {
                        // then
                        read++;
                        float expected = (float) read / total;
                        float actual = percentageSupplier.get();
                        assertEquals( String.format( "%f==%f", expected, actual ), expected, actual, 0.0 );
                        return false;
                    }

                    @Override
                    protected StorageNodeCursor allocateCursor( StorageReader storageReader )
                    {
                        return storageReader.allocateNodeCursor();
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

        void setStoreScan( StoreScan<?> storeScan )
        {
            this.storeScan = storeScan;
        }
    }
}
