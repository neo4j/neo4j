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

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.impl.block.factory.primitive.IntPredicates;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.transaction.state.storeview.PropertyAwareEntityStoreScan;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StubStorageCursors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

class PropertyAwareEntityStoreScanTest
{
    private final LockService locks = mock( LockService.class, RETURNS_MOCKS );
    private final StubStorageCursors cursors = new StubStorageCursors();

    @Test
    void shouldGiveBackCompletionPercentage()
    {
        // given
        long total = 10;
        for ( long i = 0; i < total; i++ )
        {
            cursors.withNode( i );
        }

        MutableInt read = new MutableInt();
        final PercentageSupplier percentageSupplier = new PercentageSupplier();
        final PropertyAwareEntityStoreScan<StorageNodeCursor,RuntimeException> scan =
                new PropertyAwareEntityStoreScan<StorageNodeCursor,RuntimeException>( cursors, total, IntPredicates.alwaysTrue(),
                        id -> locks.acquireNodeLock( id, LockService.LockType.READ_LOCK ) )
                {
                    @Override
                    public boolean process( StorageNodeCursor node )
                    {
                        // then
                        read.incrementAndGet();
                        float expected = (float) read.intValue() / total;
                        float actual = percentageSupplier.get();
                        assertEquals( expected, actual, 0.0, String.format( "%f==%f", expected, actual ) );
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

        // then
        assertEquals( total, read.intValue() );
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
