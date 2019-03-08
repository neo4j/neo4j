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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.junit.Before;
import org.junit.Test;

import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.index.label.AllEntriesLabelScanReader;
import org.neo4j.internal.index.label.LabelScanReader;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.lock.LockService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StubStorageCursors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamicIndexStoreViewTest
{
    private final LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private final StubStorageCursors cursors = new StubStorageCursors();
    private final Visitor<EntityUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );
    private final Visitor<NodeLabelUpdate,Exception> labelUpdateVisitor = mock( Visitor.class );
    private final IntPredicate propertyKeyIdFilter = mock( IntPredicate.class );
    private final AllEntriesLabelScanReader nodeLabelRanges = mock( AllEntriesLabelScanReader.class );

    @Before
    public void setUp()
    {
        when( labelScanStore.allNodeLabelRanges()).thenReturn( nodeLabelRanges );
    }

    @Test
    public void visitOnlyLabeledNodes() throws Exception
    {
        LabelScanReader labelScanReader = mock( LabelScanReader.class );
        when( labelScanStore.newReader() ).thenReturn( labelScanReader );
        when( nodeLabelRanges.maxCount() ).thenReturn( 1L );

        long[] nodeIds = {1, 2, 3, 4, 5, 6, 7, 8};
        PrimitiveLongResourceIterator labeledNodesIterator = PrimitiveLongResourceCollections.iterator( null, nodeIds );
        when( labelScanReader.nodesWithAnyOfLabels( new int[] {2, 6} ) ).thenReturn( labeledNodesIterator );
        for ( long nodeId : nodeIds )
        {
            cursors.withNode( nodeId ).propertyId( 1 ).relationship( 1 ).labels( 2, 6 );
        }
        // Create a couple of more nodes, just lying around
        for ( long i = 0, id = nodeIds[nodeIds.length - 1] + 1; i < 10; i++ )
        {
            cursors.withNode( id );
        }

        DynamicIndexStoreView storeView = dynamicIndexStoreView();
        StoreScan<Exception> storeScan = storeView.visitNodes( new int[]{2, 6}, propertyKeyIdFilter, propertyUpdateVisitor, labelUpdateVisitor, false );
        storeScan.run();

        verify( labelUpdateVisitor, times( nodeIds.length ) ).visit( any() );
    }

    private DynamicIndexStoreView dynamicIndexStoreView()
    {
        LockService locks = LockService.NO_LOCK_SERVICE;
        Supplier<StorageReader> storageReaderSupplier = () -> cursors;
        return new DynamicIndexStoreView( new NeoStoreIndexStoreView( locks, storageReaderSupplier ), labelScanStore,
                locks, storageReaderSupplier, NullLogProvider.getInstance() );
    }
}
