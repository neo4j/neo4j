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
import org.mockito.Mockito;

import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.NodeUpdates;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class DynamicIndexStoreViewTest
{
    private final LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private final NeoStores neoStores = mock( NeoStores.class );
    private final NodeStore nodeStore = mock( NodeStore.class );
    private final CountsTracker countStore = mock( CountsTracker.class );
    private final Visitor<NodeUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );
    private final Visitor<NodeLabelUpdate,Exception> labelUpdateVisitor = mock( Visitor.class );
    private final IntPredicate propertyKeyIdFilter = mock( IntPredicate.class );
    private final AllEntriesLabelScanReader nodeLabelRanges = mock( AllEntriesLabelScanReader.class );

    @Before
    public void setUp()
    {
        NodeRecord nodeRecord = getNodeRecord();
        when( labelScanStore.allNodeLabelRanges()).thenReturn( nodeLabelRanges );
        when( neoStores.getCounts() ).thenReturn( countStore );
        when( neoStores.getNodeStore() ).thenReturn( nodeStore );
        when( nodeStore.newRecord() ).thenReturn( nodeRecord );
        when( nodeStore.getRecord( anyLong(), any( NodeRecord.class ), any( RecordLoad.class ) ) ).thenReturn( nodeRecord );
    }

    @Test
    public void visitOnlyLabeledNodes() throws Exception
    {
        LabelScanReader labelScanReader = mock( LabelScanReader.class );
        when( labelScanStore.newReader() ).thenReturn( labelScanReader );
        when( nodeLabelRanges.maxCount() ).thenReturn( 1L );

        PrimitiveLongResourceIterator labeledNodesIterator = PrimitiveLongResourceCollections.iterator( null, 1, 2, 3, 4, 5, 6, 7, 8 );
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( 200L );
        when( nodeStore.getHighId() ).thenReturn( 20L );
        when( labelScanReader.nodesWithAnyOfLabels( 2, 6)).thenReturn( labeledNodesIterator );

        mockLabelNodeCount( countStore, 2 );
        mockLabelNodeCount( countStore, 6 );

        DynamicIndexStoreView storeView = dynamicIndexStoreView();

        StoreScan<Exception> storeScan = storeView
                .visitNodes( new int[]{2, 6}, propertyKeyIdFilter, propertyUpdateVisitor, labelUpdateVisitor, false );

        storeScan.run();

        Mockito.verify( nodeStore, times( 8 ) )
                .getRecord( anyLong(), any( NodeRecord.class ), any( RecordLoad.class ) );
    }

    @Test
    public void shouldBeAbleToForceStoreScan() throws Exception
    {
        when( labelScanStore.newReader() ).thenThrow( new RuntimeException( "Should not be used" ) );

        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( 200L );
        when( nodeStore.getHighId() ).thenReturn( 20L );

        mockLabelNodeCount( countStore, 2 );
        mockLabelNodeCount( countStore, 6 );

        DynamicIndexStoreView storeView = dynamicIndexStoreView();

        StoreScan<Exception> storeScan = storeView
                .visitNodes( new int[]{2, 6}, propertyKeyIdFilter, propertyUpdateVisitor, labelUpdateVisitor, true );

        storeScan.run();

        Mockito.verify( nodeStore, times( 20 ) )
                .getRecord( anyLong(), any( NodeRecord.class ), any( RecordLoad.class ) );
    }

    private DynamicIndexStoreView dynamicIndexStoreView()
    {
        LockService locks = LockService.NO_LOCK_SERVICE;
        return new DynamicIndexStoreView( new NeoStoreIndexStoreView( locks, neoStores ), labelScanStore,
                locks, neoStores, NullLogProvider.getInstance() );
    }

    private NodeRecord getNodeRecord()
    {
        NodeRecord nodeRecord = new NodeRecord( 0L );
        nodeRecord.initialize( true, 1L, false, 1L, 0L );
        return nodeRecord;
    }

    private void mockLabelNodeCount( CountsTracker countStore, int labelId )
    {
        Register.DoubleLongRegister register = Registers.newDoubleLongRegister( labelId, labelId );
        when( countStore.nodeCount( eq( labelId ), any( Register.DoubleLongRegister.class ) ) ).thenReturn( register );
    }

}
