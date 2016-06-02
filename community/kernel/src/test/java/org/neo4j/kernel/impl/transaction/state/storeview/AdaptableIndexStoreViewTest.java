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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class AdaptableIndexStoreViewTest
{

    private LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private NeoStores neoStores = mock( NeoStores.class );
    private NodeStore nodeStore = mock( NodeStore.class );
    private CountsTracker countStore = mock( CountsTracker.class );
    private Visitor<NodePropertyUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );
    private Visitor<NodeLabelUpdate,Exception> labelUpdateVisitor = mock( Visitor.class );
    private IntPredicate propertyKeyIdFilter = mock( IntPredicate.class );

    @Before
    public void setUp()
    {
        NodeRecord nodeRecord = getNodeRecord();
        when( neoStores.getCounts() ).thenReturn( countStore );
        when( neoStores.getNodeStore() ).thenReturn( nodeStore );
        when( nodeStore.newRecord() ).thenReturn( nodeRecord );
        when( nodeStore.getRecord( anyLong(), any( NodeRecord.class ), any( RecordLoad.class ) ) ).thenReturn( nodeRecord );
    }

    @Test
    public void visitAllNodesWhenThresholdReached() throws Exception
    {
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( 10L );
        when( nodeStore.getHighId() ).thenReturn( 10L );

        mockLabelNodeCount( countStore, 1 );
        mockLabelNodeCount( countStore, 2 );
        mockLabelNodeCount( countStore, 3 );

        AdaptableIndexStoreView storeView =
                new AdaptableIndexStoreView( labelScanStore, LockService.NO_LOCK_SERVICE, neoStores );

        StoreScan<Exception> storeScan = storeView
                .visitNodes( new int[]{1, 2, 3}, propertyKeyIdFilter, propertyUpdateVisitor, labelUpdateVisitor );

        storeScan.run();

        Mockito.verify( nodeStore, times( 10 ) )
                .getRecord( anyLong(), any( NodeRecord.class ), any( RecordLoad.class ) );
    }

    @Test
    public void visitOnlyLabeledNodesWhenThresholdNotReached() throws Exception
    {
        LabelScanReader labelScanReader = mock( LabelScanReader.class );
        when( labelScanStore.newReader() ).thenReturn( labelScanReader );

        PrimitiveLongIterator labeledNodesIterator = PrimitiveLongCollections.iterator( 1, 2, 3, 4, 5, 6, 7, 8 );
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( 20L );
        when( nodeStore.getHighId() ).thenReturn( 20L );
        when( labelScanReader.getMinIndexedNodeId() ).thenReturn( 20L );
        when( labelScanReader.nodesWithAnyOfLabels( 2, 6)).thenReturn( labeledNodesIterator );

        mockLabelNodeCount( countStore, 2 );
        mockLabelNodeCount( countStore, 6 );

        AdaptableIndexStoreView storeView =
                new AdaptableIndexStoreView( labelScanStore, LockService.NO_LOCK_SERVICE, neoStores );

        StoreScan<Exception> storeScan = storeView
                .visitNodes( new int[]{2, 6}, propertyKeyIdFilter, propertyUpdateVisitor, labelUpdateVisitor );

        storeScan.run();

        Mockito.verify( nodeStore, times( 8 ) )
                .getRecord( anyLong(), any( NodeRecord.class ), any( RecordLoad.class ) );
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