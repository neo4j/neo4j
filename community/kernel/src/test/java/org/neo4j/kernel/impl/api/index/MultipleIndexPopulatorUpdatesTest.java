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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collections;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.state.storeview.AdaptableIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.LabelScanViewNodeStoreScan;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.StoreViewNodeStoreScan;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class MultipleIndexPopulatorUpdatesTest
{
    @Mock( answer = Answers.RETURNS_MOCKS )
    private LogProvider logProvider;

    @Test
    public void updateForHigherNodeAppliedWhenUsingLabelStoreScan()
            throws IndexPopulationFailedKernelException, IOException, IndexEntryConflictException
    {
        LabelScanStore labelScanStore = mock( LabelScanStore.class );
        NeoStores neoStores = Mockito.mock( NeoStores.class );
        CountsTracker countsTracker = mock( CountsTracker.class );
        NodeStore nodeStore = mock( NodeStore.class );
        LabelScanReader labelScanReader = mock( LabelScanReader.class );
        PropertyStore propertyStore = mock( PropertyStore.class );

        NodeRecord nodeRecord = getNodeRecord();

        PropertyRecord propertyRecord = getPropertyRecord();

        when( neoStores.getCounts()).thenReturn( countsTracker );
        when( neoStores.getNodeStore()).thenReturn( nodeStore );
        when( labelScanStore.newReader() ).thenReturn( labelScanReader );
        when( neoStores.getPropertyStore() ).thenReturn( propertyStore );
        when( propertyStore.getPropertyRecordChain( anyInt() ) ).thenReturn(
                Collections.singletonList( propertyRecord ) );

        when( countsTracker.nodeCount( anyInt(), any( Register.DoubleLongRegister.class ) ) )
                .thenReturn( Registers.newDoubleLongRegister( 3, 3 ) );
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( 20L );
        when( nodeStore.newRecord() ).thenReturn( nodeRecord );
        when( nodeStore.getRecord( anyInt(), eq( nodeRecord ), any( RecordLoad.class ) ) ).thenAnswer(
                new SetNodeIdRecordAnswer( nodeRecord, 1 ) );
        when( nodeStore.getRecord( eq(7L), eq( nodeRecord ), any( RecordLoad.class ) ) ).thenAnswer( new
                SetNodeIdRecordAnswer( nodeRecord, 7 ) );

        when( labelScanReader.getMinIndexedNodeId() ).thenReturn( 1L );
        when( labelScanReader.nodesWithAnyOfLabels( anyInt() ) ).thenReturn( PrimitiveLongCollections
                .iterator( 1L, 7L, 15L ) );


        ProcessListenableAdaptableIndexView
                storeView = new ProcessListenableAdaptableIndexView( labelScanStore, LockService.NO_LOCK_SERVICE, neoStores );
        MultipleIndexPopulator indexPopulator = new MultipleIndexPopulator( storeView, logProvider );

        storeView.setProcessListener( new NodeUpdateProcessListener( indexPopulator ) );

        IndexPopulator populator = createIndexPopulator();
        IndexUpdater indexUpdater = mock( IndexUpdater.class );
        when( populator.newPopulatingUpdater( storeView ) ).thenReturn( indexUpdater );

        IndexDescriptor descriptor = new IndexDescriptor( 1, 1 );
        addPopulator( indexPopulator, populator, descriptor );

        indexPopulator.create();
        StoreScan<IndexPopulationFailedKernelException> storeScan = indexPopulator.indexAllNodes();
        storeScan.run();

        Mockito.verify( indexUpdater, times( 1 ) ).process( any(NodePropertyUpdate.class) );
    }

    @Test
    public void updateForHigherNodeIgnoredWhenUsingFullNodeStoreScan()
            throws IndexPopulationFailedKernelException, IOException, IndexEntryConflictException
    {
        NeoStores neoStores = Mockito.mock( NeoStores.class );
        CountsTracker countsTracker = mock( CountsTracker.class );
        NodeStore nodeStore = mock( NodeStore.class );
        PropertyStore propertyStore = mock( PropertyStore.class );

        NodeRecord nodeRecord = getNodeRecord();

        PropertyRecord propertyRecord = getPropertyRecord();

        when( neoStores.getCounts()).thenReturn( countsTracker );
        when( neoStores.getNodeStore()).thenReturn( nodeStore );
        when( neoStores.getPropertyStore() ).thenReturn( propertyStore );
        when( propertyStore.getPropertyRecordChain( anyInt() ) ).thenReturn(
                Collections.singletonList( propertyRecord ) );

        when( countsTracker.nodeCount( anyInt(), any( Register.DoubleLongRegister.class ) ) )
                .thenReturn( Registers.newDoubleLongRegister( 3, 3 ) );
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( 20L );
        when( nodeStore.newRecord() ).thenReturn( nodeRecord );
        when( nodeStore.getRecord( anyInt(), eq( nodeRecord ), any( RecordLoad.class ) ) ).thenAnswer(
                new SetNodeIdRecordAnswer( nodeRecord, 1 ) );
        when( nodeStore.getRecord( eq(7L), eq( nodeRecord ), any( RecordLoad.class ) ) ).thenAnswer( new
                SetNodeIdRecordAnswer( nodeRecord, 7 ) );


        ProcessListenableNeoStoreIndexView
                storeView = new ProcessListenableNeoStoreIndexView( LockService.NO_LOCK_SERVICE, neoStores );
        MultipleIndexPopulator indexPopulator = new MultipleIndexPopulator( storeView, logProvider );

        storeView.setProcessListener( new NodeUpdateProcessListener( indexPopulator ) );

        IndexPopulator populator = createIndexPopulator();
        IndexUpdater indexUpdater = mock( IndexUpdater.class );
        when( populator.newPopulatingUpdater( storeView ) ).thenReturn( indexUpdater );

        IndexDescriptor descriptor = new IndexDescriptor( 1, 1 );
        addPopulator( indexPopulator, populator, descriptor );

        indexPopulator.create();
        StoreScan<IndexPopulationFailedKernelException> storeScan = indexPopulator.indexAllNodes();
        storeScan.run();

        Mockito.verify( indexUpdater, times( 0 ) ).process( any(NodePropertyUpdate.class) );
    }

    private NodeRecord getNodeRecord()
    {
        NodeRecord nodeRecord = new NodeRecord( 1L );
        nodeRecord.initialize( true, 0, false, 1, 0x0000000001L );
        InlineNodeLabels.putSorted( nodeRecord, new long[]{1}, null, null );
        return nodeRecord;
    }

    private PropertyRecord getPropertyRecord()
    {
        PropertyRecord propertyRecord = new PropertyRecord( 1 );
        PropertyBlock propertyBlock = new PropertyBlock();
        propertyBlock.setValueBlocks( new long[]{0} );
        propertyBlock.setKeyIndexId( 1 );
        propertyBlock.setSingleBlock( (0x000000000F000009L << 24) + 1 );
        propertyRecord.setPropertyBlock( propertyBlock );
        return propertyRecord;
    }

    private IndexPopulator createIndexPopulator()
    {
        IndexPopulator populator = mock( IndexPopulator.class );
        when( populator.sampleResult() ).thenReturn( new IndexSample() );
        return populator;
    }

    private MultipleIndexPopulator.IndexPopulation addPopulator( MultipleIndexPopulator multipleIndexPopulator, IndexPopulator indexPopulator,
            IndexDescriptor descriptor )
    {
        return addPopulator( multipleIndexPopulator, descriptor, indexPopulator, mock( FlippableIndexProxy.class ),
                mock( FailedIndexProxyFactory.class ) );
    }

    private MultipleIndexPopulator.IndexPopulation addPopulator(MultipleIndexPopulator multipleIndexPopulator, IndexDescriptor descriptor,
            IndexPopulator indexPopulator,
            FlippableIndexProxy flippableIndexProxy, FailedIndexProxyFactory failedIndexProxyFactory )
    {
        return multipleIndexPopulator.addPopulator( indexPopulator, descriptor,
                mock( SchemaIndexProvider.Descriptor.class ), IndexConfiguration.NON_UNIQUE,
                flippableIndexProxy, failedIndexProxyFactory, "userIndexDescription" );
    }

    private static class SetNodeIdRecordAnswer implements Answer<NodeRecord>
    {
        private final NodeRecord nodeRecord;
        private long id;

        public SetNodeIdRecordAnswer( NodeRecord nodeRecord, long id )
        {
            this.nodeRecord = nodeRecord;
            this.id = id;
        }

        @Override
        public NodeRecord answer( InvocationOnMock invocation ) throws Throwable
        {
            nodeRecord.setId( id );
            return nodeRecord;
        }
    }

    private static class NodeUpdateProcessListener implements Listener<NodeRecord>
    {
        private final MultipleIndexPopulator indexPopulator;

        public NodeUpdateProcessListener( MultipleIndexPopulator indexPopulator )
        {
            this.indexPopulator = indexPopulator;
        }

        @Override
        public void receive( NodeRecord nodeRecord )
        {
            if (nodeRecord.getId() == 7)
            {
                indexPopulator.queue( NodePropertyUpdate.change( 8L, 1, "a", new long[]{1}, "b", new long[]{1} ) );
            }
        }
    }

    private class ProcessListenableAdaptableIndexView extends AdaptableIndexStoreView
    {

        private LabelScanStore labelScanStore;
        private Listener<NodeRecord> processListener;

        public ProcessListenableAdaptableIndexView( LabelScanStore labelScanStore, LockService locks,
                NeoStores neoStores )
        {
            super( labelScanStore, locks, neoStores );
            this.labelScanStore = labelScanStore;
        }

        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter,
                Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor )
        {

            return new ListenableLabelScanViewNodeStoreScan<>( nodeStore, locks, propertyStore, labelScanStore, labelUpdateVisitor,
                    propertyUpdatesVisitor, labelIds, propertyKeyIdFilter, processListener );
        }

        public void setProcessListener( Listener<NodeRecord> processListener )
        {
            this.processListener = processListener;
        }
    }

    private class ProcessListenableNeoStoreIndexView extends NeoStoreIndexStoreView
    {

        private Listener<NodeRecord> processListener;

        public ProcessListenableNeoStoreIndexView( LockService locks, NeoStores neoStores )
        {
            super( locks, neoStores );
        }

        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter,
                Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor )
        {

            return new ListenableNodeScanViewNodeStoreScan( nodeStore, locks, propertyStore, labelUpdateVisitor,
                    propertyUpdatesVisitor, labelIds, propertyKeyIdFilter, processListener );
        }

        public void setProcessListener( Listener<NodeRecord> processListener )
        {
            this.processListener = processListener;
        }
    }

    private class ListenableLabelScanViewNodeStoreScan<FAILURE extends Exception> extends LabelScanViewNodeStoreScan<FAILURE>
    {

        private Listener<NodeRecord> processListener;

        ListenableLabelScanViewNodeStoreScan( NodeStore nodeStore, LockService locks,
                PropertyStore propertyStore, LabelScanStore labelScanStore,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
                Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor, int[] labelIds,
                IntPredicate propertyKeyIdFilter, Listener<NodeRecord> processListener )
        {
            super( nodeStore, locks, propertyStore, labelScanStore, labelUpdateVisitor, propertyUpdatesVisitor,
                    labelIds,
                    propertyKeyIdFilter );
            this.processListener = processListener;
        }


        @Override
        public void process( NodeRecord nodeRecord) throws FAILURE
        {
            processListener.receive( nodeRecord );
            super.process( nodeRecord );
        }
    }

    private class ListenableNodeScanViewNodeStoreScan<FAILURE extends Exception> extends StoreViewNodeStoreScan<FAILURE>
    {

        private Listener<NodeRecord> processListener;

        ListenableNodeScanViewNodeStoreScan( NodeStore nodeStore, LockService locks,
                PropertyStore propertyStore, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
                Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor, int[] labelIds,
                IntPredicate propertyKeyIdFilter, Listener<NodeRecord> processListener )
        {
            super( nodeStore, locks, propertyStore, labelUpdateVisitor, propertyUpdatesVisitor,
                    labelIds,
                    propertyKeyIdFilter );
            this.processListener = processListener;
        }

        @Override
        public void process( NodeRecord nodeRecord) throws FAILURE
        {
            processListener.receive( nodeRecord );
            super.process( nodeRecord );
        }
    }
}
