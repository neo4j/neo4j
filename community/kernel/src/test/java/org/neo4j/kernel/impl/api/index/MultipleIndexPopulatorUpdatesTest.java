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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.function.IntPredicate;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.StoreViewNodeStoreScan;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.storable.Values;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.IndexCapability.NO_CAPABILITY;

@RunWith( MockitoJUnitRunner.class )
public class MultipleIndexPopulatorUpdatesTest
{
    @Mock( answer = Answers.RETURNS_MOCKS )
    private LogProvider logProvider;

    @Test
    public void updateForHigherNodeIgnoredWhenUsingFullNodeStoreScan()
            throws IndexPopulationFailedKernelException, IOException, IndexEntryConflictException
    {
        NeoStores neoStores = Mockito.mock( NeoStores.class );
        CountsTracker countsTracker = mock( CountsTracker.class );
        NodeStore nodeStore = mock( NodeStore.class );
        PropertyStore propertyStore = mock( PropertyStore.class );

        NodeRecord nodeRecord = getNodeRecord();

        when( neoStores.getCounts()).thenReturn( countsTracker );
        when( neoStores.getNodeStore()).thenReturn( nodeStore );
        when( neoStores.getPropertyStore() ).thenReturn( propertyStore );

        when( nodeStore.newRecord() ).thenReturn( nodeRecord );

        ProcessListenableNeoStoreIndexView
                storeView = new ProcessListenableNeoStoreIndexView( LockService.NO_LOCK_SERVICE, neoStores );
        MultipleIndexPopulator indexPopulator = new MultipleIndexPopulator( storeView, logProvider, mock( SchemaState.class ) );

        storeView.setProcessListener( new NodeUpdateProcessListener( indexPopulator ) );

        IndexPopulator populator = createIndexPopulator();
        IndexUpdater indexUpdater = mock( IndexUpdater.class );

        addPopulator( indexPopulator, populator, 1, SchemaIndexDescriptorFactory.forLabel( 1, 1 ) );

        indexPopulator.create();
        StoreScan<IndexPopulationFailedKernelException> storeScan = indexPopulator.indexAllNodes();
        storeScan.run();

        Mockito.verify( indexUpdater, never() ).process( any(IndexEntryUpdate.class) );
    }

    private NodeRecord getNodeRecord()
    {
        NodeRecord nodeRecord = new NodeRecord( 1L );
        nodeRecord.initialize( true, 0, false, 1, 0x0000000001L );
        InlineNodeLabels.putSorted( nodeRecord, new long[]{1}, null, null );
        return nodeRecord;
    }

    private IndexPopulator createIndexPopulator()
    {
        return mock( IndexPopulator.class );
    }

    private MultipleIndexPopulator.IndexPopulation addPopulator( MultipleIndexPopulator multipleIndexPopulator,
            IndexPopulator indexPopulator, long indexId, SchemaIndexDescriptor descriptor )
    {
        return addPopulator( multipleIndexPopulator, indexId, descriptor, indexPopulator,
                mock( FlippableIndexProxy.class ), mock( FailedIndexProxyFactory.class ) );
    }

    private MultipleIndexPopulator.IndexPopulation addPopulator( MultipleIndexPopulator multipleIndexPopulator,
                                                                 long indexId, SchemaIndexDescriptor descriptor, IndexPopulator indexPopulator,
                                                                 FlippableIndexProxy flippableIndexProxy, FailedIndexProxyFactory failedIndexProxyFactory )
    {
        return multipleIndexPopulator.addPopulator( indexPopulator, indexId,
                new IndexMeta( indexId, descriptor, mock( IndexProvider.Descriptor.class ), NO_CAPABILITY ),
                flippableIndexProxy, failedIndexProxyFactory, "userIndexDescription" );
    }

    private static class NodeUpdateProcessListener implements Listener<NodeRecord>
    {
        private final MultipleIndexPopulator indexPopulator;
        private final LabelSchemaDescriptor index;

        NodeUpdateProcessListener( MultipleIndexPopulator indexPopulator )
        {
            this.indexPopulator = indexPopulator;
            this.index = SchemaDescriptorFactory.forLabel( 1, 1 );
        }

        @Override
        public void receive( NodeRecord nodeRecord )
        {
            if ( nodeRecord.getId() == 7 )
            {
                indexPopulator.queue( IndexEntryUpdate.change( 8L, index, Values.of( "a" ), Values.of( "b" ) ) );
            }
        }
    }

    private class ProcessListenableNeoStoreIndexView extends NeoStoreIndexStoreView
    {
        private Listener<NodeRecord> processListener;

        ProcessListenableNeoStoreIndexView( LockService locks, NeoStores neoStores )
        {
            super( locks, neoStores );
        }

        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter,
                Visitor<NodeUpdates,FAILURE> propertyUpdatesVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
                boolean forceStoreScan )
        {

            return new ListenableNodeScanViewNodeStoreScan<>( nodeStore, locks, propertyStore, labelUpdateVisitor,
                    propertyUpdatesVisitor, labelIds, propertyKeyIdFilter, processListener );
        }

        void setProcessListener( Listener<NodeRecord> processListener )
        {
            this.processListener = processListener;
        }
    }

    private class ListenableNodeScanViewNodeStoreScan<FAILURE extends Exception> extends StoreViewNodeStoreScan<FAILURE>
    {
        private final Listener<NodeRecord> processListener;

        ListenableNodeScanViewNodeStoreScan( NodeStore nodeStore, LockService locks,
                PropertyStore propertyStore, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
                Visitor<NodeUpdates,FAILURE> propertyUpdatesVisitor, int[] labelIds,
                IntPredicate propertyKeyIdFilter, Listener<NodeRecord> processListener )
        {
            super( nodeStore, locks, propertyStore, labelUpdateVisitor, propertyUpdatesVisitor,
                    labelIds,
                    propertyKeyIdFilter );
            this.processListener = processListener;
        }

        @Override
        public void process( NodeRecord nodeRecord ) throws FAILURE
        {
            processListener.receive( nodeRecord );
            super.process( nodeRecord );
        }
    }
}
