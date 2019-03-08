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

import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.common.EntityType;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.IndexDescriptor;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.storeview.StoreViewNodeStoreScan;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.values.storable.Values;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class MultipleIndexPopulatorUpdatesTest
{
    @Mock( answer = Answers.RETURNS_MOCKS )
    private LogProvider logProvider;

    @Test
    public void updateForHigherNodeIgnoredWhenUsingFullNodeStoreScan()
            throws IndexPopulationFailedKernelException, IndexEntryConflictException
    {
        IndexStatisticsStore indexStatisticsStore = mock( IndexStatisticsStore.class );

        StorageReader reader = mock( StorageReader.class );
        when( reader.allocateNodeCursor() ).thenReturn( mock( StorageNodeCursor.class ) );
        ProcessListenableNeoStoreIndexView
                storeView = new ProcessListenableNeoStoreIndexView( LockService.NO_LOCK_SERVICE, () -> reader );
        MultipleIndexPopulator indexPopulator =
                new MultipleIndexPopulator( storeView, logProvider, EntityType.NODE, mock( SchemaState.class ), indexStatisticsStore );

        storeView.setProcessListener( new NodeUpdateProcessListener( indexPopulator ) );

        IndexPopulator populator = createIndexPopulator();
        IndexUpdater indexUpdater = mock( IndexUpdater.class );

        addPopulator( indexPopulator, populator, 1, TestIndexDescriptorFactory.forLabel( 1, 1 ) );

        indexPopulator.create();
        StoreScan<IndexPopulationFailedKernelException> storeScan = indexPopulator.indexAllEntities();
        storeScan.run();

        Mockito.verify( indexUpdater, never() ).process( any(IndexEntryUpdate.class) );
    }

    private IndexPopulator createIndexPopulator()
    {
        return mock( IndexPopulator.class );
    }

    private void addPopulator( MultipleIndexPopulator multipleIndexPopulator,
            IndexPopulator indexPopulator, long indexId, IndexDescriptor descriptor )
    {
        addPopulator( multipleIndexPopulator, descriptor.withId( indexId ), indexPopulator, mock( FlippableIndexProxy.class ),
                mock( FailedIndexProxyFactory.class ) );
    }

    private void addPopulator( MultipleIndexPopulator multipleIndexPopulator, StoreIndexDescriptor descriptor,
            IndexPopulator indexPopulator, FlippableIndexProxy flippableIndexProxy, FailedIndexProxyFactory failedIndexProxyFactory )
    {
        multipleIndexPopulator.addPopulator( indexPopulator, descriptor.withoutCapabilities(), flippableIndexProxy, failedIndexProxyFactory,
                "userIndexDescription" );
    }

    private static class NodeUpdateProcessListener implements Listener<StorageNodeCursor>
    {
        private final MultipleIndexPopulator indexPopulator;
        private final LabelSchemaDescriptor index;

        NodeUpdateProcessListener( MultipleIndexPopulator indexPopulator )
        {
            this.indexPopulator = indexPopulator;
            this.index = SchemaDescriptorFactory.forLabel( 1, 1 );
        }

        @Override
        public void receive( StorageNodeCursor node )
        {
            if ( node.entityReference() == 7 )
            {
                indexPopulator.queueConcurrentUpdate( IndexEntryUpdate.change( 8L, index, Values.of( "a" ), Values.of( "b" ) ) );
            }
        }
    }

    private class ProcessListenableNeoStoreIndexView extends NeoStoreIndexStoreView
    {
        private Listener<StorageNodeCursor> processListener;

        ProcessListenableNeoStoreIndexView( LockService locks, Supplier<StorageReader> storageReaderSupplier )
        {
            super( locks, storageReaderSupplier );
        }

        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
                IntPredicate propertyKeyIdFilter,
                Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
                boolean forceStoreScan )
        {

            return new ListenableNodeScanViewNodeStoreScan<>( storageEngine.get(), locks, labelUpdateVisitor,
                    propertyUpdatesVisitor, labelIds, propertyKeyIdFilter, processListener );
        }

        void setProcessListener( Listener<StorageNodeCursor> processListener )
        {
            this.processListener = processListener;
        }
    }

    private class ListenableNodeScanViewNodeStoreScan<FAILURE extends Exception> extends StoreViewNodeStoreScan<FAILURE>
    {
        private final Listener<StorageNodeCursor> processListener;

        ListenableNodeScanViewNodeStoreScan( StorageReader storageReader, LockService locks,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
                Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor, int[] labelIds,
                IntPredicate propertyKeyIdFilter, Listener<StorageNodeCursor> processListener )
        {
            super( storageReader, locks, labelUpdateVisitor, propertyUpdatesVisitor,
                    labelIds,
                    propertyKeyIdFilter );
            this.processListener = processListener;
        }

        @Override
        public boolean process( StorageNodeCursor cursor ) throws FAILURE
        {
            processListener.receive( cursor );
            return super.process( cursor );
        }
    }
}
