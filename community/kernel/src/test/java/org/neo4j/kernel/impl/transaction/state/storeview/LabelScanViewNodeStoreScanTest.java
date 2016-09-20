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

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.index.FailedIndexProxyFactory;
import org.neo4j.kernel.impl.api.index.FlippableIndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class LabelScanViewNodeStoreScanTest
{
    private NeoStoreIndexStoreView storeView = mock( NeoStoreIndexStoreView.class );
    private NodeStore nodeStore = mock( NodeStore.class );
    private PropertyStore propertyStore = mock( PropertyStore.class );
    private LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private LabelScanReader labelScanReader = mock( LabelScanReader.class );
    private IntPredicate propertyKeyIdFilter = mock( IntPredicate.class );
    private Visitor<NodeLabelUpdate,Exception> labelUpdateVisitor = mock( Visitor.class );
    private Visitor<NodePropertyUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );

    @Test
    public void iterateOverLabeledNodeIds() throws Exception
    {
        PrimitiveLongIterator labeledNodes = PrimitiveLongCollections.iterator( 1, 2, 4, 8 );


        when( labelScanStore.newReader() ).thenReturn( labelScanReader );
        when( nodeStore.getHighId() ).thenReturn( 15L );
        when( labelScanReader.nodesWithAnyOfLabels( 1, 2 ) ).thenReturn( labeledNodes );

        int[] labelIds = new int[]{1, 2};

        LabelScanViewNodeStoreScan<Exception> storeScan = getLabelScanViewStoreScan( labelIds );
        PrimitiveLongResourceIterator idIterator = storeScan.getNodeIdIterator();
        List<Long> visitedNodeIds = PrimitiveLongCollections.asList( idIterator );

        assertThat(visitedNodeIds, Matchers.hasSize( 4 ));
        assertThat( visitedNodeIds, Matchers.hasItems( 1L, 2L, 4L, 8L ) );
    }

    @Test
    public void configureSamplersToNotUseOnlineSampling() throws Exception
    {
        LabelScanViewNodeStoreScan<Exception> scanViewStoreScan = getLabelScanViewStoreScan( new int[]{1, 2} );
        IndexStoreView storeView = mock( IndexStoreView.class );
        LabelScanTestMultipleIndexPopulator indexPopulator = new LabelScanTestMultipleIndexPopulator( storeView, NullLogProvider.getInstance() );

        List<MultipleIndexPopulator.IndexPopulation> populations =
                Arrays.asList( getPopulation( indexPopulator ), getPopulation( indexPopulator ) );
        scanViewStoreScan.configure( populations );

        for ( MultipleIndexPopulator.IndexPopulation population : populations )
        {
            verify( population.populator ).configureSampling( false );
        }
    }

    @Test
    public void acceptConcurrentUpdates()
    {
        LabelScanViewNodeStoreScan<Exception> scanViewStoreScan = getLabelScanViewStoreScan( new int[]{1, 2} );
        populateWithConcurrentUpdates( scanViewStoreScan );

        assertEquals( "Contain updates only for 3 properties", 3, scanViewStoreScan.propertyLabelNodes.size() );
        assertEquals( "Contain 2 updates for property 2", 2, scanViewStoreScan.propertyLabelNodes.get( 2 ).size() );
        assertEquals( "Contain 1 updates for property 5", 1, scanViewStoreScan.propertyLabelNodes.get( 5 ).size() );
        assertEquals( "Contain 2 updates for property 4", 2, scanViewStoreScan.propertyLabelNodes.get( 4 ).size() );

        assertTrue( "Should contain update for expected node by property -> label key pair",
                scanViewStoreScan.propertyLabelNodes.get( 2 ).get( 1 ).contains( 1 ) );
        assertTrue( "Should contain update for expected node by property -> label key pair",
                scanViewStoreScan.propertyLabelNodes.get( 2 ).get( 2 ).contains( 2 ) );

        assertTrue( "Should contain update for expected node by property -> label key pair",
                scanViewStoreScan.propertyLabelNodes.get( 5 ).get( 1 ).contains( 2 ) );
        assertTrue( "Should contain update for expected node by property -> label key pair",
                scanViewStoreScan.propertyLabelNodes.get( 4 ).get( 1 ).contains( 3 ) );
        assertTrue( "Should contain update for expected node by property -> label key pair",
                scanViewStoreScan.propertyLabelNodes.get( 4 ).get( 2 ).contains( 3 ) );
    }

    @Test
    public void completeScanWithConcurrentAdd() throws Exception
    {
        LabelScanViewNodeStoreScan<Exception> scanViewStoreScan = getLabelScanViewStoreScan( new int[]{1, 2} );
        populateWithConcurrentUpdates( scanViewStoreScan );

        when( nodeStore.getRecord( eq( 1L ), any( NodeRecord.class ), eq( RecordLoad.FORCE) ) )
                .thenReturn( getActiveRecord( 1L) );

        Property property = Property.property( 2, "testProperty" );
        when( storeView.getProperty( 1L, 2 ) ).thenReturn( property );

        IndexPopulator indexPopulator = mock( IndexPopulator.class );
        IndexUpdater indexUpdater = mock( IndexUpdater.class );
        when( indexPopulator.newPopulatingUpdater( storeView ) ).thenReturn( indexUpdater );

        scanViewStoreScan.complete( indexPopulator, new IndexDescriptor( 1, 2 )  );
        verify( indexUpdater ).process( NodePropertyUpdate.remove( 1L, 2, StringUtils.EMPTY, new long[] {1}) );
        verify( indexUpdater ).process( NodePropertyUpdate.add( 1L, 2, "testProperty", new long[] {1}) );
        verify( indexUpdater ).close();
        verifyNoMoreInteractions( indexUpdater );
    }

    @Test
    public void completeScanWithConcurrentRemoveUpdate() throws Exception
    {
        LabelScanViewNodeStoreScan<Exception> scanViewStoreScan = getLabelScanViewStoreScan( new int[]{1, 2} );
        populateWithConcurrentUpdates( scanViewStoreScan );

        when( nodeStore.getRecord( eq( 3L ), any( NodeRecord.class ), eq( RecordLoad.FORCE) ) )
                .thenReturn( getDeletedRecord( 3L) );

        IndexPopulator indexPopulator = mock( IndexPopulator.class );
        IndexUpdater indexUpdater = mock( IndexUpdater.class );
        when( indexPopulator.newPopulatingUpdater( storeView ) ).thenReturn( indexUpdater );

        scanViewStoreScan.complete( indexPopulator, new IndexDescriptor( 1, 4 )  );
        verify( indexUpdater ).process( NodePropertyUpdate.remove( 3L, 4, StringUtils.EMPTY, new long[] {1}) );
        verify( indexUpdater ).close();
        verifyNoMoreInteractions( indexUpdater );
    }

    @Test
    public void completeScanWithConcurrentChangeUpdate() throws Exception
    {
        LabelScanViewNodeStoreScan<Exception> scanViewStoreScan = getLabelScanViewStoreScan( new int[]{1, 2} );
        populateWithConcurrentUpdates( scanViewStoreScan );

        when( nodeStore.getRecord( eq( 2L ), any( NodeRecord.class ), eq( RecordLoad.FORCE) ) )
                .thenReturn( getActiveRecord( 2L) );

        Property property1 = Property.property( 2, "testProperty1" );
        when( storeView.getProperty( 2L, 2 ) ).thenReturn( property1 );

        Property property2 = Property.property( 2, "testProperty2" );
        when( storeView.getProperty( 2L, 5 ) ).thenReturn( property2 );

        IndexPopulator indexPopulator = mock( IndexPopulator.class );
        IndexUpdater indexUpdater = mock( IndexUpdater.class );
        when( indexPopulator.newPopulatingUpdater( storeView ) ).thenReturn( indexUpdater );

        scanViewStoreScan.complete( indexPopulator, new IndexDescriptor( 2, 2 )  );
        verify( indexUpdater ).process( NodePropertyUpdate.remove( 2L, 2, StringUtils.EMPTY, new long[] {2}) );
        verify( indexUpdater ).process( NodePropertyUpdate.add( 2L, 2, "testProperty1", new long[] {2}) );
        verify( indexUpdater ).close();
        verifyNoMoreInteractions( indexUpdater );

        reset( indexUpdater );

        scanViewStoreScan.complete( indexPopulator, new IndexDescriptor( 1, 5 )  );
        verify( indexUpdater ).process( NodePropertyUpdate.remove( 2L, 5, StringUtils.EMPTY, new long[] {1}) );
        verify( indexUpdater ).process( NodePropertyUpdate.add( 2L, 5, "testProperty2", new long[] {1}) );
        verify( indexUpdater ).close();
        verifyNoMoreInteractions( indexUpdater );
    }

    private NodeRecord getDeletedRecord( long id )
    {
        return new NodeRecord( id, false, 1L, 2L, false );
    }

    private NodeRecord getActiveRecord( long id )
    {
        return new NodeRecord( id, false, 1L, 2L, true );
    }

    private void populateWithConcurrentUpdates( LabelScanViewNodeStoreScan<Exception> scanViewStoreScan )
    {
        MultipleIndexPopulator.MultipleIndexUpdater indexUpdater = mock( MultipleIndexPopulator.MultipleIndexUpdater.class );
        scanViewStoreScan.acceptUpdate( indexUpdater, NodePropertyUpdate.add( 1, 2, "add", new long[]{1} ), 0L );
        scanViewStoreScan.acceptUpdate( indexUpdater, NodePropertyUpdate.change( 2, 2, "changeBefore", new long[]{2},
                "changeAfter", new long[]{1, 2} ), 0L );
        scanViewStoreScan.acceptUpdate( indexUpdater, NodePropertyUpdate.change( 2, 5, "changeBefore2", new long[]{1},
                "changeAfter2", new long[]{1, 2} ), 0L );
        scanViewStoreScan.acceptUpdate( indexUpdater, NodePropertyUpdate.remove( 3, 4, "remove", new long[]{1,2}), 0L );
    }

    private MultipleIndexPopulator.IndexPopulation getPopulation( LabelScanTestMultipleIndexPopulator indexPopulator )
    {
        return indexPopulator.createPopulation( mock( IndexPopulator.class ), null, null, null, null, null, null );
    }

    private LabelScanViewNodeStoreScan<Exception> getLabelScanViewStoreScan( int[] labelIds )
    {
        return new LabelScanViewNodeStoreScan<>( storeView, nodeStore, LockService.NO_LOCK_SERVICE, propertyStore,
                labelScanStore, labelUpdateVisitor, propertyUpdateVisitor, labelIds, propertyKeyIdFilter );
    }

    private class LabelScanTestMultipleIndexPopulator extends MultipleIndexPopulator
    {
        public LabelScanTestMultipleIndexPopulator( IndexStoreView storeView,
                LogProvider logProvider )
        {
            super( storeView, logProvider );
        }

        @Override
        public IndexPopulation createPopulation( IndexPopulator populator,
                IndexDescriptor descriptor, IndexConfiguration config,
                SchemaIndexProvider.Descriptor providerDescriptor,
                FlippableIndexProxy flipper, FailedIndexProxyFactory failedIndexProxyFactory,
                String indexUserDescription )
        {
            return super.createPopulation( populator, descriptor, config, providerDescriptor, flipper,
                            failedIndexProxyFactory,
                            indexUserDescription );
        }
    }

}
