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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.NodeUpdates;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.api.index.FailedIndexProxyFactory;
import org.neo4j.kernel.impl.api.index.FlippableIndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class LabelScanViewNodeStoreScanTest
{
    private NodeStore nodeStore = mock( NodeStore.class );
    private PropertyStore propertyStore = mock( PropertyStore.class );
    private LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private LabelScanReader labelScanReader = mock( LabelScanReader.class );
    private IntPredicate propertyKeyIdFilter = mock( IntPredicate.class );
    private Visitor<NodeLabelUpdate,Exception> labelUpdateVisitor = mock( Visitor.class );
    private Visitor<NodeUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );

    @Before
    public void setUp()
    {
        when( labelScanStore.newReader() ).thenReturn( labelScanReader );
    }

    @Test
    public void iterateOverLabeledNodeIds() throws Exception
    {
        PrimitiveLongIterator labeledNodes = PrimitiveLongCollections.iterator( 1, 2, 4, 8 );

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
    public void resetNodeIdIteratorDuringConcurrentUpdates()
    {
        when( labelScanReader.nodesWithAnyOfLabels( 1, 2 ) )
                .thenReturn( PrimitiveLongCollections.iterator( 1, 2, 3, 4 ) );

        LabelScanViewNodeStoreScan<Exception> scanViewStoreScan = getLabelScanViewStoreScan( new int[]{1, 2} );
        PrimitiveLongResourceIterator nodeIdIterator = scanViewStoreScan.getNodeIdIterator();

        verify( labelScanStore).newReader();
        verify( labelScanReader ).nodesWithAnyOfLabels( 1, 2 );

        assertTrue( "Contain 4 nodes id.", nodeIdIterator.hasNext() );
        assertEquals( "First expected node id is 1.", 1, nodeIdIterator.next() );
        assertTrue( "Contain 4 nodes id.", nodeIdIterator.hasNext() );
        assertEquals( "Second expected node id is 2.", 2, nodeIdIterator.next() );

        populateWithConcurrentUpdates( scanViewStoreScan );

        assertTrue( "Contain 4 nodes id.", nodeIdIterator.hasNext() );
        assertEquals( "Third expected node id is 3.", 3, nodeIdIterator.next() );

        verify( labelScanReader ).close();
        verify( labelScanStore, times( 2 ) ).newReader();
        verify( labelScanReader, times( 2 ) ).nodesWithAnyOfLabels( 1, 2 );

        assertTrue( "Contain 4 nodes id.", nodeIdIterator.hasNext() );
        assertEquals( "Fourth expected node id is 4.", 4, nodeIdIterator.next() );

        assertFalse( nodeIdIterator.hasNext() );

        verifyNoMoreInteractions( labelScanReader, labelScanStore );
    }

    private void populateWithConcurrentUpdates( LabelScanViewNodeStoreScan<Exception> scanViewStoreScan )
    {
        MultipleIndexPopulator.MultipleIndexUpdater indexUpdater = mock( MultipleIndexPopulator.MultipleIndexUpdater.class );
        scanViewStoreScan.acceptUpdate( indexUpdater, IndexEntryUpdate.add( 1, null, "add" ), 0L );
        scanViewStoreScan.acceptUpdate( indexUpdater, IndexEntryUpdate.change( 2, null, "changeBefore", "changeAfter" ), 0L );
        scanViewStoreScan.acceptUpdate( indexUpdater, IndexEntryUpdate.change( 2, null, "changeBefore2", "changeAfter2" ), 0L );
        scanViewStoreScan.acceptUpdate( indexUpdater, IndexEntryUpdate.remove( 3, null, "remove" ), 0L );
    }

    private MultipleIndexPopulator.IndexPopulation getPopulation( LabelScanTestMultipleIndexPopulator indexPopulator )
    {
        return indexPopulator.createPopulation( mock( IndexPopulator.class ), 1, null, null, null, null, null );
    }

    private LabelScanViewNodeStoreScan<Exception> getLabelScanViewStoreScan( int[] labelIds )
    {
        return new LabelScanViewNodeStoreScan<>( nodeStore, LockService.NO_LOCK_SERVICE, propertyStore,
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
        public IndexPopulation createPopulation( IndexPopulator populator, long indexId,
                NewIndexDescriptor descriptor, SchemaIndexProvider.Descriptor providerDescriptor,
                FlippableIndexProxy flipper, FailedIndexProxyFactory failedIndexProxyFactory,
                String indexUserDescription )
        {
            return super.createPopulation( populator, indexId, descriptor, providerDescriptor, flipper,
                            failedIndexProxyFactory, indexUserDescription );
        }
    }

}
