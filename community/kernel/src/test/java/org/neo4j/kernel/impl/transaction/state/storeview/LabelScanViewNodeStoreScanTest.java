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

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LabelScanViewNodeStoreScanTest
{
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
        when( labelScanReader.getHighestIndexedNodeId() ).thenReturn( 10L );
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( 10L );
        when( labelScanReader.nodesWithAnyOfLabels( 1, 2 ) ).thenReturn( labeledNodes );

        int[] labelIds = new int[]{1, 2};

        LabelScanViewNodeStoreScan<Exception> storeScan = getLabelScanViewStoreScan( labelIds );
        PrimitiveLongResourceIterator idIterator = storeScan.getNodeIdIterator();
        List<Long> visitedNodeIds = PrimitiveLongCollections.asList( idIterator );

        assertThat(visitedNodeIds, Matchers.hasSize( 4 ));
        assertThat( visitedNodeIds, Matchers.hasItems( 1L, 2L, 4L, 8L ) );
    }

    @Test
    public void iterateOverLabelAndNewlyCreatedNodes() throws Exception
    {
        PrimitiveLongIterator labeledNodes = PrimitiveLongCollections.iterator( 1, 2, 4, 8, 9, 10 );

        when( labelScanStore.newReader() ).thenReturn( labelScanReader );
        when( labelScanReader.getHighestIndexedNodeId() ).thenReturn( 7L );
        when( nodeStore.getHighestPossibleIdInUse() ).thenReturn( 10L );
        when( labelScanReader.nodesWithAnyOfLabels( 1, 2 ) ).thenReturn( labeledNodes );

        int[] labelIds = new int[]{1, 2};

        LabelScanViewNodeStoreScan<Exception> storeScan = getLabelScanViewStoreScan( labelIds );
        PrimitiveLongResourceIterator idIterator = storeScan.getNodeIdIterator();
        List<Long> visitedNodeIds = PrimitiveLongCollections.asList( idIterator );

        assertThat(visitedNodeIds, Matchers.hasSize( 6 ));
        assertThat( visitedNodeIds, Matchers.hasItems( 1L, 2L, 4L, 8L, 9L, 10L ) );
    }

    private LabelScanViewNodeStoreScan<Exception> getLabelScanViewStoreScan( int[] labelIds )
    {
        return new LabelScanViewNodeStoreScan<>( nodeStore, LockService.NO_LOCK_SERVICE, propertyStore, labelScanStore,
                labelUpdateVisitor, propertyUpdateVisitor, labelIds, propertyKeyIdFilter );
    }

}