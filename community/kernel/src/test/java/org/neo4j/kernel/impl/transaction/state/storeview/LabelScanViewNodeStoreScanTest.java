/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.EntityUpdates;
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
    private Visitor<EntityUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );

    @Before
    public void setUp()
    {
        when( labelScanStore.newReader() ).thenReturn( labelScanReader );
    }

    @Test
    public void iterateOverLabeledNodeIds()
    {
        PrimitiveLongResourceIterator labeledNodes = PrimitiveLongResourceCollections.iterator( null, 1, 2, 4, 8 );

        when( nodeStore.getHighId() ).thenReturn( 15L );
        when( labelScanReader.nodesWithAnyOfLabels( 1, 2 ) ).thenReturn( labeledNodes );

        int[] labelIds = new int[]{1, 2};

        LabelScanViewNodeStoreScan<Exception> storeScan = getLabelScanViewStoreScan( labelIds );
        PrimitiveLongResourceIterator idIterator = storeScan.getNodeIdIterator();
        List<Long> visitedNodeIds = PrimitiveLongCollections.asList( idIterator );

        assertThat(visitedNodeIds, Matchers.hasSize( 4 ));
        assertThat( visitedNodeIds, Matchers.hasItems( 1L, 2L, 4L, 8L ) );
    }

    private LabelScanViewNodeStoreScan<Exception> getLabelScanViewStoreScan( int[] labelIds )
    {
        return new LabelScanViewNodeStoreScan<>( nodeStore, LockService.NO_LOCK_SERVICE, propertyStore,
                labelScanStore, labelUpdateVisitor, propertyUpdateVisitor, labelIds, propertyKeyIdFilter );
    }
}
