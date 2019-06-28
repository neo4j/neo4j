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

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.index.label.LabelScanReader;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.storageengine.api.StubStorageCursors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LabelScanViewNodeStoreScanTest
{
    private final StubStorageCursors cursors = new StubStorageCursors();
    private final LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private final LabelScanReader labelScanReader = mock( LabelScanReader.class );
    private final IntPredicate propertyKeyIdFilter = mock( IntPredicate.class );
    private final Visitor<NodeLabelUpdate,Exception> labelUpdateVisitor = mock( Visitor.class );
    private final Visitor<EntityUpdates,Exception> propertyUpdateVisitor = mock( Visitor.class );

    @BeforeEach
    void setUp()
    {
        when( labelScanStore.newReader() ).thenReturn( labelScanReader );
    }

    @Test
    void iterateOverLabeledNodeIds()
    {
        PrimitiveLongResourceIterator labeledNodes = PrimitiveLongResourceCollections.iterator( null, 1, 2, 4, 8 );

        long highId = 15L;
        for ( long i = 0; i < highId; i++ )
        {
            cursors.withNode( i );
        }
        int[] labelIds = new int[]{1, 2};
        when( labelScanReader.nodesWithAnyOfLabels( labelIds ) ).thenReturn( labeledNodes );

        LabelScanViewNodeStoreScan<Exception> storeScan = getLabelScanViewStoreScan( labelIds );
        PrimitiveLongResourceIterator idIterator = storeScan.getEntityIdIterator();
        List<Long> visitedNodeIds = PrimitiveLongCollections.asList( idIterator );

        assertThat(visitedNodeIds, Matchers.hasSize( 4 ));
        assertThat( visitedNodeIds, Matchers.hasItems( 1L, 2L, 4L, 8L ) );
    }

    private LabelScanViewNodeStoreScan<Exception> getLabelScanViewStoreScan( int[] labelIds )
    {
        return new LabelScanViewNodeStoreScan<>( cursors, LockService.NO_LOCK_SERVICE,
                labelScanStore, labelUpdateVisitor, propertyUpdateVisitor, labelIds, propertyKeyIdFilter );
    }
}
