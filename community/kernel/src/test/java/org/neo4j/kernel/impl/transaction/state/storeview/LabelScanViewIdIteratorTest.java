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

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LabelScanViewIdIteratorTest
{

    private LabelScanViewNodeStoreScan labelScanView = mock( LabelScanViewNodeStoreScan.class );
    private LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private LabelScanReader labelScanReader = mock( LabelScanReader.class );
    private LabelScanViewIdIterator scanViewIdIterator;
    private PrimitiveLongIterator nodeIdsIterator = PrimitiveLongCollections.iterator( 2, 4, 7 );

    @Before
    public void setUp()
    {
        setUpMocks();

        scanViewIdIterator = new LabelScanViewIdIterator( labelScanView, labelScanStore, new int[]{1, 2} );
    }

    @Test
    public void iterateOverStoredLabelNodeIds() throws Exception
    {
        checkItem( scanViewIdIterator, 2 );
        checkItem( scanViewIdIterator, 4 );
        checkItem( scanViewIdIterator, 7 );
        assertFalse( "Should contain only 3 nodes.", scanViewIdIterator.hasNext() );
    }

    @Test
    public void iterateOverStoredLabelNodeIdsWithUpdates()
    {
        when( labelScanView.isOutdated() ).thenReturn( true );

        verifyOpenIteratorAndResetMocks();
        checkItem( scanViewIdIterator, 2 );

        verifyLabelScanReopenAndResetMocks();
        checkItem( scanViewIdIterator, 4 );

        verifyLabelScanReopenAndResetMocks();
        checkItem( scanViewIdIterator, 7 );

        verifyLabelScanReopenAndResetMocks();

        assertFalse( "Should contain only 3 nodes.", scanViewIdIterator.hasNext() );
    }

    @Test
    public void iterateOverIncreasedNumberOfAffectedStoredLabelNodeIds()
    {
        when( labelScanView.isOutdated() ).thenReturn( true );

        verifyOpenIteratorAndResetMocks();
        checkItem( scanViewIdIterator, 2 );

        verifyLabelScanReopenAndResetMocks();
        checkItem( scanViewIdIterator, 4 );

        nodeIdsIterator = PrimitiveLongCollections.iterator( 2, 3, 4, 5, 6, 7 );

        verifyLabelScanReopenAndResetMocks();
        checkItem( scanViewIdIterator, 5 );

        verifyLabelScanReopenAndResetMocks();
        checkItem( scanViewIdIterator, 6 );

        verifyLabelScanReopenAndResetMocks();
        checkItem( scanViewIdIterator, 7 );

        assertFalse( "Should iterate as result over 5 nodes.", scanViewIdIterator.hasNext() );
    }

    @Test
    public void iterateOverDecreasedNumberOfAffectedNodes()
    {
        when( labelScanView.isOutdated() ).thenReturn( true );

        verifyOpenIteratorAndResetMocks();
        checkItem( scanViewIdIterator, 2 );

        nodeIdsIterator = PrimitiveLongCollections.iterator( 1, 2 );
        verifyLabelScanReopenAndResetMocks();

        assertFalse( "Should iterate as result over 1 node only.", scanViewIdIterator.hasNext() );
    }

    private void setUpMocks()
    {
        when( labelScanStore.newReader() ).thenReturn( labelScanReader );
        when( labelScanReader.nodesWithAnyOfLabels( 1, 2 ) ).thenReturn( nodeIdsIterator );
    }

    private void verifyOpenIteratorAndResetMocks()
    {
        verify( labelScanStore ).newReader();
        verify( labelScanReader ).nodesWithAnyOfLabels( 1, 2 );
        reset( labelScanReader, labelScanStore );
        setUpMocks();
    }

    private void verifyLabelScanReopenAndResetMocks()
    {
        verify( labelScanReader ).close();
        verifyOpenIteratorAndResetMocks();
    }

    private void checkItem( LabelScanViewIdIterator scanViewIdIterator, long expectedItem )
    {
        assertTrue( "Should contain at least one more item.", scanViewIdIterator.hasNext() );
        assertEquals( "Item should be equal to expected.", expectedItem,  scanViewIdIterator.next() );
    }
}
