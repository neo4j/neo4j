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
package org.neo4j.kernel.impl.index.labelscan;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.asArray;

public class NativeLabelScanReaderTest
{
    private static final int LABEL_ID = 1;

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldFindMultipleNodesInEachRange() throws Exception
    {
        // GIVEN
        GBPTree<LabelScanKey,LabelScanValue> index = mock( GBPTree.class );
        RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor = mock( RawCursor.class );
        when( cursor.next() ).thenReturn( true, true, true, false );
        when( cursor.get() ).thenReturn(
                // range, bits
                hit( 0, 0b1000_1000__1100_0010L ),
                hit( 1, 0b0000_0010__0000_1000L ),
                hit( 3, 0b0010_0000__1010_0001L ),
                null );
        when( index.seek( any( LabelScanKey.class ), any( LabelScanKey.class ) ) )
                .thenReturn( cursor );
        try ( NativeLabelScanReader reader = new NativeLabelScanReader( index ) )
        {
            // WHEN
            PrimitiveLongIterator iterator = reader.nodesWithLabel( LABEL_ID );

            // THEN
            assertArrayEquals( new long[] {
                    // base 0*64 = 0
                    1, 6, 7, 11, 15,
                    // base 1*64 = 64
                    64 + 3, 64 + 9,
                    // base 3*64 = 192
                    192 + 0, 192 + 5, 192 + 7, 192 + 13 },

                    asArray( iterator ) );
        }
    }

    private static Hit<LabelScanKey,LabelScanValue> hit( long baseNodeId, long bits )
    {
        LabelScanKey key = new LabelScanKey( LABEL_ID, baseNodeId );
        LabelScanValue value = new LabelScanValue();
        value.bits = bits;
        return new MutableHit<>( key, value );
    }
}
