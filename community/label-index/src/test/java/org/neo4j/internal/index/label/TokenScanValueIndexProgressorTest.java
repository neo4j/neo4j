/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.index.label;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.internal.index.label.NativeAllEntriesTokenScanReaderTest.EMPTY_CURSOR;
import static org.neo4j.internal.index.label.NativeAllEntriesTokenScanReaderTest.randomData;

@SuppressWarnings( "StatementWithEmptyBody" )
@ExtendWith( RandomExtension.class )
public class TokenScanValueIndexProgressorTest
{

    @Inject
    private RandomRule random;

    @Test
    void shouldNotProgressOnEmptyCursor()
    {
        TokenScanValueIndexProgressor progressor =
                new TokenScanValueIndexProgressor( EMPTY_CURSOR, new MyClient( LongStream.empty().iterator() ), IndexOrder.ASCENDING );
        assertFalse( progressor.next() );
    }

    @Test
    void shouldProgressAscendingThroughBitSet()
    {
        List<NativeAllEntriesTokenScanReaderTest.Labels> labels = randomData( random );

        for ( NativeAllEntriesTokenScanReaderTest.Labels label : labels )
        {
            long[] nodeIds = label.getNodeIds();
            TokenScanValueIndexProgressor progressor =
                    new TokenScanValueIndexProgressor( label.cursor(), new MyClient( LongStream.of( nodeIds ).iterator() ), IndexOrder.ASCENDING );
            while ( progressor.next() )
            {
            }
        }
    }

    @Test
    void shouldProgressDescendingThroughBitSet()
    {
        List<NativeAllEntriesTokenScanReaderTest.Labels> labels = randomData( random );

        for ( NativeAllEntriesTokenScanReaderTest.Labels label : labels )
        {
            long[] nodeIds = label.getNodeIds();
            PrimitiveIterator.OfLong iterator = LongStream.of( nodeIds )
                                                          .boxed()
                                                          .sorted( Collections.reverseOrder() )
                                                          .mapToLong( l -> l )
                                                          .iterator();
            TokenScanValueIndexProgressor progressor =
                    new TokenScanValueIndexProgressor( label.descendingCursor(), new MyClient( iterator ), IndexOrder.DESCENDING );
            while ( progressor.next() )
            {
            }
        }
    }

    static class MyClient implements IndexProgressor.EntityTokenClient
    {
        private final PrimitiveIterator.OfLong expectedNodeIds;

        MyClient( PrimitiveIterator.OfLong expectedNodeIds )
        {
            this.expectedNodeIds = expectedNodeIds;
        }

        @Override
        public boolean acceptEntity( long reference, TokenSet tokens )
        {
            assertEquals( expectedNodeIds.next(), reference );
            return true;
        }
    }
}
