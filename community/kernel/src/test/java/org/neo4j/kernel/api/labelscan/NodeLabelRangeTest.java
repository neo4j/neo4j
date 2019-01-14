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
package org.neo4j.kernel.api.labelscan;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class NodeLabelRangeTest
{
    @Test
    public void shouldTransposeNodeIdsAndLabelIds()
    {
        // given
        long[][] labelsPerNode = new long[][] {
            {1},
            {1, 3},
            {3, 5, 7},
            {},
            {1, 5, 7},
            {},
            {},
            {1, 2, 3, 4}
            };

        // when
        NodeLabelRange range = new NodeLabelRange( 0, labelsPerNode );

        // then
        assertArrayEquals( new long[] {0, 1, 2, 3, 4, 5, 6, 7}, range.nodes() );
        for ( int i = 0; i < labelsPerNode.length; i++ )
        {
            assertArrayEquals( labelsPerNode[i], range.labels( i ) );
        }
    }

    @Test
    public void shouldRebaseOnRangeId()
    {
        // given
        long[][] labelsPerNode = new long[][] {
            {1},
            {1, 3},
            {3, 5, 7},
            {},
            {1, 5, 7},
            {},
            {},
            {1, 2, 3, 4}
        };

        // when
        NodeLabelRange range = new NodeLabelRange( 10, labelsPerNode );

        // then
        long baseNodeId = range.id() * labelsPerNode.length;
        long[] expectedNodeIds = new long[labelsPerNode.length];
        for ( int i = 0; i < expectedNodeIds.length; i++ )
        {
            expectedNodeIds[i] = baseNodeId + i;
        }
        assertArrayEquals( expectedNodeIds, range.nodes() );
    }
}
