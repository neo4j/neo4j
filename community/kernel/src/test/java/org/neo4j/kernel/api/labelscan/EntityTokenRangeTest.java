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
package org.neo4j.kernel.api.labelscan;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.index.label.EntityTokenRange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;

class EntityTokenRangeTest
{
    @Test
    void shouldTransposeNodeIdsAndLabelIds()
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
        EntityTokenRange range = new EntityTokenRange( 0, labelsPerNode, NODE );

        // then
        assertArrayEquals( new long[] {0, 1, 2, 3, 4, 5, 6, 7}, range.entities() );
        for ( int i = 0; i < labelsPerNode.length; i++ )
        {
            assertArrayEquals( labelsPerNode[i], range.tokens( i ) );
        }
    }

    @Test
    void shouldRebaseOnRangeId()
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
        EntityTokenRange range = new EntityTokenRange( 10, labelsPerNode, NODE );

        // then
        long baseNodeId = range.id() * labelsPerNode.length;
        long[] expectedNodeIds = new long[labelsPerNode.length];
        for ( int i = 0; i < expectedNodeIds.length; i++ )
        {
            expectedNodeIds[i] = baseNodeId + i;
        }
        assertArrayEquals( expectedNodeIds, range.entities() );
    }

    @Test
    void shouldAdaptToStringToEntityTypeNode()
    {
        EntityTokenRange nodeLabelRange = new EntityTokenRange( 0, new long[0][], NODE );
        assertThat( nodeLabelRange.toString() ).contains( "NodeLabelRange" );
    }

    @Test
    void shouldAdaptToStringToEntityTypeRelationship()
    {
        EntityTokenRange relationshipTypeRange = new EntityTokenRange( 0, new long[0][], RELATIONSHIP );
        assertThat( relationshipTypeRange.toString() ).contains( "RelationshipTypeRange" );
    }
}
