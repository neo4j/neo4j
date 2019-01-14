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
package org.neo4j.values.virtual;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.node;
import static org.neo4j.values.virtual.VirtualValueTestUtil.nodes;
import static org.neo4j.values.virtual.VirtualValueTestUtil.path;
import static org.neo4j.values.virtual.VirtualValueTestUtil.rel;
import static org.neo4j.values.virtual.VirtualValueTestUtil.relationships;

class GraphElementValueTest
{
    @Test
    void nodeShouldEqualItself()
    {
        assertEqual( node( 1L ), node( 1L ) );
    }

    @Test
    void nodeShouldNotEqualOtherNode()
    {
        assertNotEqual( node( 1L ), node( 2L ) );
    }

    @Test
    void edgeShouldEqualItself()
    {
        assertEqual( rel( 1L, 1L, 2L ), rel( 1L ,1L, 2L) );
    }

    @Test
    void edgeShouldNotEqualOtherEdge()
    {
        assertNotEqual( rel( 1L, 1L, 2L), rel( 2L, 1L, 2L ) );
    }

    @Test
    void pathShouldEqualItself()
    {
        assertEqual( path( node( 1L ) ), path( node( 1L ) ) );
        assertEqual(
                path( node( 1L ), rel( 2L, 1L, 3L ), node( 3L ) ),
                path( node( 1L ), rel( 2L, 1L, 3L ), node( 3L ) ) );

        assertEqual(
                path( node( 1L ), rel( 2L, 1L, 3L ), node( 2L ), rel( 3L, 2L, 1L ), node( 1L ) ),
                path( node( 1L ), rel( 2L, 1L, 3L ), node( 2L ), rel( 3L, 2L, 1L ), node( 1L ) ) );
    }

    @Test
    void pathShouldNotEqualOtherPath()
    {
        assertNotEqual( path( node( 1L ) ), path( node( 2L ) ) );
        assertNotEqual( path( node( 1L ) ), path( node( 1L ), rel( 1L, 1L, 2L ), node( 2L ) ) );
        assertNotEqual( path( node( 1L ) ), path( node( 2L ), rel( 1L, 2L, 1L ), node( 1L ) ) );

        assertNotEqual(
                path( node( 1L ), rel( 2L, 1L, 3L ), node( 3L ) ),
                path( node( 1L ), rel( 3L, 1L, 3L ), node( 3L ) ) );

        assertNotEqual(
                path( node( 1L ), rel( 2L, 1L, 2L ), node( 2L ) ),
                path( node( 1L ), rel( 2L, 1L, 3L ), node( 3L ) ) );
    }

    @Test
    void pathShouldNotOnlyContainRelationship()
    {
        assertThrows( AssertionError.class, () -> VirtualValues.path( nodes(), relationships( 1L ) ) );
    }

    @Test
    void pathShouldContainOneMoreNodeThenEdges()
    {
        assertThrows( Exception.class, () -> VirtualValues.path( nodes( 1L, 2L ), relationships() ) );
    }

    @Test
    void pathShouldHandleNulls()
    {
        assertThrows( AssertionError.class, () -> VirtualValues.path( null, null ) );
    }

    @Test
    void pathShouldHandleNullEdge()
    {
        assertThrows( AssertionError.class, () -> VirtualValues.path( nodes( 1L ), null ) );
    }

    @Test
    void pathShouldHandleNullNodes()
    {
        assertThrows( AssertionError.class, () -> VirtualValues.path( null, relationships( 1L ) ) );
    }
}
