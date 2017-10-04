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
package org.neo4j.values.virtual;

import org.junit.Test;

import static org.junit.Assert.fail;
import static org.neo4j.values.virtual.VirtualValueTestUtil.assertEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.assertNotEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.edge;
import static org.neo4j.values.virtual.VirtualValueTestUtil.edges;
import static org.neo4j.values.virtual.VirtualValueTestUtil.node;
import static org.neo4j.values.virtual.VirtualValueTestUtil.nodes;
import static org.neo4j.values.virtual.VirtualValueTestUtil.path;

public class GraphElementValueTest
{
    @Test
    public void nodeShouldEqualItself()
    {
        assertEqual( node( 1L ), node( 1L ) );
    }

    @Test
    public void nodeShouldNotEqualOtherNode()
    {
        assertNotEqual( node( 1L ), node( 2L ) );
    }

    @Test
    public void edgeShouldEqualItself()
    {
        assertEqual( edge( 1L, 1L, 2L ), edge( 1L ,1L, 2L) );
    }

    @Test
    public void edgeShouldNotEqualOtherEdge()
    {
        assertNotEqual( edge( 1L, 1L, 2L), edge( 2L, 1L, 2L ) );
    }

    @Test
    public void pathShouldEqualItself()
    {
        assertEqual( path( node( 1L ) ), path( node( 1L ) ) );
        assertEqual(
                path( node( 1L ), edge( 2L, 1L, 3L ), node( 3L ) ),
                path( node( 1L ), edge( 2L, 1L, 3L ), node( 3L ) ) );

        assertEqual(
                path( node( 1L ), edge( 2L, 1L, 3L ), node( 2L ), edge( 3L, 2L, 1L ), node( 1L ) ),
                path( node( 1L ), edge( 2L, 1L, 3L ), node( 2L ), edge( 3L, 2L, 1L ), node( 1L ) ) );
    }

    @Test
    public void pathShouldNotEqualOtherPath()
    {
        assertNotEqual( path( node( 1L ) ), path( node( 2L ) ) );
        assertNotEqual( path( node( 1L ) ), path( node( 1L ), edge( 1L, 1L, 2L ), node( 2L ) ) );
        assertNotEqual( path( node( 1L ) ), path( node( 2L ), edge( 1L, 2L, 1L ), node( 1L ) ) );

        assertNotEqual(
                path( node( 1L ), edge( 2L, 1L, 3L ), node( 3L ) ),
                path( node( 1L ), edge( 3L, 1L, 3L ), node( 3L ) ) );

        assertNotEqual(
                path( node( 1L ), edge( 2L, 1L, 2L ), node( 2L ) ),
                path( node( 1L ), edge( 2L, 1L, 3L ), node( 3L ) ) );
    }

    @Test
    public void pathShouldNotOnlyContainRelationship()
    {
        try
        {
            VirtualValues.path( nodes(), edges( 1L ) );
            fail();
        }
        catch ( AssertionError e )
        {
            // ignore
        }
    }

    @Test
    public void pathShouldContainOneMoreNodeThenEdges()
    {
        try
        {
            VirtualValues.path( nodes( 1L,2L ), edges() );
            fail();
        }
        catch ( Exception e )
        {
            // ignore
        }
    }

    @Test
    public void pathShouldHandleNull()
    {
        try
        {
            VirtualValues.path( null, null );
            fail();
        }
        catch ( AssertionError e )
        {
            // ignore
        }

        try
        {
            VirtualValues.path( nodes( 1L ), null );
            fail();
        }
        catch ( AssertionError e )
        {
            // ignore
        }

        try
        {
            VirtualValues.path( null, edges( 1L ) );
            fail();
        }
        catch ( AssertionError e )
        {
            // ignore
        }
    }
}
