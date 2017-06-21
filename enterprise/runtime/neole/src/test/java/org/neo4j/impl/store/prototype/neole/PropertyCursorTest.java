/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.store.prototype.neole;

import org.junit.ClassRule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.impl.kernel.api.NodeCursor;
import org.neo4j.impl.kernel.api.PropertyCursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;

public class PropertyCursorTest
{
    private static long bare, byteProp, shortProp, intProp, inlineLongProp, longProp,
            floatProp, doubleProp, trueProp, falseProp;

    @ClassRule
    public static final GraphSetup graph = new GraphSetup()
    {
        @Override
        protected void create( GraphDatabaseService graphDb )
        {
            try ( Transaction tx = graphDb.beginTx() )
            {
                bare = graphDb.createNode().getId();

                byteProp = createNodeWithProperty( graphDb, "byteProp", (byte)13 );
                shortProp = createNodeWithProperty( graphDb, "shortProp", (short)13 );
                intProp = createNodeWithProperty( graphDb, "intProp", 13 );
                inlineLongProp = createNodeWithProperty( graphDb, "inlineLongProp", 13L );
                longProp = createNodeWithProperty( graphDb, "longProp", Long.MAX_VALUE );

                floatProp = createNodeWithProperty( graphDb, "floatProp", 13.0f );
                doubleProp = createNodeWithProperty( graphDb, "doubleProp", 13.0 );

                trueProp = createNodeWithProperty( graphDb, "trueProp", true );
                falseProp = createNodeWithProperty( graphDb, "falseProp", false );

                tx.success();
            }
        }

        private long createNodeWithProperty( GraphDatabaseService graphDb, String propertyKey, Object value )
        {
            Node p = graphDb.createNode();
            p.setProperty( propertyKey, value );
            return p.getId();
        }
    }
            .withConfig( dense_node_threshold, "1" );

    @Test
    public void shouldNotAccessNonExistentProperties() throws Exception
    {
        // given
        try ( NodeCursor node = graph.allocateNodeCursor();
                PropertyCursor props = graph.allocatePropertyCursor() )
        {
            // when
            graph.singleNode( bare, node );
            assertTrue( "node by reference", node.next() );
            assertFalse( "no properties", node.hasProperties() );

            node.properties( props );
            assertFalse( "no properties by direct method", props.next() );

            graph.nodeProperties( node.propertiesReference(), props );
            assertFalse( "no properties via property ref", props.next() );

            assertFalse( "only one node", node.next() );
        }
    }

    @Test
    public void shouldAccessIntProperty() throws Exception
    {
        assertAccessSingleProperty( byteProp, (byte)13 );
        assertAccessSingleProperty( shortProp, (short)13 );
        assertAccessSingleProperty( intProp, 13 );
        assertAccessSingleProperty( inlineLongProp, 13L );
        assertAccessSingleProperty( longProp, Long.MAX_VALUE );
        assertAccessSingleProperty( floatProp, 13.0f );
        assertAccessSingleProperty( doubleProp, 13.0 );
        assertAccessSingleProperty( trueProp, true );
        assertAccessSingleProperty( falseProp, false );
    }

    private void assertAccessSingleProperty( long nodeId, Object expectedValue )
    {
        // given
        try ( NodeCursor node = graph.allocateNodeCursor();
                PropertyCursor props = graph.allocatePropertyCursor() )
        {
            // when
            graph.singleNode( nodeId, node );
            assertTrue( "node by reference", node.next() );
            assertTrue( "has properties", node.hasProperties() );

            node.properties( props );
            assertTrue( "has properties by direct method", props.next() );
            assertEquals( "correct value", expectedValue, props.propertyValue() );
            assertFalse( "single property", props.next() );

            graph.nodeProperties( node.propertiesReference(), props );
            assertTrue( "has properties via property ref", props.next() );
            assertEquals( "correct value", expectedValue, props.propertyValue() );
            assertFalse( "single property", props.next() );
        }
    }
}
