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
package org.neo4j.internal.store.prototype.neole;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;

public class PropertyCursorTest
{
    private static long bare, byteProp, shortProp, intProp, inlineLongProp, longProp,
            floatProp, doubleProp, trueProp, falseProp, charProp, shortStringProp, utf8Prop, allProps;

    private static String chinese = "造Unicode之";
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

                charProp = createNodeWithProperty( graphDb, "charProp", 'x' );
                shortStringProp = createNodeWithProperty( graphDb, "shortStringProp", "hello" );
                utf8Prop = createNodeWithProperty( graphDb, "utf8Prop", chinese );

                Node all = graphDb.createNode();
                // first property record
                all.setProperty( "byteProp", (byte)13 );
                all.setProperty( "shortProp", (short)13 );
                all.setProperty( "intProp", 13 );
                all.setProperty( "inlineLongProp", 13L );
                // second property record
                all.setProperty( "longProp", Long.MAX_VALUE );
                all.setProperty( "floatProp", 13.0f );
                all.setProperty( "doubleProp", 13.0 );
                //                  ^^^
                // third property record halfway through double?
                all.setProperty( "trueProp", true );
                all.setProperty( "falseProp", false );

                all.setProperty( "charProp", 'x' );
                all.setProperty( "shortStringProp", "hello" );
                all.setProperty( "utf8Prop", chinese );

                allProps = all.getId();

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
    public void shouldAccessSingleProperty() throws Exception
    {
        assumeThat( "x86_64", equalTo( System.getProperty( "os.arch" ) ) );

        assertAccessSingleProperty( byteProp, Values.of( (byte)13 ) );
        assertAccessSingleProperty( shortProp, Values.of( (short)13 ) );
        assertAccessSingleProperty( intProp, Values.of( 13 ) );
        assertAccessSingleProperty( inlineLongProp, Values.of( 13L ) );
        assertAccessSingleProperty( longProp, Values.of( Long.MAX_VALUE ) );
        assertAccessSingleProperty( floatProp, Values.of( 13.0f ) );
        assertAccessSingleProperty( doubleProp, Values.of( 13.0 ) );
        assertAccessSingleProperty( trueProp, Values.of( true ) );
        assertAccessSingleProperty( falseProp, Values.of( false ) );
        assertAccessSingleProperty( charProp, Values.of( 'x' ) );
        assertAccessSingleProperty( shortStringProp, Values.of( "hello" ) );
        assertAccessSingleProperty( utf8Prop, Values.of( chinese ) );
    }

    @Test
    public void shouldAccessAllNodeProperties() throws Exception
    {
        assumeThat( "x86_64", equalTo( System.getProperty( "os.arch" ) ) );

        // given
        try ( NodeCursor node = graph.allocateNodeCursor();
                PropertyCursor props = graph.allocatePropertyCursor() )
        {
            // when
            graph.singleNode( allProps, node );
            assertTrue( "node by reference", node.next() );
            assertTrue( "has properties", node.hasProperties() );

            node.properties( props );
            Set<Object> values = new HashSet<>();
            while ( props.next() )
            {
                values.add( props.propertyValue().asObject() );
            }

            assertTrue( "byteProp", values.contains( (byte)13 ) );
            assertTrue( "shortProp", values.contains( (short)13 ) );
            assertTrue( "intProp", values.contains( 13 ) );
            assertTrue( "inlineLongProp", values.contains( 13L ) );
            assertTrue( "longProp", values.contains( Long.MAX_VALUE ) );
            assertTrue( "floatProp", values.contains( 13.0f ) );
            assertTrue( "doubleProp", values.contains( 13.0 ) );
            assertTrue( "trueProp", values.contains( true ) );
            assertTrue( "falseProp", values.contains( false ) );
            assertTrue( "charProp", values.contains( 'x' ) );
            assertTrue( "shortStringProp", values.contains( "hello" ) );
            assertTrue( "utf8Prop", values.contains( chinese ) );

            assertEquals( "number of values", 12, values.size() );
        }
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
