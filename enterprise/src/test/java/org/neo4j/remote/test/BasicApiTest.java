/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.remote.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.remote.AbstractTestBase;
import org.neo4j.remote.RemoteGraphDatabase;
import org.neo4j.testsupport.Value;

public final class BasicApiTest extends AbstractTestBase
{
    public BasicApiTest( Callable<RemoteGraphDatabase> factory )
    {
        super( factory );
    }

    public @Test
    void canGetConnectToGraphDatabase()
    {
        assertNotNull( graphDb() );
    }

    public @Test
    void canGetReferenceNode()
    {
        inTransaction( new Runnable()
        {
            public void run()
            {
                assertNotNull( graphDb().getReferenceNode() );
            }
        } );
    }

    public @Test
    void canCreateNode()
    {
        final Value<Long> nodeId = new Value<Long>();
        inTransaction( new Runnable()
        {
            public void run()
            {
                Node node = graphDb().createNode();
                assertNotNull( node );
                nodeId.set( node.getId() );
            }
        } );
        inTransaction( new Runnable()
        {
            public void run()
            {
                assertNotNull( graphDb().getNodeById( nodeId.get() ) );
            }
        } );
    }

    public @Test
    void canCreateRelationship()
    {
        final Value<Node> startNode = new Value<Node>();
        final Value<Node> endNode = new Value<Node>();
        inTransaction( new Runnable()
        {
            public void run()
            {
                Node start = graphDb().createNode(), end = graphDb().createNode();
                startNode.set( start );
                endNode.set( end );
                Relationship rel = start.createRelationshipTo( end,
                        withName( "TEST" ) );
                assertNotNull( rel );
                verifyRelationship(rel, "TEST", start, end);
            }
        } );
        inTransaction( new Runnable()
        {
            public void run()
            {
                Node start = startNode.get(), end = endNode.get();
                Iterator<Relationship> relationships = start.getRelationships().iterator();
                assertTrue( relationships.hasNext() );
                Relationship rel = relationships.next();
                assertNotNull( rel );
                verifyRelationship( rel, "TEST", start, end );
                assertFalse( relationships.hasNext() );
            }
        } );
    }

    private void verifyRelationship( Relationship rel, String typeName,
            Node start, Node end )
    {
        assertEquals( typeName, rel.getType().name() );
        assertEquals( start, rel.getStartNode() );
        assertEquals( end, rel.getEndNode() );
        assertEquals( start, rel.getOtherNode( end ) );
        assertEquals( end, rel.getOtherNode( start ) );
    }

    public @Test
    void canHandleNodeProperties()
    {
        canHandleProperties( new Callable<PropertyContainer>()
        {
            public PropertyContainer call()
            {
                return graphDb().createNode();
            }
        } );
    }

    public @Test
    void canHandleRelationshipProperties()
    {
        canHandleProperties( new Callable<PropertyContainer>()
        {
            public PropertyContainer call()
            {
                return graphDb().createNode().createRelationshipTo(
                        graphDb().createNode(), withName( "TEST" ) );
            }
        } );
    }

    private void canHandleProperties( final Callable<PropertyContainer> factory )
    {
        final Value<PropertyContainer> entity = new Value<PropertyContainer>();
        inTransaction( new Runnable()
        {
            public void run()
            {
                try
                {
                    entity.set( factory.call() );
                }
                catch ( Exception e )
                {
                    fail( "failed to create entity (" + e + ")" );
                }
                assignProperties( entity.get() );
                verifyProperties( entity.get() );
            }
        } );
        inTransaction( new Runnable()
        {
            public void run()
            {
                verifyProperties( entity.get() );
            }
        } );
    }

    private static final Map<String, Object> PROPERTIES = new HashMap<String, Object>()
    {
        private static final long serialVersionUID = 1L;
        {
            // String
            put( "key", "value" );
            put( "strings", new String[] { "one", "two", "three" } );
            put( "empty strings", new String[0] );
            // Boolean
            put( "boolean", true );
            put( "boxed boolean", Boolean.TRUE );
            put( "booleans", new boolean[] { true, false } );
            put( "boxed booleans", new Boolean[] { false, true } );
            put( "empty booleans", new boolean[0] );
            put( "empty boxed booleans", new Boolean[0] );
            // Byte
            put( "byte", (byte) -3 );
            put( "boxed byte", new Byte( (byte) 10 ) );
            put( "bytes", new byte[] { -128, 127 } );
            put( "boxed bytes", new Byte[] { 16, 32, 64 } );
            put( "empty bytes", new byte[0] );
            put( "empty boxed bytes", new Byte[0] );
            // Char
            put( "char", 'A' );
            put( "boxed char", new Character( 'a' ) );
            put( "chars", new char[] { 'a', 'b', 'c' } );
            put( "boxed chars", new Character[] { 'x', 'y', 'z' } );
            put( "empty chars", new char[0] );
            put( "empty boxed chars", new Character[0] );
            // Short
            put( "short", (short) -4 );
            put( "boxed short", new Short( (short) 11 ) );
            put( "shorts", new short[] { 1024, 768 } );
            put( "boxed shorts", new Short[] { 1, 12, 75, 6, 7 } );
            put( "empty shorts", new short[0] );
            put( "empty boxed shorts", new Short[0] );
            // Int
            put( "integer", (int) -5 );
            put( "boxed integer", new Integer( 12 ) );
            put( "integers", new int[] { 0x0099cc, 0x323232 } );
            put( "boxed integers", new Integer[] { 13, 37, 13, 37 } );
            put( "empty integers", new int[0] );
            put( "empty boxed integers", new Integer[0] );
            // Long
            put( "long", (long) 0xFFFFFFFFFFFFL );
            put( "boxed long", new Long( 100 ) );
            put( "longs", new long[] { -32, 66, -19 } );
            put( "boxed longs", new Long[] { 100L, 200L, 300L, 400L, 500L } );
            put( "empty longs", new long[0] );
            put( "empty boxed longs", new Long[0] );
            // Float
            put( "float", (float) 11.30 );
            put( "boxed float", new Float( 3.14 ) );
            put( "floats", new float[] { (float) 19.91, (float) 13.37 } );
            put( "boxed floats",
                    new Float[] { (float) 1, (float) 2, (float) 3 } );
            put( "empty floats", new float[0] );
            put( "empty boxed floats", new Float[0] );
            // Double
            put( "double", (double) 0.33333 );
            put( "boxed double", new Double( 3.1415 ) );
            put( "doubles", new double[] { 1.0, 2.0, 3.0 } );
            put( "boxed doubles", new Double[] { 0.5, 0.25, 0.125 } );
            put( "empty doubles", new double[0] );
            put( "empty boxed doubles", new Double[0] );
        }
    };

    private void assignProperties( PropertyContainer entity )
    {
        for ( Map.Entry<String, Object> property : PROPERTIES.entrySet() )
        {
            entity.setProperty( property.getKey(), property.getValue() );
        }
    }

    private void verifyProperties( PropertyContainer entity )
    {
        for ( Map.Entry<String, Object> property : PROPERTIES.entrySet() )
        {
            Object expected = property.getValue();
            Object actual = entity.getProperty( property.getKey() );
            if ( expected.getClass().isArray() )
            {
                assertArrayEquals( expected, actual );
            }
            else
            {
                assertEquals( expected, actual );
            }
        }
        for ( String property : entity.getPropertyKeys() )
        {
            assertTrue( PROPERTIES.containsKey( property ) );
        }
    }
}
