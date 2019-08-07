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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ImpermanentDbmsExtension
class NodeTest
{
    @Inject
    private GraphDatabaseService db;

    @Test
    void shouldGiveHelpfulExceptionWhenDeletingNodeWithRels()
    {
        // Given
        Node node;

        try ( Transaction transaction = db.beginTx() )
        {
            node = db.createNode();
            Node node2 = db.createNode();
            node.createRelationshipTo( node2, RelationshipType.withName( "MAYOR_OF" ) );
            transaction.commit();
        }

        // And given a transaction deleting just the node

        ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, () ->
        {
            try ( Transaction transaction = db.beginTx() )
            {
                node.delete();
                transaction.commit();
            }
        } );
        assertThat( exception.getMessage(), containsString( "Cannot delete node<" + node.getId() + ">, because it still has relationships. " +
                "To delete this node, you must first delete its relationships." ) );
    }

    @Test
    void testNodeCreateAndDelete()
    {
        long nodeId;
        try ( Transaction transaction = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            db.getNodeById( nodeId );
            node.delete();
            transaction.commit();
        }
        assertThrows( NotFoundException.class, () -> {
            try ( Transaction transaction = db.beginTx() )
            {
                db.getNodeById( nodeId );
            }
        } );
    }

    @Test
    void testDeletedNode()
    {
        // do some evil stuff
        try ( Transaction transaction = db.beginTx() )
        {
            Node node = db.createNode();
            node.delete();
            assertThrows( Exception.class, () -> node.setProperty( "key1", 1 ) );
        }
    }

    @Test
    void testNodeAddPropertyWithNullKey()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            assertThrows( IllegalArgumentException.class, () -> node1.setProperty( null, "bar" ) );
        }
    }

    @Test
    void testNodeAddPropertyWithNullValue()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            assertThrows( IllegalArgumentException.class, () -> node1.setProperty( "foo", null ) );
        }
    }

    @Test
    void testNodeAddProperty()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();

            String key1 = "key1";
            String key2 = "key2";
            String key3 = "key3";
            Integer int1 = 1;
            Integer int2 = 2;
            String string1 = "1";
            String string2 = "2";

            // add property
            node1.setProperty( key1, int1 );
            node2.setProperty( key1, string1 );
            node1.setProperty( key2, string2 );
            node2.setProperty( key2, int2 );
            assertTrue( node1.hasProperty( key1 ) );
            assertTrue( node2.hasProperty( key1 ) );
            assertTrue( node1.hasProperty( key2 ) );
            assertTrue( node2.hasProperty( key2 ) );
            assertFalse( node1.hasProperty( key3 ) );
            assertFalse( node2.hasProperty( key3 ) );
            assertEquals( int1, node1.getProperty( key1 ) );
            assertEquals( string1, node2.getProperty( key1 ) );
            assertEquals( string2, node1.getProperty( key2 ) );
            assertEquals( int2, node2.getProperty( key2 ) );
        }
    }

    @Test
    void testNodeRemoveProperty()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            String key1 = "key1";
            String key2 = "key2";
            Integer int1 = 1;
            Integer int2 = 2;
            String string1 = "1";
            String string2 = "2";

            Node node1 = db.createNode();
            Node node2 = db.createNode();

            if ( node1.removeProperty( key1 ) != null )
            {
                fail( "Remove of non existing property should return null" );
            }
            assertThrows( IllegalArgumentException.class, () -> node1.removeProperty( null ) );

            node1.setProperty( key1, int1 );
            node2.setProperty( key1, string1 );
            node1.setProperty( key2, string2 );
            node2.setProperty( key2, int2 );
            assertThrows( IllegalArgumentException.class, () -> node1.removeProperty( null ) );

            // test remove property
            assertEquals( int1, node1.removeProperty( key1 ) );
            assertEquals( string1, node2.removeProperty( key1 ) );
            // test remove of non existing property

            if ( node2.removeProperty( key1 ) != null )
            {
                fail( "Remove of non existing property return null." );
            }
        }
    }

    @Test
    void testNodeChangeProperty()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            String key1 = "key1";
            String key2 = "key2";
            String key3 = "key3";
            Integer int1 = 1;
            Integer int2 = 2;
            String string1 = "1";
            String string2 = "2";
            Boolean bool1 = Boolean.TRUE;
            Boolean bool2 = Boolean.FALSE;

            Node node1 = db.createNode();
            Node node2 = db.createNode();
            node1.setProperty( key1, int1 );
            node2.setProperty( key1, string1 );
            node1.setProperty( key2, string2 );
            node2.setProperty( key2, int2 );

            assertThrows( IllegalArgumentException.class, () -> node1.setProperty( null, null ) );

            // test change property
            node1.setProperty( key1, int2 );
            node2.setProperty( key1, string2 );
            assertEquals( string2, node2.getProperty( key1 ) );
            node1.setProperty( key3, bool1 );
            node1.setProperty( key3, bool2 );
            assertEquals( string2, node2.getProperty( key1 ) );
            node1.delete();
            node2.delete();
        }
    }

    @Test
    void testNodeChangeProperty2()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            String key1 = "key1";
            Integer int1 = 1;
            Integer int2 = 2;
            String string1 = "1";
            String string2 = "2";
            Boolean bool1 = Boolean.TRUE;
            Boolean bool2 = Boolean.FALSE;
            Node node1 = db.createNode();
            node1.setProperty( key1, int1 );
            node1.setProperty( key1, int2 );
            assertEquals( int2, node1.getProperty( key1 ) );
            node1.removeProperty( key1 );
            node1.setProperty( key1, string1 );
            node1.setProperty( key1, string2 );
            assertEquals( string2, node1.getProperty( key1 ) );
            node1.removeProperty( key1 );
            node1.setProperty( key1, bool1 );
            node1.setProperty( key1, bool2 );
            assertEquals( bool2, node1.getProperty( key1 ) );
            node1.removeProperty( key1 );
            node1.delete();
        }
    }

    @Test
    void testNodeGetProperties()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            String key1 = "key1";
            String key2 = "key2";
            String key3 = "key3";
            Integer int1 = 1;
            Integer int2 = 2;
            String string = "3";

            Node node1 = db.createNode();
            assertThrows( NotFoundException.class, () -> node1.getProperty( key1 ) );
            assertThrows( IllegalArgumentException.class, () -> node1.getProperty( null ) );
            assertFalse( node1.hasProperty( key1 ) );
            assertFalse( node1.hasProperty( null ) );
            node1.setProperty( key1, int1 );
            node1.setProperty( key2, int2 );
            node1.setProperty( key3, string );
            Iterator<String> keys = node1.getPropertyKeys().iterator();
            keys.next();
            keys.next();
            keys.next();
            Map<String,Object> properties = node1.getAllProperties();
            assertEquals( properties.get( key1 ), int1 );
            assertEquals( properties.get( key2 ), int2 );
            assertEquals( properties.get( key3 ), string );
            properties = node1.getProperties( key1, key2 );
            assertEquals( properties.get( key1 ), int1 );
            assertEquals( properties.get( key2 ), int2 );
            assertFalse( properties.containsKey( key3 ) );

            properties = node1.getProperties();
            assertTrue( properties.isEmpty() );

            assertThrows( NullPointerException.class, () ->
            {
                String[] names = null;
                node1.getProperties( names );
                fail();
            } );

            assertThrows( NullPointerException.class, () ->
            {
                String[] names = new String[]{null};
                node1.getProperties( names );
            } );

            node1.removeProperty( key3 );
            assertFalse( node1.hasProperty( key3 ) );
            assertFalse( node1.hasProperty( null ) );
            node1.delete();
        }
    }

    @Test
    void testAddPropertyThenDelete()
    {
        Node node;
        try ( Transaction transaction = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( "test", "test" );
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            node.setProperty( "test2", "test2" );
            node.delete();
            transaction.commit();
        }
    }

    @Test
    void testChangeProperty()
    {
        Node node;
        try ( Transaction transaction = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( "test", "test1" );
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            node.setProperty( "test", "test2" );
            node.removeProperty( "test" );
            node.setProperty( "test", "test3" );
            assertEquals( "test3", node.getProperty( "test" ) );
            node.removeProperty( "test" );
            node.setProperty( "test", "test4" );
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            assertEquals( "test4", node.getProperty( "test" ) );
        }
    }

    @Test
    void testChangeProperty2()
    {
        Node node;
        try ( Transaction transaction = db.beginTx() )
        {
            node = db.createNode();
            node.setProperty( "test", "test1" );
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            node.removeProperty( "test" );
            node.setProperty( "test", "test3" );
            assertEquals( "test3", node.getProperty( "test" ) );
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            assertEquals( "test3", node.getProperty( "test" ) );
            node.removeProperty( "test" );
            node.setProperty( "test", "test4" );
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            assertEquals( "test4", node.getProperty( "test" ) );
        }
    }
}
