/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestAutoIndexing extends AbstractNeo4jTestCase
{
    private static Map<String, String> config;

    @Override
    protected boolean restartGraphDbBetweenTests()
    {
        return true;
    }

    protected static Map<String, String> getConfig()
    {
        return config;
    }

    @Test
    public void testNodeAutoIndexFromAPISanity()
    {
        AutoIndexer autoIndexer = getGraphDb().index().getAutoIndexer();
        autoIndexer.addAutoIndexingForNodeProperty( "test_uuid" );
        newTransaction();

        Node node1 = getGraphDb().createNode();
        node1.setProperty( "test_uuid", "node1" );
        Node node2 = getGraphDb().createNode();
        node2.setProperty( "test_uuid", "node2" );

        // will index on commit
        assertFalse( autoIndexer.getNodesFor( "test_uuid", "node1" ).hasNext() );
        assertFalse( autoIndexer.getNodesFor( "test_uuid", "node2" ).hasNext() );

        newTransaction();

        assertEquals( node1, autoIndexer.getNodesFor( "test_uuid", "node1" ).getSingle() );
        assertEquals( node2, autoIndexer.getNodesFor( "test_uuid", "node2" ).getSingle() );
    }

    @Test
    public void testRelationshipAutoIndexFromAPISanity()
    {
        AutoIndexer autoIndexer = getGraphDb().index().getAutoIndexer();
        autoIndexer.addAutoIndexingForRelationshipProperty( "test_uuid" );
        newTransaction();

        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Node node3 = getGraphDb().createNode();

        Relationship rel12 = node1.createRelationshipTo( node2,
                DynamicRelationshipType.withName( "DYNAMIC" ) );
        Relationship rel23 = node2.createRelationshipTo( node3,
                DynamicRelationshipType.withName( "DYNAMIC" ) );

        rel12.setProperty( "test_uuid", "rel12" );
        rel23.setProperty( "test_uuid", "rel23" );

        // will index on commit
        assertFalse( autoIndexer.getRelationshipsFor( "test_uuid", "rel12" ).hasNext() );
        assertFalse( autoIndexer.getRelationshipsFor( "test_uuid", "rel23" ).hasNext() );

        newTransaction();

        assertEquals(
                rel12,
                autoIndexer.getRelationshipsFor( "test_uuid", "rel12" ).getSingle() );
        assertEquals(
                rel23,
                autoIndexer.getRelationshipsFor( "test_uuid", "rel23" ).getSingle() );
    }

    @Test
    public void testSmallGraphWithNonIndexableProps()
    {
        tearDownTest(); // To restart with custom configuration
        config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "nodeProp1, nodeProp2" );
        config.put( Config.RELATIONSHIP_KEYS_INDEXABLE, "relProp1, relProp2" );
        setUpTest();

        newTransaction();

        // Build the graph, a 3-cycle
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Node node3 = getGraphDb().createNode();

        Relationship rel12 = node1.createRelationshipTo( node2,
                DynamicRelationshipType.withName( "DYNAMIC" ) );
        Relationship rel23 = node2.createRelationshipTo( node3,
                DynamicRelationshipType.withName( "DYNAMIC" ) );
        Relationship rel31 = node3.createRelationshipTo( node1,
                DynamicRelationshipType.withName( "DYNAMIC" ) );

        // Nodes
        node1.setProperty( "nodeProp1", "node1Value1" );
        node1.setProperty( "nodePropNonIndexable1", "node1ValueNonIndexable" );

        node2.setProperty( "nodeProp2", "node2Value1" );
        node2.setProperty( "nodePropNonIndexable2", "node2ValueNonIndexable" );

        node3.setProperty( "nodeProp1", "node3Value1" );
        node3.setProperty( "nodeProp2", "node3Value2" );
        node3.setProperty( "nodePropNonIndexable3", "node3ValueNonIndexable" );

        // Relationships
        rel12.setProperty( "relProp1", "rel12Value1" );
        rel12.setProperty( "relPropNonIndexable1", "rel12ValueNonIndexable" );

        rel23.setProperty( "relProp2", "rel23Value1" );
        rel23.setProperty( "nodePropNonIndexable2", "rel23ValueNonIndexable" );

        rel31.setProperty( "relProp1", "rel31Value1" );
        rel31.setProperty( "relProp2", "rel31Value2" );
        rel31.setProperty( "relPropNonIndexable3", "rel31ValueNonIndexable" );

        newTransaction();

        // Committed, time to check
        AutoIndexer autoIndexer = getGraphDb().index().getAutoIndexer();
        assertEquals(
                node1,
                autoIndexer.getNodesFor( "nodeProp1", "node1Value1" ).getSingle() );
        assertEquals(
                node2,
                autoIndexer.getNodesFor( "nodeProp2", "node2Value1" ).getSingle() );
        assertEquals(
                node3,
                autoIndexer.getNodesFor( "nodeProp1", "node3Value1" ).getSingle() );
        assertEquals(
                node3,
                autoIndexer.getNodesFor( "nodeProp2", "node3Value2" ).getSingle() );
        assertFalse( autoIndexer.getNodesFor( "nodePropNonIndexable1",
                "node1ValueNonIndexable" ).hasNext() );
        assertFalse( autoIndexer.getNodesFor( "nodePropNonIndexable2",
                "node2ValueNonIndexable" ).hasNext() );
        assertFalse( autoIndexer.getNodesFor( "nodePropNonIndexable3",
                "node3ValueNonIndexable" ).hasNext() );

        assertEquals(
                rel12,
                autoIndexer.getNodesFor( "relProp1", "rel12Value1" ).getSingle() );
        assertEquals(
                rel23,
                autoIndexer.getNodesFor( "relProp2", "rel23Value1" ).getSingle() );
        assertEquals(
                rel31,
                autoIndexer.getNodesFor( "relProp1", "rel31Value1" ).getSingle() );
        assertEquals(
                rel31,
                autoIndexer.getNodesFor( "relProp2", "rel31Value2" ).getSingle() );
        assertFalse( autoIndexer.getNodesFor( "relPropNonIndexable1",
                "rel12ValueNonIndexable" ).hasNext() );
        assertFalse( autoIndexer.getNodesFor( "relPropNonIndexable2",
                "rel23ValueNonIndexable" ).hasNext() );
        assertFalse( autoIndexer.getNodesFor( "relPropNonIndexable3",
                "rel31ValueNonIndexable" ).hasNext() );
    }

    @Test
    public void testDefaultIsOff()
    {
        Node node1 = getGraphDb().createNode();
        node1.setProperty( "testProp", "node1" );

        newTransaction();
        AutoIndexer autoIndexer = getGraphDb().index().getAutoIndexer();
        assertFalse( autoIndexer.getNodesFor( "testProp", "node1" ).hasNext() );
    }

    @Test
    public void testDefaultIsOffIfExplicit()
    {
        tearDownTest(); // To restart with custom configuration
        config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "nodeProp1, nodeProp2" );
        config.put( Config.RELATIONSHIP_KEYS_INDEXABLE, "relProp1, relProp2" );
        config.put( Config.AUTO_INDEXING_ENABLED, "false" );
        setUpTest();

        AutoIndexer autoIndexer = getGraphDb().index().getAutoIndexer();
        autoIndexer.addAutoIndexingForNodeProperty( "testProp" );
        newTransaction();

        Node node1 = getGraphDb().createNode();
        node1.setProperty( "nodeProp1", "node1" );
        node1.setProperty( "nodeProp2", "node1" );
        node1.setProperty( "testProp", "node1" );

        newTransaction();

        assertFalse( autoIndexer.getNodesFor( "nodeProp1", "node1" ).hasNext() );
        assertFalse( autoIndexer.getNodesFor( "nodeProp2", "node1" ).hasNext() );
        assertFalse( autoIndexer.getNodesFor( "testProp", "node1" ).hasNext() );
    }

    @Test
    public void testDefaultsAreSeparateForNodesAndRelationships()
    {
        tearDownTest(); // To restart with custom configuration
        config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "propName" );
        setUpTest();
        // Now only node properties named propName should be indexed.

        newTransaction();

        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        node1.setProperty( "propName", "node1" );
        node2.setProperty( "propName", "node2" );
        node2.setProperty( "propName_", "node2" );

        Relationship rel = node1.createRelationshipTo( node2,
                DynamicRelationshipType.withName( "DYNAMIC" ) );
        rel.setProperty( "propName", "rel1" );

        newTransaction();

        AutoIndexer autoIndexer = getGraphDb().index().getAutoIndexer();
        assertEquals( node1,
                autoIndexer.getNodesFor( "propName", "node1" ).getSingle() );
        assertEquals( node1,
                autoIndexer.getNodesFor( "propName", "node2" ).getSingle() );
        assertFalse( autoIndexer.getRelationshipsFor( "propName", "rel1" ).hasNext() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void testIfBothListsSetFromConfigResultsInError()
    {
        tearDownTest(); // To restart with custom configuration
        config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "foo" );
        config.put( Config.NODE_KEYS_NON_INDEXABLE, "bar" );
        setUpTest();
    }

    @Test
    public void testIfBothListsSetFromAPIResultsInError()
    {
        AutoIndexer autoIndexer = getGraphDb().index().getAutoIndexer();
        autoIndexer.addAutoIndexingForNodeProperty( "testProp" );
        // This is fine, we can remove already added stuff
        autoIndexer.removeAutoIndexingForNodeProperty( "testProp" );

        autoIndexer.addAutoIndexingForRelationshipProperty( "inThere" );
        try
        {
            autoIndexer.removeAutoIndexingForRelationshipProperty( "notThere" );
            fail("Removing non added auto indexed property while there are auto indexed ones is an error");
        }
        catch(Exception e)
        {
            // it was deliberate
        }

        autoIndexer.removeAutoIndexingForRelationshipProperty( "inThere" );
        // This should succeed, both lists are empty
        autoIndexer.removeAutoIndexingForRelationshipProperty( "dontWannaSee" );
        // This should succeed since it was in the ignore list
        autoIndexer.addAutoIndexingForRelationshipProperty( "dontWannaSee" );
        // Put it in the ignore list, get ready for the failure below
        autoIndexer.removeAutoIndexingForRelationshipProperty( "dontWannaSee" );

        try
        {
            autoIndexer.addAutoIndexingForRelationshipProperty( "notThere" );
            fail( "Adding a new auto indexed property while there are non-auto indexed ones is an error" );
        }
        catch ( Exception e )
        {
            // it was deliberate
        }
    }
}