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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;

public class TestAutoIndexing
{
    protected static final File WorkDir = new File( "target" + File.separator
                                                    + "var", "autoIndexer" );

    private GraphDatabaseService graphDb;
    private Transaction tx;
    private Map<String, String> config;

    private void newTransaction()
    {
        if ( tx != null )
        {
            tx.success();
            tx.finish();
        }
        tx = graphDb.beginTx();
    }

    private Map<String, String> getConfig()
    {
        if ( config == null )
        {
            return Collections.emptyMap();
        }
        return config;
    }

    @Before
    public void startDb()
    {
        WorkDir.mkdir();
        graphDb = new EmbeddedGraphDatabase( WorkDir.getAbsolutePath(),
                getConfig() );
    }

    @After
    public void stopDb() throws Exception
    {
        if ( tx != null )
        {
            tx.finish();
        }
        if ( graphDb != null )
        {
            graphDb.shutdown();
        }
        FileUtils.deleteRecursively( WorkDir );
        tx = null;
        config = null;
        graphDb = null;
    }

    @Test
    public void testNodeAutoIndexFromAPISanity()
    {
        AutoIndexer autoIndexer = graphDb.index().getAutoIndexer();
        autoIndexer.startAutoIndexingNodeProperty( "test_uuid" );
        autoIndexer.setAutoIndexingEnabled( true );
        newTransaction();

        Node node1 = graphDb.createNode();
        node1.setProperty( "test_uuid", "node1" );
        Node node2 = graphDb.createNode();
        node2.setProperty( "test_uuid", "node2" );

        // will index on commit
        assertFalse( autoIndexer.getNodeIndex().get( "test_uuid", "node1" ).hasNext() );
        assertFalse( autoIndexer.getNodeIndex().get( "test_uuid", "node2" ).hasNext() );

        newTransaction();

        assertEquals(
                node1,
                autoIndexer.getNodeIndex().get( "test_uuid", "node1" ).getSingle() );
        assertEquals(
                node2,
                autoIndexer.getNodeIndex().get( "test_uuid", "node2" ).getSingle() );
    }

    @Test
    public void testRelationshipAutoIndexFromAPISanity()
    {
        final String propNameToIndex = "test";
        AutoIndexer autoIndexer = graphDb.index().getAutoIndexer();
        autoIndexer.startAutoIndexingRelationshipProperty( propNameToIndex );
        autoIndexer.setAutoIndexingEnabled( true );
        newTransaction();

        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        Node node3 = graphDb.createNode();

        Relationship rel12 = node1.createRelationshipTo( node2,
                DynamicRelationshipType.withName( "DYNAMIC" ) );
        Relationship rel23 = node2.createRelationshipTo( node3,
                DynamicRelationshipType.withName( "DYNAMIC" ) );

        rel12.setProperty( propNameToIndex, "rel12" );
        rel23.setProperty( propNameToIndex, "rel23" );

        // will index on commit
        assertFalse( autoIndexer.getRelationshipIndex().get( propNameToIndex,
                "rel12" ).hasNext() );
        assertFalse( autoIndexer.getRelationshipIndex().get( propNameToIndex,
                "rel23" ).hasNext() );

        newTransaction();

        assertEquals(
                rel12,
                autoIndexer.getRelationshipIndex().get( propNameToIndex,
                        "rel12" ).getSingle() );
        assertEquals(
                rel23,
                autoIndexer.getRelationshipIndex().get( propNameToIndex,
                        "rel23" ).getSingle() );
    }

    @Test
    public void testSmallGraphWithNonIndexableProps() throws Exception
    {
        stopDb();
        config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "nodeProp1, nodeProp2" );
        config.put( Config.RELATIONSHIP_KEYS_INDEXABLE, "relProp1, relProp2" );
        config.put( Config.AUTO_INDEXING_ENABLED, "true" );
        startDb();

        assertTrue( graphDb.index().getAutoIndexer().isAutoIndexingEnabled() );

        newTransaction();

        // Build the graph, a 3-cycle
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        Node node3 = graphDb.createNode();

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
        AutoIndexer autoIndexer = graphDb.index().getAutoIndexer();
        assertEquals(
                node1,
                autoIndexer.getNodeIndex().get( "nodeProp1", "node1Value1" ).getSingle() );
        assertEquals(
                node2,
                autoIndexer.getNodeIndex().get( "nodeProp2", "node2Value1" ).getSingle() );
        assertEquals(
                node3,
                autoIndexer.getNodeIndex().get( "nodeProp1", "node3Value1" ).getSingle() );
        assertEquals(
                node3,
                autoIndexer.getNodeIndex().get( "nodeProp2", "node3Value2" ).getSingle() );
        assertFalse( autoIndexer.getNodeIndex().get( "nodePropNonIndexable1",
                "node1ValueNonIndexable" ).hasNext() );
        assertFalse( autoIndexer.getNodeIndex().get( "nodePropNonIndexable2",
                "node2ValueNonIndexable" ).hasNext() );
        assertFalse( autoIndexer.getNodeIndex().get( "nodePropNonIndexable3",
                "node3ValueNonIndexable" ).hasNext() );

        assertEquals(
                rel12,
                autoIndexer.getRelationshipIndex().get( "relProp1",
                        "rel12Value1" ).getSingle() );
        assertEquals(
                rel23,
                autoIndexer.getRelationshipIndex().get( "relProp2",
                        "rel23Value1" ).getSingle() );
        assertEquals(
                rel31,
                autoIndexer.getRelationshipIndex().get( "relProp1",
                        "rel31Value1" ).getSingle() );
        assertEquals(
                rel31,
                autoIndexer.getRelationshipIndex().get( "relProp2",
                        "rel31Value2" ).getSingle() );
        assertFalse( autoIndexer.getRelationshipIndex().get(
                "relPropNonIndexable1",
                "rel12ValueNonIndexable" ).hasNext() );
        assertFalse( autoIndexer.getRelationshipIndex().get(
                "relPropNonIndexable2",
                "rel23ValueNonIndexable" ).hasNext() );
        assertFalse( autoIndexer.getRelationshipIndex().get(
                "relPropNonIndexable3",
                "rel31ValueNonIndexable" ).hasNext() );
    }

    @Test
    public void testDefaultIsOff()
    {
        newTransaction();
        Node node1 = graphDb.createNode();
        node1.setProperty( "testProp", "node1" );

        newTransaction();
        AutoIndexer autoIndexer = graphDb.index().getAutoIndexer();
        assertFalse( autoIndexer.getNodeIndex().get( "testProp", "node1" ).hasNext() );
    }

    @Test
    public void testDefaultIsOffIfExplicit() throws Exception
    {
        stopDb();
        config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "nodeProp1, nodeProp2" );
        config.put( Config.RELATIONSHIP_KEYS_INDEXABLE, "relProp1, relProp2" );
        config.put( Config.AUTO_INDEXING_ENABLED, "false" );
        startDb();

        AutoIndexer autoIndexer = graphDb.index().getAutoIndexer();
        autoIndexer.startAutoIndexingNodeProperty( "testProp" );
        newTransaction();

        Node node1 = graphDb.createNode();
        node1.setProperty( "nodeProp1", "node1" );
        node1.setProperty( "nodeProp2", "node1" );
        node1.setProperty( "testProp", "node1" );

        newTransaction();

        assertFalse( autoIndexer.getNodeIndex().get( "nodeProp1", "node1" ).hasNext() );
        assertFalse( autoIndexer.getNodeIndex().get( "nodeProp2", "node1" ).hasNext() );
        assertFalse( autoIndexer.getNodeIndex().get( "testProp", "node1" ).hasNext() );
    }

    @Test
    public void testDefaultsAreSeparateForNodesAndRelationships()
            throws Exception
    {
        stopDb();
        config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "propName" );
        config.put( Config.AUTO_INDEXING_ENABLED, "true" );
        // Now only node properties named propName should be indexed.
        startDb();

        newTransaction();

        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        node1.setProperty( "propName", "node1" );
        node2.setProperty( "propName", "node2" );
        node2.setProperty( "propName_", "node2" );

        Relationship rel = node1.createRelationshipTo( node2,
                DynamicRelationshipType.withName( "DYNAMIC" ) );
        rel.setProperty( "propName", "rel1" );

        newTransaction();

        AutoIndexer autoIndexer = graphDb.index().getAutoIndexer();
        assertEquals( node1,
                autoIndexer.getNodeIndex().get( "propName", "node1" ).getSingle() );
        assertEquals(
                node2,
                autoIndexer.getNodeIndex().get( "propName", "node2" ).getSingle() );
        assertFalse( autoIndexer.getRelationshipIndex().get( "propName", "rel1" ).hasNext() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void testIfBothListsSetFromConfigResultsInError() throws Exception
    {
        stopDb();
        config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "foo" );
        config.put( Config.NODE_KEYS_NON_INDEXABLE, "bar" );
        startDb();
    }

    @Test
    public void testIfBothListsSetFromAPIResultsInError()
    {
        AutoIndexer autoIndexer = graphDb.index().getAutoIndexer();
        autoIndexer.setAutoIndexingEnabled( true );
        autoIndexer.startAutoIndexingNodeProperty( "testProp" );
        // This is fine, we can remove already added stuff
        autoIndexer.stopAutoIndexingNodeProperty( "testProp" );

        // This is to prepare for failing below
        autoIndexer.startAutoIndexingRelationshipProperty( "inThere" );
        try
        {
            autoIndexer.startIgnoringRelationshipProperty( "notThere" );
            fail( "Removing non-monitored property while there are auto indexed ones is an error" );
        }
        catch(Exception e)
        {
            // it was deliberate
        }

        autoIndexer.stopAutoIndexingRelationshipProperty( "inThere" );
        // This should succeed, both lists are empty
        autoIndexer.startIgnoringRelationshipProperty( "dontWannaSee" );
        // Stop monitoring the above. This just removes it.

        try
        {
            autoIndexer.startAutoIndexingRelationshipProperty( "notThere" );
            fail( "Adding a new auto indexed property while there are non-auto indexed ones is an error" );
        }
        catch ( Exception e )
        {
            // it was deliberate
        }
    }

    @Test
    public void testStartStopAutoIndexing() throws Exception
    {
        stopDb();
        config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "propName" );
        config.put( Config.AUTO_INDEXING_ENABLED, "true" );
        // Now only node properties named propName should be indexed.
        startDb();

        AutoIndexer autoIndexer = graphDb.index().getAutoIndexer();

        newTransaction();

        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        node1.setProperty( "propName", "node" );
        autoIndexer.setAutoIndexingEnabled( false );
        // Committing with auto indexing off, should not be in the index
        newTransaction();

        assertFalse( autoIndexer.getNodeIndex().get( "nodeProp1", "node1" ).hasNext() );
        autoIndexer.setAutoIndexingEnabled( true );
        node2.setProperty( "propName", "node" );

        newTransaction();

        assertEquals( node2,
                autoIndexer.getNodeIndex().get( "propName", "node" ).getSingle() );
    }
}