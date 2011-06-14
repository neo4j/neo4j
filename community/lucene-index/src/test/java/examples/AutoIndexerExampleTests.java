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
package examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.AutoIndex;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class AutoIndexerExampleTests
{

    private String getStoreDir( String testName )
    {
        File base = new File( "target", "example-auto-index" );
        return new File( base.getAbsolutePath(), testName ).getAbsolutePath();
    }

    @Test
    public void testConfig()
    {
        // START SNIPPET: ConfigAutoIndexer

        /*
         * Creating the configuration, adding nodeProp1 and nodeProp2 as
         * auto indexed properties for Nodes and relProp1 and relProp2 as
         * auto indexed properties for Relationships. Only those will be
         * indexed. We also have to enable auto indexing for both these
         * primitives explicitly.
         */
        Map<String, String> config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "nodeProp1, nodeProp2" );
        config.put( Config.RELATIONSHIP_KEYS_INDEXABLE, "relProp1, relProp2" );
        config.put( Config.NODE_AUTO_INDEXING, "true" );
        config.put( Config.RELATIONSHIP_AUTO_INDEXING, "true" );

        EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase(
                getStoreDir( "testConfig" ), config );

        Transaction tx = graphDb.beginTx();
        Node node1 = null, node2 = null;
        Relationship rel = null;
        try
        {
            // Create the primitives
            node1 = graphDb.createNode();
            node2 = graphDb.createNode();
            rel = node1.createRelationshipTo( node2,
                    DynamicRelationshipType.withName( "DYNAMIC" ) );

            // Add indexable and non-indexable properties
            node1.setProperty( "nodeProp1", "nodeProp1Value" );
            node2.setProperty( "nodeProp2", "nodeProp2Value" );
            node1.setProperty( "nonIndexed", "nodeProp2NonIndexedValue" );
            rel.setProperty( "relProp1", "relProp1Value" );
            rel.setProperty( "relPropNonIndexed", "relPropValueNonIndexed" );

            // Make things persistent
            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
        }
        finally
        {
            tx.finish();
        }
        // END SNIPPET: ConfigAutoIndexer

        // START SNIPPET: APIReadAutoIndex
        // Get the Node auto index
        AutoIndex<Node> autoNodeIndex = graphDb.index().getNodeAutoIndexer().getAutoIndex();
        // node1 and node2 both had auto indexed properties, get them
        assertEquals( node1,
                autoNodeIndex.get( "nodeProp1", "nodeProp1Value" ).getSingle() );
        assertEquals( node2,
                autoNodeIndex.get( "nodeProp2", "nodeProp2Value" ).getSingle() );
        // node2 also had a property that should be ignored.
        assertFalse( autoNodeIndex.get( "nonIndexed",
                "nodeProp2NonIndexedValue" ).hasNext() );

        // Get the relationship auto index
        AutoIndex<Relationship> autoRelIndex = graphDb.index().getRelationshipAutoIndexer().getAutoIndex();
        // One property was set for auto indexing
        assertEquals( rel,
                autoRelIndex.get( "relProp1", "relProp1Value" ).getSingle() );
        // The rest should be ignored
        assertFalse( autoRelIndex.get( "relPropNonIndexed",
                "relPropValueNonIndexed" ).hasNext() );
        // END SNIPPET: APIReadAutoIndex
    }

    @Test
    public void testConfigIgnore()
    {
        // START SNIPPET: ConfigAutoIndexIgnore

        /*
         * Creating the configuration, adding nodeProp1 and nodeProp2 as
         * ignored properties for Nodes and relProp1 and relProp2 as
         * ignored properties for Relationships. Those are the only ones
         * not indexed. We also have to enable auto indexing for both these
         * primitives explicitly.
         */
        Map<String, String> config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_NON_INDEXABLE, "nodeProp1, nodeProp2" );
        config.put( Config.RELATIONSHIP_KEYS_NON_INDEXABLE,
                "relProp1, relProp2" );
        config.put( Config.NODE_AUTO_INDEXING, "true" );
        config.put( Config.RELATIONSHIP_AUTO_INDEXING, "true" );

        EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase(
                getStoreDir( "testConfigIgnore" ), config );

        Transaction tx = graphDb.beginTx();
        Node node1 = null, node2 = null;
        Relationship rel = null;
        try
        {
            // Create the primitives
            node1 = graphDb.createNode();
            node2 = graphDb.createNode();
            rel = node1.createRelationshipTo( node2,
                    DynamicRelationshipType.withName( "DYNAMIC" ) );

            // Add indexable and non-indexable properties
            node1.setProperty( "nodeProp1", "nodeProp1Value" );
            node2.setProperty( "nodeProp2", "nodeProp2Value" );
            node2.setProperty( "Indexed", "nodeProp2IndexedValue" );
            rel.setProperty( "relProp1", "relProp1Value" );
            rel.setProperty( "relPropIndexed", "relPropValueIndexed" );

            // Make things persistent
            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
        }
        finally
        {
            tx.finish();
        }
        // END SNIPPET: ConfigAutoIndexIgnore

        // Get the Node auto index
        AutoIndex<Node> autoNodeIndex = graphDb.index().getNodeAutoIndexer().getAutoIndex();
        // node1 and node2 both had properties that the auto index should have
        // ignored.
        assertFalse( autoNodeIndex.get( "nodeProp1", "nodeProp1Value" ).hasNext() );
        assertFalse( autoNodeIndex.get( "nodeProp2", "nodeProp2Value" ).hasNext() );
        // node2 also had a property that should be indexed.
        assertEquals(
                node2,
                autoNodeIndex.get( "Indexed", "nodeProp2IndexedValue" ).getSingle() );

        // Get the relationship auto index
        AutoIndex<Relationship> autoRelIndex = graphDb.index().getRelationshipAutoIndexer().getAutoIndex();
        // One property was set as ignored
        assertFalse( autoRelIndex.get( "relProp1", "relProp1Value" ).hasNext() );
        // The rest should be indexed
        assertEquals(
                rel,
                autoRelIndex.get( "relPropIndexed", "relPropValueIndexed" ).getSingle() );
    }

    @Test
    public void testAPI()
    {
        // START SNIPPET: APIAutoIndexer

        // Start without any configuration
        EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase(
                getStoreDir( "testAPI" ) );

        // Get the Node AutoIndexer, set nodeProp1 and nodeProp2 as auto
        // indexed.
        AutoIndexer<Node> nodeAutoIndexer = graphDb.index().getNodeAutoIndexer();
        nodeAutoIndexer.startAutoIndexingProperty( "nodeProp1" );
        nodeAutoIndexer.startAutoIndexingProperty( "nodeProp2" );

        // Get the Relationship AutoIndexer, set relProp1 as ignored.
        AutoIndexer<Relationship> relAutoIndexer = graphDb.index().getRelationshipAutoIndexer();
        relAutoIndexer.startIgnoringProperty( "relProp1" );

        // None of the AutoIndexers are enabled so far. Do that now
        nodeAutoIndexer.setEnabled( true );
        relAutoIndexer.setEnabled( true );

        // END SNIPPET: APIAutoIndexer

        Transaction tx = graphDb.beginTx();
        Node node1 = null, node2 = null;
        Relationship rel = null;
        try
        {
            // Create the primitives
            node1 = graphDb.createNode();
            node2 = graphDb.createNode();
            rel = node1.createRelationshipTo( node2,
                    DynamicRelationshipType.withName( "DYNAMIC" ) );

            // Add indexable and non-indexable properties
            node1.setProperty( "nodeProp1", "nodeProp1Value" );
            node2.setProperty( "nodeProp2", "nodeProp2Value" );
            node1.setProperty( "nonIndexed", "nodeProp2NonIndexedValue" );
            rel.setProperty( "relProp1", "relProp1Value" );
            rel.setProperty( "relPropIndexed", "relPropValueIndexed" );

            // Make things persistent
            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
        }
        finally
        {
            tx.finish();
        }

        // Get the Node auto index
        AutoIndex<Node> autoNodeIndex = nodeAutoIndexer.getAutoIndex();
        // node1 and node2 both had auto indexed properties, get them
        assertEquals( node1,
                autoNodeIndex.get( "nodeProp1", "nodeProp1Value" ).getSingle() );
        assertEquals( node2,
                autoNodeIndex.get( "nodeProp2", "nodeProp2Value" ).getSingle() );
        // node2 also had a property that should be ignored.
        assertFalse( autoNodeIndex.get( "nonIndexed",
                "nodeProp2NonIndexedValue" ).hasNext() );

        // Get the relationship auto index
        AutoIndex<Relationship> autoRelIndex = relAutoIndexer.getAutoIndex();
        // One property was set as ignored
        assertFalse( autoRelIndex.get( "relProp1", "relProp1Value" ).hasNext() );
        // The rest should be indexed
        assertEquals(
                rel,
                autoRelIndex.get( "relPropIndexed", "relPropValueIndexed" ).getSingle() );

    }

    @Test
    public void testAPIOverConfig()
    {
        // START SNIPPET: APIOverConfigAutoIndexer

        /*
         * Set the property keys nodeProp1 and nodeProp2 as indexable
         * for Nodes and the key relProp1 as non indexable for Relationships.
         * Enable auto indexing.
         */
        Map<String, String> config = new HashMap<String, String>();
        config.put( Config.NODE_KEYS_INDEXABLE, "nodeProp1, nodeProp2" );
        config.put( Config.RELATIONSHIP_KEYS_NON_INDEXABLE,
                "relPropNonIndexable" );
        config.put( Config.NODE_AUTO_INDEXING, "true" );
        config.put( Config.RELATIONSHIP_AUTO_INDEXING, "true" );

        EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase(
                getStoreDir( "testAPIOverConfig" ), config );

        AutoIndexer<Node> nodeAutoIndexer = graphDb.index().getNodeAutoIndexer();
        /*
         * Now auto indexing is enabled. This means that if we add a
         * non indexable property for Nodes we will get an IllegalStateException.
         */

        try
        {
            nodeAutoIndexer.startIgnoringProperty( "nodeProp2" );
            fail( "Configuration from db startup and through the API should be consolidated" );
        }
        catch ( IllegalStateException e )
        {
            // Good
        }

        /*
         * So, the above does not work. We can either remove one by one
         * the auto indexed property names and then  add the ignored or
         * we can disable auto indexing for Nodes, make changes
         * in any order and then re-enable it. Here we do the latter.
         */
        nodeAutoIndexer.setEnabled( false );
        /*
         * Now this should work
         */
        nodeAutoIndexer.startIgnoringProperty( "nodeProp2" );
        nodeAutoIndexer.stopAutoIndexingProperty( "nodeProp1" );
        nodeAutoIndexer.stopAutoIndexingProperty( "nodeProp2" );
        // Now we are consistent. Re-enable it.
        nodeAutoIndexer.setEnabled( true );

        // END SNIPPET: APIOverConfigAutoIndexer

        // We also close down the Relationship auto indexing
        AutoIndexer<Relationship> relAutoIndexer = graphDb.index().getRelationshipAutoIndexer();
        relAutoIndexer.setEnabled( false );

        Transaction tx = graphDb.beginTx();
        Node node1 = null, node2 = null;
        Relationship rel = null;
        try
        {
            // Create the primitives
            node1 = graphDb.createNode();
            node2 = graphDb.createNode();
            rel = node1.createRelationshipTo( node2,
                    DynamicRelationshipType.withName( "DYNAMIC" ) );

            // Add indexable and non-indexable properties
            node1.setProperty( "nodeProp1", "nodeProp1Value" );
            node2.setProperty( "nodeProp2", "nodeProp2Value" );
            rel.setProperty( "relProp2", "relProp1Value" );

            // Make things persistent
            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
        }
        finally
        {
            tx.finish();
        }

        // Get the Node auto index
        AutoIndex<Node> autoNodeIndex = nodeAutoIndexer.getAutoIndex();
        // node1 had auto indexed properties, get them
        assertEquals( node1,
                autoNodeIndex.get( "nodeProp1", "nodeProp1Value" ).getSingle() );
        // node2 also a property that should be ignored.
        assertFalse( autoNodeIndex.get( "nodeProp2", "nodeProp2Value" ).hasNext() );

        // Get the relationship auto index
        AutoIndex<Relationship> autoRelIndex = relAutoIndexer.getAutoIndex();
        // The auto indexer is off - nothing should be returned
        assertFalse( autoRelIndex.get( "relProp2", "relProp1Value" ).hasNext() );

    }

    @Test
    public void testSemantics()
    {
        // START SNIPPET: Semantics
        Map<String, String> config = new HashMap<String, String>();
        config.put( Config.RELATIONSHIP_KEYS_NON_INDEXABLE,
                "relPropNonIndexable" );
        config.put( Config.NODE_AUTO_INDEXING, "true" );
        config.put( Config.RELATIONSHIP_AUTO_INDEXING, "true" );

        EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase(
                getStoreDir( "testSemantics" ), config );

        AutoIndexer<Node> nodeAutoIndexer = graphDb.index().getNodeAutoIndexer();
        Node node1 = null, node2 = null;
        Transaction tx = graphDb.beginTx();
        try
        {
            node1 = graphDb.createNode();
            node1.setProperty( "nodeProp1", "value1" );
            // Since we have not committed yet, this is not in the index.
            assertFalse( nodeAutoIndexer.getAutoIndex().get( "nodeProp1",
                    "value1" ).hasNext() );
            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
        }
        finally
        {
            tx.finish();
        }
        // Now it is committed. We will set that same property to ignored.
        nodeAutoIndexer.startIgnoringProperty( "nodeProp1" );
        tx = graphDb.beginTx();
        try
        {
            node2 = graphDb.createNode();
            node2.setProperty( "nodeProp1", "value1" );
            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
        }
        finally
        {
            tx.finish();
        }
        /*
         *  The index still has only one entry, the one from the first transaction.
         *  This is because node1 was not touched so it was not included in the transaction
         *  data during beforeCommit().
         */
        assertEquals(
                node1,
                nodeAutoIndexer.getAutoIndex().get( "nodeProp1", "value1" ).getSingle() );
        /*
         * We change the value of nodeProp1 for node1 and see it being ignored - the old value remains.
         */
        tx = graphDb.beginTx();
        try
        {
            node1.setProperty( "nodeProp1", "value2" );
            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
        }
        finally
        {
            tx.finish();
        }
        // It is not there, for either values of nodeProp1.
        assertEquals(
                node1,
                nodeAutoIndexer.getAutoIndex().get( "nodeProp1", "value1" ).getSingle() );
        // END SNIPPET: Semantics
    }
}
