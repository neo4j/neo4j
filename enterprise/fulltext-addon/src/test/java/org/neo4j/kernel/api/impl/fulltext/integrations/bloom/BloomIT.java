/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Date;
import java.util.Map;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.impl.fulltext.FulltextProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomFulltextConfig.bloom_enabled;

public class BloomIT
{
    static final String NODES = "CALL bloom.searchNodes([%s])";
    static final String NODES_ADVANCED = "CALL bloom.searchNodes([%s], %b, %b)";
    static final String RELS = "CALL bloom.searchRelationships([%s])";
    static final String RELS_ADVANCED = "CALL bloom.searchRelationships([%s], %b, %b)";
    static final String ENTITYID = "entityid";
    static final String SCORE = "score";
    static final String SET_NODE_KEYS = "CALL bloom.setIndexedNodePropertyKeys([%s])";
    static final String SET_REL_KEYS = "CALL bloom.setIndexedRelationshipPropertyKeys([%s])";
    static final String GET_NODE_KEYS = "CALL bloom.getIndexedNodePropertyKeys";
    static final String GET_REL_KEYS = "CALL bloom.getIndexedRelationshipPropertyKeys";
    static final String AWAIT_POPULATION = "CALL bloom.awaitPopulation";
    static final String STATUS = "CALL bloom.indexStatus";

    @Rule
    public final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private GraphDatabaseService db;
    private GraphDatabaseBuilder builder;

    @Before
    public void before()
    {
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                         .setConfig( bloom_enabled, "true" );
    }

    @After
    public void after()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    private GraphDatabaseService getDb() throws KernelException
    {
        GraphDatabaseService db = builder.newGraphDatabase();
        ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( Procedures.class ).registerProcedure( BloomProcedures.class );
        return db;
    }

    @Test
    public void shouldPopulateAndQueryIndexes() throws Exception
    {
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\", \"relprop\"" ) );
        db.execute( String.format( SET_REL_KEYS, "\"prop\", \"relprop\"" ) );
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "This is a integration test." );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "This is a related integration test" );
            Relationship relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "type" ) );
            relationship.setProperty( "relprop", "They relate" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES, "\"integration\"") );
        assertTrue( result.hasNext() );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertTrue( result.hasNext() );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( RELS, "\"relate\"") );
        assertTrue( result.hasNext() );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void exactQueryShouldBeExact() throws Exception
    {
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        db.execute( String.format( SET_REL_KEYS, "\"prop\"" ) );
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "This is a integration test." );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "This is a related integration test" );
            Relationship relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "type" ) );
            relationship.setProperty( "prop", "They relate" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES_ADVANCED, "\"integration\"", false, false ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES_ADVANCED, "\"integratiun\"", false, false ) );
        assertFalse( result.hasNext() );

        result = db.execute( String.format( RELS_ADVANCED, "\"relate\"", false, false ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( RELS_ADVANCED, "\"relite\"", false, false ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void matchAllQueryShouldMatchAll() throws Exception
    {
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        db.execute( String.format( SET_REL_KEYS, "\"prop\"" ) );
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "This is a integration test." );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "This is a related integration test" );
            Relationship relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "type" ) );
            relationship.setProperty( "prop", "They relate" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES_ADVANCED, "\"integration\", \"related\"", false, true ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );

        result = db.execute( String.format( RELS_ADVANCED, "\"they\", \"relate\"", false, true ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( RELS_ADVANCED, "\"relate\", \"sometimes\"", false, true ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void exactMatchShouldScoreMuchBetterThatAlmostNotMatching() throws Exception
    {
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "This is a integration test that involves scoring and thus needs a longer sentence." );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "tase" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES, "\"integration\", \"test\", \"involves\", \"scoring\", \"needs\", \"sentence\"" ) );
        assertTrue( result.hasNext() );
        Map<String,Object> firstResult = result.next();
        assertTrue( result.hasNext() );
        Map<String,Object> secondResult = result.next();
        assertFalse( result.hasNext() );
        assertEquals( 0L, firstResult.get( ENTITYID ) );
        assertEquals( 1L, secondResult.get( ENTITYID ) );
        assertThat( (double) firstResult.get( SCORE ), greaterThan( (double) secondResult.get( SCORE ) * 10 ) );
    }

    @Test
    public void unsplitTokensShouldNotBreakFuzzyQuery() throws Exception
    {
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "This is a integration test that involves scoring and thus needs a longer sentence." );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "tase" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES, "\"integration test involves scoring needs sentence\"" ) );
        assertTrue( result.hasNext() );
        Map<String,Object> firstResult = result.next();
        assertTrue( result.hasNext() );
        Map<String,Object> secondResult = result.next();
        assertFalse( result.hasNext() );
        assertEquals( 0L, firstResult.get( ENTITYID ) );
        assertEquals( 1L, secondResult.get( ENTITYID ) );
        assertThat( (double) firstResult.get( SCORE ), greaterThan( (double) secondResult.get( SCORE ) * 10 ) );
    }

    @Test
    public void shouldBeAbleToConfigureAnalyzer() throws Exception
    {
        builder.setConfig( BloomFulltextConfig.bloom_default_analyzer, "org.apache.lucene.analysis.sv.SwedishAnalyzer" );
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "Det finns en mening" );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "There is a sentance" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES, "\"is\"") );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES, "\"a\"") );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES, "\"det\"") );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES, "\"en\"") );
        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldPopulateIndexWithExistingDataOnIndexCreate() throws Exception
    {
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"something\"" ) );

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "prop", "Roskildevej 32" ); // Copenhagen Zoo is important to find.
            nodeId = node.getId();
            tx.success();
        }
        Result result = db.execute( String.format( NODES, "\"Roskildevej\"") );
        assertFalse( result.hasNext() );
        db.shutdown();

        builder.setConfig( BloomFulltextConfig.bloom_default_analyzer, "org.apache.lucene.analysis.da.DanishAnalyzer" );
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );

        db.execute( AWAIT_POPULATION ).close();

        result = db.execute( String.format( NODES, "\"Roskildevej\"") );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void startupPopulationShouldNotCauseDuplicates() throws Exception
    {

        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.setProperty( "prop", "Jyllingevej" );
            tx.success();
        }

        // Verify it's indexed exactly once
        db.execute( AWAIT_POPULATION ).close();
        Result result = db.execute( String.format( NODES, "\"Jyllingevej\"") );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );

        db.shutdown();
        db = getDb();

        db.execute( AWAIT_POPULATION ).close();
        // Verify it's STILL indexed exactly once
        result = db.execute( String.format( NODES, "\"Jyllingevej\"") );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldNotBeAbleToFindNodesAfterRemovingIndex() throws Exception
    {

        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.setProperty( "prop", "Jyllingevej" );
            tx.success();
        }

        // Verify it's indexed exactly once
        db.execute( AWAIT_POPULATION ).close();
        Result result = db.execute( String.format( NODES, "\"Jyllingevej\"" ) );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        db.execute( String.format( SET_NODE_KEYS, "" ) );

        db.execute( AWAIT_POPULATION ).close();
        // Verify it's nowhere to be found now
        result = db.execute( String.format( NODES, "\"Jyllingevej\"" ) );
        assertFalse( result.hasNext() );

        db.shutdown();
        db = getDb();
        // Should not be found after restart
        result = db.execute( String.format( NODES, "\"Jyllingevej\"" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void staleDataFromEntityDeleteShouldNotBeAccessibleAfterConfigurationChange() throws Exception
    {

        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.setProperty( "prop", "Esplanaden" );
            tx.success();
        }

        db.shutdown();
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"not-prop\"" ) );

        try ( Transaction tx = db.beginTx() )
        {
            // This should no longer be indexed
            db.getNodeById( nodeId ).delete();
            tx.success();
        }

        db.shutdown();
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );

        // Verify that the node is no longer indexed
        db.execute( AWAIT_POPULATION ).close();
        Result result = db.execute( String.format( NODES, "\"Esplanaden\"") );
        assertFalse( result.hasNext() );
        result.close();
    }

    @Test
    public void staleDataFromPropertyRemovalShouldNotBeAccessibleAfterConfigurationChange() throws Exception
    {

        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.setProperty( "prop", "Esplanaden" );
            tx.success();
        }

        db.shutdown();
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"not-prop\"" ) );

        try ( Transaction tx = db.beginTx() )
        {
            // This should no longer be indexed
            db.getNodeById( nodeId ).removeProperty( "prop" );
            tx.success();
        }

        db.shutdown();
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );

        // Verify that the node is no longer indexed
        db.execute( AWAIT_POPULATION ).close();
        Result result = db.execute( String.format( NODES, "\"Esplanaden\"") );
        assertFalse( result.hasNext() );
        result.close();
    }

    @Test
    public void updatesAreAvailableToConcurrentReadTransactions() throws Exception
    {
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Langelinie Pavillinen" );
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES, "\"Langelinie\"") ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }

            Thread th = new Thread( () ->
            {
                try ( Transaction tx1 = db.beginTx() )
                {
                    db.createNode().setProperty( "prop", "Den Lille Havfrue, Langelinie" );
                    tx1.success();
                }
            } );
            th.start();
            th.join();

            try ( Result result = db.execute( String.format( NODES, "\"Langelinie\"") ) )
            {
                assertThat( Iterators.count( result ), is( 2L ) );
            }
        }
    }

    @Test
    public void shouldNotBeAbleToStartWithIllegalPropertyKey() throws Exception
    {
        expectedException.expectMessage( "It is not possible to index property keys starting with __lucene__fulltext__addon__" );
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\", \"" + FulltextProvider.FIELD_ENTITY_ID + "\", \"hello\"" ) );
    }

    @Test
    public void shouldBeAbleToRunConsistencyCheck() throws Exception
    {
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Langelinie Pavillinen" );
            tx.success();
        }
        db.shutdown();

        Config config = Config.defaults( bloom_enabled, "true" );
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService( new Date() );
        ConsistencyFlags checkConsistencyConfig = new ConsistencyFlags( true, true, true, true );
        ConsistencyCheckService.Result result =
                consistencyCheckService.runFullConsistencyCheck( testDirectory.graphDbDir(), config, ProgressMonitorFactory.NONE, NullLogProvider.getInstance(),
                        true, checkConsistencyConfig );
        assertTrue( result.isSuccessful() );
    }

    @Test
    public void shouldReindexNodesWhenAnalyzerIsChanged() throws Exception
    {
        final String ENGLISH = EnglishAnalyzer.class.getCanonicalName();
        final String SWEDISH = SwedishAnalyzer.class.getCanonicalName();

        builder.setConfig( BloomFulltextConfig.bloom_default_analyzer, ENGLISH );

        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Hello and hello again." );
            db.createNode().setProperty( "prop", "En tomte bodde i ett hus." );

            tx.success();
        }
        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES_ADVANCED, "\"and\"", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 0L ) );
            }
            try ( Result result = db.execute( String.format( NODES_ADVANCED, "\"ett\"", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
        }

        db.shutdown();
        builder.setConfig( BloomFulltextConfig.bloom_default_analyzer, SWEDISH );
        db = getDb();
        db.execute( AWAIT_POPULATION ).close();

        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES_ADVANCED, "\"and\"", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
            try ( Result result = db.execute( String.format( NODES_ADVANCED, "\"ett\"", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 0L ) );
            }
        }
    }

    @Test
    public void shouldReindexAfterBeingTemporarilyDisabled() throws Exception
    {

        // Create a node while the index is enabled.
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Hello and hello again." );
            tx.success();
        }

        // Shut down, disable the index, start up again and create a node that would have been indexed had the index
        // been enabled.
        db.shutdown();
        builder.setConfig( bloom_enabled, "false" );
        db = builder.newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "En tomte bodde i ett hus." );
            tx.success();
        }

        // Re-enable the index and restart. Wait for the index to rebuild.
        db.shutdown();
        builder.setConfig( bloom_enabled, "true" );
        db = getDb();
        db.execute( AWAIT_POPULATION ).close();

        // Now we should be able to find the node that was added while the index was disabled.
        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES_ADVANCED, "\"hello\"", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
            try ( Result result = db.execute( String.format( NODES_ADVANCED, "\"tomte\"", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
        }
    }

    @Test
    public void indexedPropertiesShouldBeSetByProcedure() throws Exception
    {
        db = getDb();

        db.execute( String.format( SET_NODE_KEYS, "\"prop\"" ) );
        // Create a node while the index is enabled.
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Hello and hello again." );
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES_ADVANCED, "\"hello\"", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
        }
    }

    @Test
    public void shouldBeAbleToQueryForIndexedProperties() throws Exception
    {
        db = getDb();

        db.execute( String.format( SET_NODE_KEYS, "\"prop\", \"otherprop\", \"proppmatt\"" ) );
        db.execute( String.format( SET_REL_KEYS, "\"ata\", \"mata\", \"matt\"" ) );

        Result result = db.execute( GET_NODE_KEYS );
        assertEquals( "otherprop", result.next().get( "propertyKey" ) );
        assertEquals( "prop", result.next().get( "propertyKey" ) );
        assertEquals( "proppmatt", result.next().get( "propertyKey" ) );
        assertFalse( result.hasNext() );

        result = db.execute( GET_REL_KEYS );
        assertEquals( "mata", result.next().get( "propertyKey" ) );
        assertEquals( "matt", result.next().get( "propertyKey" ) );
        assertEquals( "ata", result.next().get( "propertyKey" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void onlineIndexShouldBeReportedAsOnline() throws Exception
    {
        db = getDb();
        db.execute( String.format( SET_NODE_KEYS, "\"prop, otherprop, proppmatt\"" ) );
        db.execute( String.format( SET_REL_KEYS, "\"prop, otherprop, proppmatt\"" ) );

        db.execute( AWAIT_POPULATION );
        Result result = db.execute( STATUS );
        Map<String,Object> output = result.next();
        assertEquals( "ONLINE", output.get( "state" ) );
        assertEquals( "bloomNodes", output.get( "name" ) );
        output = result.next();
        assertEquals( "ONLINE", output.get( "state" ) );
        assertEquals( "bloomRelationships", output.get( "name" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void databaseShouldBeAbleToStartWithBloomPresentButDisabled() throws Exception
    {
        builder.setConfig( bloom_enabled, "false" );
        db = getDb();
        //all good.
    }

    @Test
    public void shouldThrowSomewhatHelpfulMessageIfCalledWhenDisabled() throws Exception
    {
        builder.setConfig( bloom_enabled, "false" );
        db = getDb();
        expectedException.expect( QueryExecutionException.class );
        expectedException.expectMessage( "enabled" );
        db.execute( AWAIT_POPULATION );
    }
}
