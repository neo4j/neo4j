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
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Date;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.impl.fulltext.FulltextProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomFulltextConfig.bloom_enabled;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomFulltextConfig.bloom_indexed_properties;

public class BloomIT
{
    private static final String NODES = "CALL db.fulltext.bloomFulltextNodes([\"%s\"])";
    private static final String RELS = "CALL db.fulltext.bloomFulltextRelationships([\"%s\"])";
    private static final String ENTITYID = "entityid";

    @Rule
    public final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private GraphDatabaseService db;
    private GraphDatabaseBuilder builder;

    @Before
    public void before() throws Exception
    {
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( bloom_enabled, "true" );
    }

    @Test
    public void shouldPopulateAndQueryIndexes() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop, relprop" );
        db = builder.newGraphDatabase();
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

        Result result = db.execute( String.format( NODES, "integration" ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( RELS, "relate" ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldBeAbleToConfigureAnalyzer() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );
        builder.setConfig( BloomFulltextConfig.bloom_analyzer, "org.apache.lucene.analysis.sv.SwedishAnalyzer" );
        db = builder.newGraphDatabase();
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "Det finns en mening" );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "There is a sentance" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES, "is" ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES, "a" ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES, "det" ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES, "en" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldPopulateIndexWithExistingDataOnIndexCreate() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "something" );
        db = builder.newGraphDatabase();

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "prop", "Roskildevej 32" ); // Copenhagen Zoo is important to find.
            nodeId = node.getId();
            tx.success();
        }
        Result result = db.execute( String.format( NODES, "Roskildevej" ) );
        assertFalse( result.hasNext() );
        db.shutdown();

        builder.setConfig( bloom_indexed_properties, "prop" );
        builder.setConfig( BloomFulltextConfig.bloom_analyzer, "org.apache.lucene.analysis.da.DanishAnalyzer" );
        db = builder.newGraphDatabase();

        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();

        result = db.execute( String.format( NODES, "Roskildevej" ) );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void startupPopulationShouldNotCauseDuplicates() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );

        db = builder.newGraphDatabase();
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.setProperty( "prop", "Jyllingevej" );
            tx.success();
        }

        // Verify it's indexed exactly once
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();
        Result result = db.execute( String.format( NODES, "Jyllingevej" ) );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );

        db.shutdown();
        db = builder.newGraphDatabase();

        // Verify it's STILL indexed exactly once
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();
        result = db.execute( String.format( NODES, "Jyllingevej" ) );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void staleDataFromEntityDeleteShouldNotBeAccessibleAfterConfigurationChange() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );

        db = builder.newGraphDatabase();
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.setProperty( "prop", "Esplanaden" );
            tx.success();
        }

        db.shutdown();
        builder.setConfig( bloom_indexed_properties, "not-prop" );
        db = builder.newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            // This should no longer be indexed
            db.getNodeById( nodeId ).delete();
            tx.success();
        }

        db.shutdown();
        builder.setConfig( bloom_indexed_properties, "prop" );
        db = builder.newGraphDatabase();

        // Verify that the node is no longer indexed
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();
        Result result = db.execute( String.format( NODES, "Esplanaden" ) );
        assertFalse( result.hasNext() );
        result.close();
    }

    @Test
    public void staleDataFromPropertyRemovalShouldNotBeAccessibleAfterConfigurationChange() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );

        db = builder.newGraphDatabase();
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.setProperty( "prop", "Esplanaden" );
            tx.success();
        }

        db.shutdown();
        builder.setConfig( bloom_indexed_properties, "not-prop" );
        db = builder.newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            // This should no longer be indexed
            db.getNodeById( nodeId ).removeProperty( "prop" );
            tx.success();
        }

        db.shutdown();
        builder.setConfig( bloom_indexed_properties, "prop" );
        db = builder.newGraphDatabase();

        // Verify that the node is no longer indexed
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();
        Result result = db.execute( String.format( NODES, "Esplanaden" ) );
        assertFalse( result.hasNext() );
        result.close();
    }

    @Test
    public void updatesAreAvailableToConcurrentReadTransactions() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );
        db = builder.newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Langelinie Pavillinen" );
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES, "Langelinie" ) ) )
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

            try ( Result result = db.execute( String.format( NODES, "Langelinie" ) ) )
            {
                assertThat( Iterators.count( result ), is( 2L ) );
            }
        }
    }

    @Test
    public void shouldNotBeAbleToStartWithoutConfiguringProperties() throws Exception
    {
        expectedException.expect( new RootCauseMatcher<>( RuntimeException.class, "Properties to index must be configured for bloom fulltext" ) );
        db = builder.newGraphDatabase();
    }

    @Test
    public void shouldNotBeAbleToStartWithIllegalPropertyKey() throws Exception
    {
        expectedException.expect( InvalidSettingException.class );
        builder.setConfig( bloom_indexed_properties, "prop, " + FulltextProvider.FIELD_ENTITY_ID + ", hello" );
        db = builder.newGraphDatabase();
    }

    @Test
    public void shouldBeAbleToRunConsistencyCheck() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );
        db = builder.newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Langelinie Pavillinen" );
            tx.success();
        }
        db.shutdown();

        Config config = Config.defaults( bloom_indexed_properties, "prop" );
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService( new Date() );
        ConsistencyFlags consistencyFlags = new ConsistencyFlags( true, true, true, true );
        ConsistencyCheckService.Result result =
                consistencyCheckService.runFullConsistencyCheck( testDirectory.graphDbDir(), config, ProgressMonitorFactory.NONE, NullLogProvider.getInstance(),
                        true, consistencyFlags );
        assertTrue( result.isSuccessful() );
    }

    @Test
    public void shouldReindexNodesWhenAnalyzerIsChanged() throws Exception
    {
        String ENGLISH = EnglishAnalyzer.class.getCanonicalName();
        String SWEDISH = SwedishAnalyzer.class.getCanonicalName();

        builder.setConfig( bloom_indexed_properties, "prop" );
        builder.setConfig( BloomFulltextConfig.bloom_analyzer, ENGLISH );

        db = builder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Hello and hello again." );
            db.createNode().setProperty( "prop", "En tomte bodde i ett hus." );

            tx.success();
        }
        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES, "and" ) ) )
            {
                assertThat( Iterators.count( result ), is( 0L ) );
            }
            try ( Result result = db.execute( String.format( NODES, "ett" ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
        }

        db.shutdown();
        builder.setConfig( BloomFulltextConfig.bloom_analyzer, SWEDISH );
        db = builder.newGraphDatabase();
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();

        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES, "and" ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
            try ( Result result = db.execute( String.format( NODES, "ett" ) ) )
            {
                assertThat( Iterators.count( result ), is( 0L ) );
            }
        }
    }

    @Test
    public void shouldReindexAfterBeingTemporarilyDisabled() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );

        // Create a node while the index is enabled.
        db = builder.newGraphDatabase();
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
        db = builder.newGraphDatabase();
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();

        // Now we should be able to find the node that was added while the index was disabled.
        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES, "hello" ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
            try ( Result result = db.execute( String.format( NODES, "tomte" ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
        }
    }

    @After
    public void after() throws Exception
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }
}
