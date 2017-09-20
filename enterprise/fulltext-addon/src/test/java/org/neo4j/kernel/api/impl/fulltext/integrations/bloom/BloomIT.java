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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.impl.fulltext.FulltextProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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

    private TestGraphDatabaseFactory factory;
    private GraphDatabaseService db;

    @Before
    public void before() throws Exception
    {
        createTestGraphDatabaseFactory();
        configureBloomExtension();
    }

    private void createTestGraphDatabaseFactory()
    {
        factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
    }

    private void configureBloomExtension()
    {
        factory.addKernelExtensions( Collections.singletonList( new BloomKernelExtensionFactory() ) );
    }

    @Test
    public void shouldPopulateAndQueryIndexes() throws Exception
    {
        db = factory.newImpermanentDatabase( testDirectory.graphDbDir(),
                Collections.singletonMap( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop, relprop" ) );
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
        Map<Setting<?>,String> config = new HashMap<>();
        config.put( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop" );
        config.put( LoadableBloomFulltextConfig.bloom_analyzer, "org.apache.lucene.analysis.sv.SwedishAnalyzer" );
        db = factory.newImpermanentDatabase( testDirectory.graphDbDir(), config );
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
        createTestGraphDatabaseFactory();
        db = factory.newEmbeddedDatabase( testDirectory.graphDbDir() );
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "prop", "Roskildevej 32" ); // Copenhagen Zoo is important to find.
            nodeId = node.getId();
            tx.success();
        }
        db.shutdown();

        configureBloomExtension();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop" );
        builder.setConfig( LoadableBloomFulltextConfig.bloom_analyzer, "org.apache.lucene.analysis.da.DanishAnalyzer" );
        db = builder.newGraphDatabase();

        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();

        Result result = db.execute( String.format( NODES, "Roskildevej" ) );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void startupPopulationShouldNotCauseDuplicates() throws Exception
    {
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop" );

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
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop" );

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
        builder.setConfig( LoadableBloomFulltextConfig.bloom_indexed_properties, "not-prop" );
        db = builder.newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            // This should no longer be indexed
            db.getNodeById( nodeId ).delete();
            tx.success();
        }

        db.shutdown();
        builder.setConfig( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop" );
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
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop" );

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
        builder.setConfig( LoadableBloomFulltextConfig.bloom_indexed_properties, "not-prop" );
        db = builder.newGraphDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            // This should no longer be indexed
            db.getNodeById( nodeId ).removeProperty( "prop" );
            tx.success();
        }

        db.shutdown();
        builder.setConfig( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop" );
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
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop" );
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
        Map<Setting<?>,String> config = new HashMap<>();
        expectedException.expect( new RootCauseMatcher<>( InvalidSettingException.class, "Bad value" ) );
        db = factory.newImpermanentDatabase( testDirectory.graphDbDir(), config );
    }

    @Test
    public void shouldNotBeAbleToStartWithIllegalPropertyKey() throws Exception
    {
        Map<Setting<?>,String> config = new HashMap<>();
        config.put( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop, " + FulltextProvider.LUCENE_FULLTEXT_ADDON_INTERNAL_ID + ", hello" );
        expectedException.expect( InvalidSettingException.class );
        db = factory.newImpermanentDatabase( testDirectory.graphDbDir(), config );
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
