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

import org.apache.commons.lang3.SystemUtils;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
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
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.impl.fulltext.FulltextProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.mockito.matcher.RootCauseMatcher;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomFulltextConfig.bloom_enabled;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomFulltextConfig.bloom_indexed_properties;

public class BloomIT
{
    private static final String NODES = "CALL db.fulltext.bloomFulltextNodes([\"%s\"], %b, %b)";
    private static final String RELS = "CALL db.fulltext.bloomFulltextRelationships([\"%s\"], %b, %b)";
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
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() );
        builder.setConfig( bloom_enabled, "true" );
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
        builder.setConfig( bloom_indexed_properties, "prop, relprop" );
        db = getDb();
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

        Result result = db.execute( String.format( NODES, "integration", true, false ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( RELS, "relate", true, false ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void exactQueryShouldBeExact() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );
        db = getDb();
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "This is a integration test." );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "This is a related integration test" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES, "integration", false, false ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES, "integratiun", false, false ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void matchAllQueryShouldMatchAll() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );
        db = getDb();
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "This is a integration test." );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "This is a related integration test" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES, "integration, related", false, true ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldBeAbleToConfigureAnalyzer() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );
        builder.setConfig( BloomFulltextConfig.bloom_analyzer, "org.apache.lucene.analysis.sv.SwedishAnalyzer" );
        db = getDb();
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "Det finns en mening" );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "There is a sentance" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES, "is", true, false ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES, "a", true, false ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES, "det", true, false ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( NODES, "en", true, false ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void shouldPopulateIndexWithExistingDataOnIndexCreate() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "something" );
        db = getDb();

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "prop", "Roskildevej 32" ); // Copenhagen Zoo is important to find.
            nodeId = node.getId();
            tx.success();
        }
        Result result = db.execute( String.format( NODES, "Roskildevej", true, false ) );
        assertFalse( result.hasNext() );
        db.shutdown();

        builder.setConfig( bloom_indexed_properties, "prop" );
        builder.setConfig( BloomFulltextConfig.bloom_analyzer, "org.apache.lucene.analysis.da.DanishAnalyzer" );
        db = getDb();

        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();

        result = db.execute( String.format( NODES, "Roskildevej", true, false ) );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void startupPopulationShouldNotCauseDuplicates() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );

        db = getDb();
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
        Result result = db.execute( String.format( NODES, "Jyllingevej", true, false ) );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );

        db.shutdown();
        db = getDb();

        // Verify it's STILL indexed exactly once
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();
        result = db.execute( String.format( NODES, "Jyllingevej", true, false ) );
        assertTrue( result.hasNext() );
        assertEquals( nodeId, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void staleDataFromEntityDeleteShouldNotBeAccessibleAfterConfigurationChange() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );

        db = getDb();
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
        db = getDb();

        try ( Transaction tx = db.beginTx() )
        {
            // This should no longer be indexed
            db.getNodeById( nodeId ).delete();
            tx.success();
        }

        db.shutdown();
        builder.setConfig( bloom_indexed_properties, "prop" );
        db = getDb();

        // Verify that the node is no longer indexed
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();
        Result result = db.execute( String.format( NODES, "Esplanaden", true, false ) );
        assertFalse( result.hasNext() );
        result.close();
    }

    @Test
    public void staleDataFromPropertyRemovalShouldNotBeAccessibleAfterConfigurationChange() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );

        db = getDb();
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
        db = getDb();

        try ( Transaction tx = db.beginTx() )
        {
            // This should no longer be indexed
            db.getNodeById( nodeId ).removeProperty( "prop" );
            tx.success();
        }

        db.shutdown();
        builder.setConfig( bloom_indexed_properties, "prop" );
        db = getDb();

        // Verify that the node is no longer indexed
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();
        Result result = db.execute( String.format( NODES, "Esplanaden", true, false ) );
        assertFalse( result.hasNext() );
        result.close();
    }

    @Test
    public void updatesAreAvailableToConcurrentReadTransactions() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );
        db = getDb();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Langelinie Pavillinen" );
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES, "Langelinie", true, false ) ) )
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

            try ( Result result = db.execute( String.format( NODES, "Langelinie", true, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 2L ) );
            }
        }
    }

    @Test
    public void shouldNotBeAbleToStartWithoutConfiguringProperties() throws Exception
    {
        expectedException.expect( new RootCauseMatcher<>( RuntimeException.class, "Properties to index must be configured for bloom fulltext" ) );
        db = getDb();
    }

    @Test
    public void shouldNotBeAbleToStartWithIllegalPropertyKey() throws Exception
    {
        expectedException.expect( InvalidSettingException.class );
        builder.setConfig( bloom_indexed_properties, "prop, " + FulltextProvider.FIELD_ENTITY_ID + ", hello" );
        db = getDb();
    }

    @Test
    public void shouldBeAbleToRunConsistencyCheck() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop" );
        db = getDb();

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Langelinie Pavillinen" );
            tx.success();
        }
        db.shutdown();

        Config config = Config.defaults( bloom_indexed_properties, "prop" );
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
        String ENGLISH = EnglishAnalyzer.class.getCanonicalName();
        String SWEDISH = SwedishAnalyzer.class.getCanonicalName();

        builder.setConfig( bloom_indexed_properties, "prop" );
        builder.setConfig( BloomFulltextConfig.bloom_analyzer, ENGLISH );

        db = getDb();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "Hello and hello again." );
            db.createNode().setProperty( "prop", "En tomte bodde i ett hus." );

            tx.success();
        }
        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES, "and", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 0L ) );
            }
            try ( Result result = db.execute( String.format( NODES, "ett", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
        }

        db.shutdown();
        builder.setConfig( BloomFulltextConfig.bloom_analyzer, SWEDISH );
        db = getDb();
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();

        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES, "and", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
            try ( Result result = db.execute( String.format( NODES, "ett", false, false ) ) )
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
        db = getDb();
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
        db.execute( "CALL db.fulltext.bloomAwaitPopulation" ).close();

        // Now we should be able to find the node that was added while the index was disabled.
        try ( Transaction ignore = db.beginTx() )
        {
            try ( Result result = db.execute( String.format( NODES, "hello", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
            try ( Result result = db.execute( String.format( NODES, "tomte", false, false ) ) )
            {
                assertThat( Iterators.count( result ), is( 1L ) );
            }
        }
    }

    @Test
    public void shouldBeAbleToQueryForIndexedProperties() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop, otherprop, proppmatt" );

        db = getDb();

        Result result = db.execute( "CALL db.fulltext.bloomFulltextProperties" );
        assertEquals( "otherprop", result.next().get( "propertyKey" ) );
        assertEquals( "prop", result.next().get( "propertyKey" ) );
        assertEquals( "proppmatt", result.next().get( "propertyKey" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void onlineIndexShouldBeReportedAsOnline() throws Exception
    {
        builder.setConfig( bloom_indexed_properties, "prop, otherprop, proppmatt" );

        db = getDb();

        db.execute( "CALL db.fulltext.bloomAwaitPopulation" );
        Result result = db.execute( "CALL db.fulltext.bloomFulltextStatus" );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertEquals( "ONLINE", result.next().get( "state" ) );
        assertFalse( result.hasNext() );
    }

    @Test
    public void failureToStartUpMustNotPreventShutDown() throws Exception
    {
        // Ignore this test on Windows because the test relies on file permissions to trigger failure modes in
        // the code. Unfortunately, file permissions are an incredible pain to work with on Windows.
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        builder.setConfig( BloomFulltextConfig.bloom_indexed_properties, "prop" );

        // Create the store directory and all its files, and add a bit of data to it
        GraphDatabaseService db = builder.newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "prop", "bla bla bla" );
            tx.success();
        }
        db.shutdown();

        File dir = testDirectory.graphDbDir();
        assertTrue( dir.setReadable( false ) );
        try
        {
            // Making the directory not readable ought to cause problems for the database as it tries to start up
            builder.newGraphDatabase().shutdown();
            fail( "Should not have started up and shut down cleanly on an unreadable store directory" );
        }
        catch ( Exception e )
        {
            // Good
        }
        catch ( Throwable th )
        {
            makeReadable( dir, th );
            throw th;
        }
        makeReadable( dir, null );
    }

    private void makeReadable( File dir, Throwable th )
    {
        if ( !dir.setReadable( true ) )
        {
            AssertionError error = new AssertionError( "Failed to make " + dir + " writable again!" );
            if ( th != null )
            {
                th.addSuppressed( error );
            }
            else
            {
                throw error;
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
