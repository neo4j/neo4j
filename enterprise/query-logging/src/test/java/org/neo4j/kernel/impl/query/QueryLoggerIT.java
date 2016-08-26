/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthSubject;
import org.neo4j.server.security.enterprise.auth.NeoShallowEmbeddedInteraction;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.api.security.AccessMode.Static.FULL;
import static org.neo4j.kernel.impl.query.QueryEngineProvider.embeddedSession;

public class QueryLoggerIT
{

    @Rule
    public final EphemeralFileSystemRule fileSystem = new EphemeralFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private GraphDatabaseBuilder databaseBuilder;
    private static final String QUERY = "CREATE (n:Foo {bar: 'baz'})";

    @Before
    public void setUp()
    {
        AssertableLogProvider inMemoryLog = new AssertableLogProvider();
        databaseBuilder = new TestEnterpriseGraphDatabaseFactory().setFileSystem( fileSystem.get() )
                .setInternalLogProvider( inMemoryLog )
                .newImpermanentDatabaseBuilder( testDirectory.graphDbDir() );
    }

    @Test
    public void shouldLogCustomUserName() throws Throwable
    {
        final File logsDirectory = new File( testDirectory.graphDbDir(), "logs" );
        final File logFilename = new File( logsDirectory, "query.log" );
        // turn on query logging
        databaseBuilder.setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() );
        databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE );
        NeoShallowEmbeddedInteraction db = new NeoShallowEmbeddedInteraction( databaseBuilder );

        // create users
        db.getManager().newUser( "mats", "neo4j", false );
        db.getManager().newUser( "andres", "neo4j", false );
        db.getManager().addRoleToUser( "architect", "mats" );
        db.getManager().addRoleToUser( "reader", "andres" );

        EnterpriseAuthSubject mats = db.login( "mats", "neo4j" );

        // run query
        db.executeQuery( mats, "UNWIND range(0, 10) AS i CREATE (:Foo {p: i})", Collections.emptyMap(), ResourceIterator::close );
        db.executeQuery( mats, "CREATE (:Label)", Collections.emptyMap(), ResourceIterator::close );

        // switch user, run query
        EnterpriseAuthSubject andres = db.login( "andres", "neo4j" );
        db.executeQuery( andres, "MATCH (n:Label) RETURN n", Collections.emptyMap(), ResourceIterator::close );

        // THEN
        List<String> logLines = readAllLines( logFilename );

        assertThat( logLines, hasSize( 3 ) );
        assertThat( logLines.get( 0 ), containsString( "mats" ) );
        assertThat( logLines.get( 1 ), containsString( "mats" ) );
        assertThat( logLines.get( 2 ), containsString( "andres" ) );
    }

    @Test
    public void shouldLogQuerySlowerThanThreshold() throws Exception
    {
        final File logsDirectory = new File( testDirectory.graphDbDir(), "logs" );
        final File logFilename = new File( logsDirectory, "query.log" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, Settings.FALSE )
                .newGraphDatabase();

        executeQueryAndShutdown( database );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format( " ms: %s - %s",
                embeddedSession( new FakeTransactionalContext( FULL ) ), QUERY ) ) );
        assertThat( logLines.get( 0 ), containsString( FULL.name() ) );
    }

    @Test
    public void shouldLogParametersWhenNestedMap() throws Exception
    {
        final File logsDirectory = new File( testDirectory.graphDbDir(), "logs" );
        final File logFilename = new File( logsDirectory, "query.log" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, Settings.TRUE )
                .newGraphDatabase();

        Map<String,Object> props = new LinkedHashMap<>(); // to be sure about ordering in the last assertion
        props.put( "name", "Roland" );
        props.put( "position", "Gunslinger" );
        props.put( "followers", Arrays.asList("Jake", "Eddie", "Susannah") );

        Map<String, Object> params = new HashMap<>();
        params.put( "props", props );

        String query = "CREATE ({props})";
        executeQueryAndShutdown( database, query, params );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format(
                " ms: %s - %s - {props: {name: Roland, position: Gunslinger, followers: [Jake, Eddie, Susannah]}}",
                embeddedSession( new FakeTransactionalContext( FULL ) ), query) ) );
        assertThat( logLines.get( 0 ), containsString( FULL.name() ) );
    }

    @Test
    public void shouldLogParametersWhenList() throws Exception
    {
        final File logsDirectory = new File( testDirectory.graphDbDir(), "logs" );
        final File logFilename = new File( logsDirectory, "query.log" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .newGraphDatabase();

        Map<String, Object> params = new HashMap<>();
        params.put( "ids", Arrays.asList( 0, 1, 2 ) );
        String query = "MATCH (n) WHERE id(n) in {ids} RETURN n.name";
        executeQueryAndShutdown( database, query, params );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ),
                endsWith( String.format(
                        " ms: %s - %s - {ids: [0, 1, 2]}",
                        embeddedSession( new FakeTransactionalContext( FULL ) ), query) ) );
        assertThat( logLines.get( 0 ), containsString( FULL.name() ) );
    }

    @Test
    public void disabledQueryLogging() throws IOException
    {
        final File logFilename = new File( testDirectory.graphDbDir(), "query.log" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.log_queries_filename, logFilename.getPath() )
                .newGraphDatabase();

        executeQueryAndShutdown( database );

        List<String> lines = readAllLines( logFilename );
        assertTrue( "Should not have any queries logged since query log is disabled", lines.isEmpty() );
    }

    @Test
    public void queryLogRotation() throws Exception
    {
        final File logsDirectory = new File( testDirectory.graphDbDir(), "logs" );
        final File logFilename = new File( logsDirectory, "query.log" );
        final File shiftedLogFilename = new File( logsDirectory, "query.log.1" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_rotation_threshold, "1" )
                .newGraphDatabase();

        database.execute( QUERY );
        database.execute( QUERY );
        // wait for file rotation to finish
        Thread.sleep( TimeUnit.SECONDS.toMillis( 1 ) );
        // execute one more slow query which should go to new log file
        database.execute( QUERY );
        database.shutdown();

        List<String> lines = readAllLines( logFilename );
        assertEquals( 1, lines.size() );
        List<String> shiftedLines = readAllLines( shiftedLogFilename );
        assertEquals( 2, shiftedLines.size() );
    }

    private void executeQueryAndShutdown( GraphDatabaseService database )
    {
       executeQueryAndShutdown( database, QUERY, Collections.emptyMap() );
    }

    private void executeQueryAndShutdown( GraphDatabaseService database, String query, Map<String, Object> params )
    {
        Result execute = database.execute( query, params );
        execute.close();
        database.shutdown();
    }

    private List<String> readAllLines( File logFilename ) throws IOException
    {
        List<String> logLines = new ArrayList<>();
        try ( BufferedReader reader = new BufferedReader(
                fileSystem.get().openAsReader( logFilename, StandardCharsets.UTF_8 ) ) )
        {
            for ( String line; (line = reader.readLine()) != null; )
            {
                logLines.add( line );
            }
        }
        return logLines;
    }
}
