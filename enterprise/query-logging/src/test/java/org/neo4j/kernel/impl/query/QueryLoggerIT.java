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
package org.neo4j.kernel.impl.query;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.enterprise.auth.EmbeddedInteraction;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.virtual.VirtualValues;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.security.AuthSubject.AUTH_DISABLED;

public class QueryLoggerIT
{

    // It is imperitive that this test executes using a real filesystem; otherwise rotation failures will not be
    // detected on Windows.
    @Rule
    public final DefaultFileSystemRule fileSystem = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private GraphDatabaseBuilder databaseBuilder;
    private static final String QUERY = "CREATE (n:Foo {bar: 'baz'})";

    private File logsDirectory;
    private File logFilename;

    @Before
    public void setUp()
    {
        logsDirectory = new File( testDirectory.graphDbDir(), "logs" );
        logFilename = new File( logsDirectory, "query.log" );
        AssertableLogProvider inMemoryLog = new AssertableLogProvider();
        databaseBuilder = new TestEnterpriseGraphDatabaseFactory()
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem.get() ) )
                .setInternalLogProvider( inMemoryLog )
                .newImpermanentDatabaseBuilder( testDirectory.graphDbDir() );
    }

    @Test
    public void shouldLogCustomUserName() throws Throwable
    {
        // turn on query logging
        final Map<String, String> config = stringMap(
            GraphDatabaseSettings.logs_directory.name(), logsDirectory.getPath(),
            GraphDatabaseSettings.log_queries.name(), Settings.TRUE );
        EmbeddedInteraction db = new EmbeddedInteraction( databaseBuilder, config );

        // create users
        db.getLocalUserManager().newUser( "mats", "neo4j", false );
        db.getLocalUserManager().newUser( "andres", "neo4j", false );
        db.getLocalUserManager().addRoleToUser( "architect", "mats" );
        db.getLocalUserManager().addRoleToUser( "reader", "andres" );

        EnterpriseSecurityContext mats = db.login( "mats", "neo4j" );

        // run query
        db.executeQuery( mats, "UNWIND range(0, 10) AS i CREATE (:Foo {p: i})", Collections.emptyMap(), ResourceIterator::close );
        db.executeQuery( mats, "CREATE (:Label)", Collections.emptyMap(), ResourceIterator::close );

        // switch user, run query
        EnterpriseSecurityContext andres = db.login( "andres", "neo4j" );
        db.executeQuery( andres, "MATCH (n:Label) RETURN n", Collections.emptyMap(), ResourceIterator::close );

        db.tearDown();

        // THEN
        List<String> logLines = readAllLines( logFilename );

        assertThat( logLines, hasSize( 3 ) );
        assertThat( logLines.get( 0 ), containsString( "mats" ) );
        assertThat( logLines.get( 1 ), containsString( "mats" ) );
        assertThat( logLines.get( 2 ), containsString( "andres" ) );
    }

    @Test
    public void shouldLogTXMetaDataInQueryLog() throws Throwable
    {
        // turn on query logging
        databaseBuilder.setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() );
        databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE );
        EmbeddedInteraction db = new EmbeddedInteraction( databaseBuilder, Collections.emptyMap() );
        GraphDatabaseFacade graph = db.getLocalGraph();

        db.getLocalUserManager().setUserPassword( "neo4j", "123", false );

        EnterpriseSecurityContext subject = db.login( "neo4j", "123" );
        db.executeQuery( subject, "UNWIND range(0, 10) AS i CREATE (:Foo {p: i})", Collections.emptyMap(),
                ResourceIterator::close );

        // Set meta data and execute query in transaction
        try ( InternalTransaction tx = db.beginLocalTransactionAsUser( subject, KernelTransaction.Type.explicit ) )
        {
            graph.execute( "CALL dbms.setTXMetaData( { User: 'Johan' } )", Collections.emptyMap() );
            graph.execute( "CALL dbms.procedures() YIELD name RETURN name", Collections.emptyMap() ).close();
            graph.execute( "MATCH (n) RETURN n", Collections.emptyMap() ).close();
            graph.execute( QUERY, Collections.emptyMap() );
            tx.success();
        }

        // Ensure that old meta data is not retained
        try ( InternalTransaction tx = db.beginLocalTransactionAsUser( subject, KernelTransaction.Type.explicit ) )
        {
            graph.execute( "CALL dbms.setTXMetaData( { Location: 'Sweden' } )", Collections.emptyMap() );
            graph.execute( "MATCH ()-[r]-() RETURN count(r)", Collections.emptyMap() ).close();
            tx.success();
        }

        db.tearDown();

        // THEN
        List<String> logLines = readAllLines( logFilename );

        assertThat( logLines, hasSize( 7 ) );
        assertThat( logLines.get( 0 ), not( containsString( "User: 'Johan'" ) ) );
        // we don't care if setTXMetaData contains the meta data
        //assertThat( logLines.get( 1 ), containsString( "User: Johan" ) );
        assertThat( logLines.get( 2 ), containsString( "User: 'Johan'" ) );
        assertThat( logLines.get( 3 ), containsString( "User: 'Johan'" ) );
        assertThat( logLines.get( 4 ), containsString( "User: 'Johan'" ) );

        // we want to make sure that the new transaction does not carry old meta data
        assertThat( logLines.get( 5 ), not( containsString( "User: 'Johan'" ) ) );
        assertThat( logLines.get( 6 ), containsString( "Location: 'Sweden'" ) );
    }

    @Test
    public void shouldLogQuerySlowerThanThreshold() throws Exception
    {
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, Settings.FALSE )
                .newGraphDatabase();

        executeQueryAndShutdown( database );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format( " ms: %s - %s - {}", clientConnectionInfo(), QUERY ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    public void shouldLogParametersWhenNestedMap() throws Exception
    {
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_parameter_logging_enabled, Settings.TRUE )
                .newGraphDatabase();

        Map<String,Object> props = new LinkedHashMap<>(); // to be sure about ordering in the last assertion
        props.put( "name", "Roland" );
        props.put( "position", "Gunslinger" );
        props.put( "followers", Arrays.asList( "Jake", "Eddie", "Susannah" ) );

        Map<String,Object> params = new HashMap<>();
        params.put( "props", props );

        String query = "CREATE ({props})";
        executeQueryAndShutdown( database, query, params );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), endsWith( String.format(
                " ms: %s - %s - {props: {name: 'Roland', position: 'Gunslinger', followers: ['Jake', 'Eddie', 'Susannah']}}"
                        + " - {}",
                clientConnectionInfo(),
                query ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    private String clientConnectionInfo()
    {
        return ClientConnectionInfo.EMBEDDED_CONNECTION.withUsername( AUTH_DISABLED.username() ).asConnectionDetails();
    }

    @Test
    public void shouldLogParametersWhenList() throws Exception
    {
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .newGraphDatabase();

        Map<String,Object> params = new HashMap<>();
        params.put( "ids", Arrays.asList( 0, 1, 2 ) );
        String query = "MATCH (n) WHERE id(n) in {ids} RETURN n.name";
        executeQueryAndShutdown( database, query, params );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ),
                endsWith( String.format( " ms: %s - %s - {ids: [0, 1, 2]} - {}", clientConnectionInfo(), query ) ) );
        assertThat( logLines.get( 0 ), containsString( AUTH_DISABLED.username() ) );
    }

    @Test
    public void disabledQueryLogging() throws IOException
    {
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.log_queries_filename, logFilename.getPath() )
                .newGraphDatabase();

        executeQueryAndShutdown( database );

        List<String> strings = readAllLines( logFilename );
        assertEquals( 0, strings.size() );
    }

    @Test
    public void disabledQueryLogRotation() throws Exception
    {
        final File logsDirectory = new File( testDirectory.graphDbDir(), "logs" );
        final File logFilename = new File( logsDirectory, "query.log" );
        final File shiftedLogFilename1 = new File( logsDirectory, "query.log.1" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_rotation_threshold, "0" )
                .newGraphDatabase();

        // Logging is done asynchronously, so write many times to make sure we would have rotated something
        for ( int i = 0; i < 100; i++ )
        {
            database.execute( QUERY );
        }

        database.shutdown();

        assertFalse( "There should not exist a shifted log file because rotation is disabled",
                shiftedLogFilename1.exists() );

        List<String> lines = readAllLines( logFilename );
        assertEquals( 100, lines.size() );
    }

    @Test
    public void queryLogRotation() throws Exception
    {
        final File logsDirectory = new File( testDirectory.graphDbDir(), "logs" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_max_archives, "100" )
                .setConfig( GraphDatabaseSettings.log_queries_rotation_threshold, "1" )
                .newGraphDatabase();

        // Logging is done asynchronously, and it turns out it's really hard to make it all work the same on Linux
        // and on Windows, so just write many times to make sure we rotate several times.

        for ( int i = 0; i < 100; i++ )
        {
            database.execute( QUERY );
        }

        database.shutdown();

        File[] queryLogs = fileSystem.get().listFiles( logsDirectory, ( dir, name ) -> name.startsWith( "query.log" ) );

        assertThat( "Expect to have more then one query log file.", queryLogs.length, greaterThanOrEqualTo( 2 ) );

        List<String> loggedQueries = Arrays.stream( queryLogs )
                                        .map( this::readAllLinesSilent )
                                        .flatMap( Collection::stream )
                                        .collect( Collectors.toList() );
        assertThat( "Expected log file to have at least one log entry", loggedQueries, hasSize( 100 ) );
    }

    @Test
    public void shouldNotLogPassword() throws Exception
    {
        GraphDatabaseFacade database = (GraphDatabaseFacade) databaseBuilder
                .setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.logs_directory, logsDirectory.getPath() )
                .setConfig( GraphDatabaseSettings.auth_enabled, Settings.TRUE )
                .newGraphDatabase();

        EnterpriseAuthManager authManager = database.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
        EnterpriseSecurityContext neo = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );

        String query = "CALL dbms.security.changePassword('abc123')";
        try ( InternalTransaction tx = database
                .beginTransaction( KernelTransaction.Type.explicit, neo ) )
        {
            Result res = database.execute( tx, query, VirtualValues.EMPTY_MAP );
            res.close();
            tx.success();
        }
        finally
        {
            database.shutdown();
        }

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ),
                containsString(  "CALL dbms.security.changePassword(******)") ) ;
        assertThat( logLines.get( 0 ),not( containsString( "abc123" ) ) );
        assertThat( logLines.get( 0 ), containsString( neo.subject().username() ) );
    }

    @Test
    public void canBeEnabledAndDisabledAtRuntime() throws Exception
    {
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.log_queries_filename, logFilename.getPath() )
                .newGraphDatabase();

        database.execute( QUERY ).close();

        List<String> strings = readAllLines( logFilename );
        assertEquals( 0, strings.size() );

        database.execute( "CALL dbms.setConfigValue('" + GraphDatabaseSettings.log_queries.name() + "', 'true')" ).close();
        database.execute( QUERY ).close();

        // Both config change and query should exist
        strings = readAllLines( logFilename );
        assertEquals( 2, strings.size() );

        database.execute( "CALL dbms.setConfigValue('" + GraphDatabaseSettings.log_queries.name() + "', 'false')" ).close();
        database.execute( QUERY ).close();

        // Value should not change when disabled
        strings = readAllLines( logFilename );
        assertEquals( 2, strings.size() );
    }

    private void executeQueryAndShutdown( GraphDatabaseService database )
    {
        executeQueryAndShutdown( database, QUERY, Collections.emptyMap() );
    }

    private void executeQueryAndShutdown( GraphDatabaseService database, String query, Map<String,Object> params )
    {
        Result execute = database.execute( query, params );
        execute.close();
        database.shutdown();
    }

    private List<String> readAllLinesSilent( File logFilename )
    {
        try
        {
            return readAllLines( fileSystem.get(), logFilename );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private List<String> readAllLines( File logFilename ) throws IOException
    {
        return readAllLines( fileSystem.get(), logFilename );
    }

    public static List<String> readAllLines( FileSystemAbstraction fs, File logFilename ) throws IOException
    {
        List<String> logLines = new ArrayList<>();
        // this is needed as the EphemeralFSA is broken, and creates a new file when reading a non-existent file from
        // a valid directory
        if ( !fs.fileExists( logFilename ) )
        {
            throw new FileNotFoundException( "File does not exist." );
        }

        try ( BufferedReader reader = new BufferedReader(
                fs.openAsReader( logFilename, StandardCharsets.UTF_8 ) ) )
        {
            for ( String line; ( line = reader.readLine() ) != null; )
            {
                logLines.add( line );
            }
        }
        return logLines;
    }
}
