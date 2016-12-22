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
package org.neo4j;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.rmi.RemoteException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.internal.Ports;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.GuardTimeoutException;
import org.neo4j.kernel.guard.TimeoutGuard;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.LifecycleManagingDatabase;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.web.HttpHeaderUtils;
import org.neo4j.shell.InterruptSignalHandler;
import org.neo4j.shell.Response;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionNotFound;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class TransactionGuardIntegrationTest
{
    @ClassRule
    public static CleanupRule cleanupRule = new CleanupRule();
    @ClassRule
    public static TestDirectory testDirectory = TestDirectory.testDirectory();

    private static final FakeClock fakeClock = Clocks.fakeClock();
    private TickingGuard tickingGuard = new TickingGuard( fakeClock, NullLog.getInstance(), 1, TimeUnit.SECONDS );
    private static GraphDatabaseAPI databaseWithTimeout;
    private static GraphDatabaseAPI databaseWithTimeoutAndGuard;
    private static GraphDatabaseAPI databaseWithoutTimeout;
    private static CommunityNeoServer neoServer;
    private static int boltPortCustomGuard;
    private static int boltPortDatabaseWithTimeout;

    @Test
    public void terminateLongRunningTransaction()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            transaction.success();
            database.createNode();
            fail( "Transaction should be already terminated." );
        }
        catch ( GuardTimeoutException e )
        {
            assertThat( e.getMessage(), startsWith( "Transaction timeout." ) );
            assertEquals( e.status(), Status.Transaction.TransactionTimedOut );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningTransactionWithPeriodicCommit() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeoutCustomGuard();
        try
        {
            URL url = prepareTestImportFile( 8 );
            database.execute( "USING PERIODIC COMMIT 5 LOAD CSV FROM '" + url + "' AS line CREATE ();" );
            fail( "Transaction should be already terminated." );
        }
        catch ( GuardTimeoutException ignored )
        {
        }
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateTransactionWithCustomTimeoutWithoutConfiguredDefault()
    {
        GraphDatabaseAPI database = startDatabaseWithoutTimeout();
        try ( Transaction transaction = database.beginTx( 27, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 26, TimeUnit.SECONDS );
            database.createNode();
            transaction.failure();
        }

        try ( Transaction transaction = database.beginTx( 27, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 28, TimeUnit.SECONDS );
            database.createNode();
            fail( "Transaction should be already terminated." );
        }
        catch ( GuardTimeoutException e )
        {
            assertThat( e.getMessage(), startsWith( "Transaction timeout." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningQueryTransaction()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            transaction.success();
            database.execute( "create (n)" );
            fail( "Transaction should be already terminated." );
        }
        catch ( GuardTimeoutException e )
        {
            assertThat( e.getMessage(), startsWith( "Transaction timeout." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningQueryWithCustomTimeoutWithoutConfiguredDefault()
    {
        GraphDatabaseAPI database = startDatabaseWithoutTimeout();
        try ( Transaction transaction = database.beginTx( 5, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 4, TimeUnit.SECONDS );
            database.execute( "create (n)" );
            transaction.failure();
        }

        try ( Transaction transaction = database.beginTx( 6, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 7, TimeUnit.SECONDS );
            transaction.success();
            database.execute( "create (n)" );
            fail( "Transaction should be already terminated." );
        }
        catch ( GuardTimeoutException e )
        {
            assertThat( e.getMessage(), startsWith( "Transaction timeout." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningShellQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        GraphDatabaseShellServer shellServer = getGraphDatabaseShellServer( database );
        try
        {
            SameJvmClient client = getShellClient( shellServer );
            CollectingOutput commandOutput = new CollectingOutput();
            execute( shellServer, commandOutput, client.getId(), "begin Transaction" );
            fakeClock.forward( 3, TimeUnit.SECONDS );
            execute( shellServer, commandOutput, client.getId(), "create (n);" );
            execute( shellServer, commandOutput, client.getId(), "commit" );
            fail( "Transaction should be already terminated." );
        }
        catch ( ShellException e )
        {
            assertThat( e.getMessage(), containsString( "Transaction timeout." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningShellPeriodicCommitQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeoutCustomGuard();
        GraphDatabaseShellServer shellServer = getGraphDatabaseShellServer( database );
        try
        {
            SameJvmClient client = getShellClient( shellServer );
            CollectingOutput commandOutput = new CollectingOutput();
            URL url = prepareTestImportFile( 8 );
            execute( shellServer, commandOutput, client.getId(),
                    "USING PERIODIC COMMIT 5 LOAD CSV FROM '" + url + "' AS line CREATE ();" );
            fail( "Transaction should be already terminated." );
        }
        catch ( ShellException e )
        {
            assertThat( e.getMessage(), containsString( "Transaction timeout." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningRestTransactionalEndpointQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        CommunityNeoServer neoServer = startNeoServer( (GraphDatabaseFacade) database );
        String transactionEndPoint = HTTP.POST( transactionUri( neoServer ) ).location();

        fakeClock.forward( 3, TimeUnit.SECONDS );

        HTTP.Response response =
                HTTP.POST( transactionEndPoint, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( "Response should be successful.", 200, response.status() );

        HTTP.Response commitResponse = HTTP.POST( transactionEndPoint + "/commit" );
        assertEquals( "Transaction should be already closed and not found.", 404, commitResponse.status() );

        assertEquals( "Transaction should be forcefully closed.", TransactionNotFound.code().serialize(),
                commitResponse.get( "errors" ).findValue( "code" ).asText() );
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningRestTransactionalEndpointWithCustomTimeoutQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        CommunityNeoServer neoServer = startNeoServer( (GraphDatabaseFacade) database );
        long customTimeout = TimeUnit.SECONDS.toMillis( 10 );
        HTTP.Response beginResponse = HTTP
                .withHeaders( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER, String.valueOf( customTimeout ) )
                .POST( transactionUri( neoServer ),
                        quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( "Response should be successful.", 201, beginResponse.status() );

        String transactionEndPoint = beginResponse.location();
        fakeClock.forward( 3, TimeUnit.SECONDS );

        HTTP.Response response =
                HTTP.POST( transactionEndPoint, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( "Response should be successful.", 200, response.status() );

        fakeClock.forward( 11, TimeUnit.SECONDS );

        response =
                HTTP.POST( transactionEndPoint, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( "Response should be successful.", 200, response.status() );

        HTTP.Response commitResponse = HTTP.POST( transactionEndPoint + "/commit" );
        assertEquals( "Transaction should be already closed and not found.", 404, commitResponse.status() );

        assertEquals( "Transaction should be forcefully closed.", TransactionNotFound.code().serialize(),
                commitResponse.get( "errors" ).findValue( "code" ).asText() );
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningDriverQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        CommunityNeoServer neoServer = startNeoServer( (GraphDatabaseFacade) database );

        org.neo4j.driver.v1.Config driverConfig = getDriverConfig();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:" + boltPortDatabaseWithTimeout, driverConfig );
                Session session = driver.session() )
        {
            org.neo4j.driver.v1.Transaction transaction = session.beginTransaction();
            transaction.run( "create (n)" ).consume();
            transaction.success();
            fakeClock.forward( 3, TimeUnit.SECONDS );
            try
            {
                transaction.run( "create (n)" ).consume();
                fail( "Transaction should be already terminated by execution guard." );
            }
            catch ( Exception expected )
            {
                // ignored
            }
        }
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningDriverPeriodicCommitQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeoutCustomGuard();
        CommunityNeoServer neoServer = startNeoServer( (GraphDatabaseFacade) database );

        org.neo4j.driver.v1.Config driverConfig = getDriverConfig();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:" + boltPortCustomGuard, driverConfig );
                Session session = driver.session() )
        {
            URL url = prepareTestImportFile( 8 );
            session.run( "USING PERIODIC COMMIT 5 LOAD CSV FROM '" + url + "' AS line CREATE ();" ).consume();
            fail( "Transaction should be already terminated by execution guard." );
        }
        catch ( Exception expected )
        {
            //
        }
        assertDatabaseDoesNotHaveNodes( database );
    }

    private GraphDatabaseAPI startDatabaseWithTimeoutCustomGuard()
    {
        if ( databaseWithTimeoutAndGuard == null )
        {
            boltPortCustomGuard = findFreePort();
            Map<Setting<?>,String> configMap = getSettingsWithTimeoutAndBolt( boltPortCustomGuard );
            databaseWithTimeoutAndGuard =
                    startCustomGuardedDatabase( testDirectory.directory( "dbWithoutTimeoutAndGuard" ), configMap );
        }
        return databaseWithTimeoutAndGuard;
    }

    private GraphDatabaseAPI startDatabaseWithTimeout()
    {
        if ( databaseWithTimeout == null )
        {
            boltPortDatabaseWithTimeout = findFreePort();
            Map<Setting<?>,String> configMap = getSettingsWithTimeoutAndBolt( boltPortDatabaseWithTimeout );
            databaseWithTimeout = startCustomDatabase( testDirectory.directory( "dbWithTimeout" ), configMap );
        }
        return databaseWithTimeout;
    }

    private GraphDatabaseAPI startDatabaseWithoutTimeout()
    {
        if ( databaseWithoutTimeout == null )
        {
            Map<Setting<?>,String> configMap = getSettingsWithoutTransactionTimeout();
            databaseWithoutTimeout = startCustomDatabase( testDirectory.directory( "dbWithoutTimeout" ),
                    configMap );
        }
        return databaseWithoutTimeout;
    }

    private org.neo4j.driver.v1.Config getDriverConfig()
    {
        return org.neo4j.driver.v1.Config.build()
                .withEncryptionLevel( org.neo4j.driver.v1.Config.EncryptionLevel.NONE )
                .toConfig();
    }

    private CommunityNeoServer startNeoServer( GraphDatabaseFacade database ) throws IOException
    {
        if ( neoServer == null )
        {
            GuardingServerBuilder serverBuilder = new GuardingServerBuilder( database );
            BoltConnector boltConnector = new BoltConnector( "bolt" );
            serverBuilder.withProperty( boltConnector.type.name(), "BOLT" )
                    .withProperty( boltConnector.enabled.name(), "true" )
                    .withProperty( boltConnector.encryption_level.name(),
                            BoltConnector.EncryptionLevel.DISABLED.name() )
                    .withProperty( GraphDatabaseSettings.auth_enabled.name(), "false" );
            neoServer = serverBuilder.build();
            cleanupRule.add( neoServer );
            neoServer.start();
        }
        return neoServer;
    }

    private Map<Setting<?>,String> getSettingsWithTimeoutAndBolt( int boltPort )
    {
        BoltConnector boltConnector = new BoltConnector( "bolt" );
        return MapUtil.genericMap(
                GraphDatabaseSettings.transaction_timeout, "2s",
                boltConnector.address, "localhost:" + boltPort,
                boltConnector.type, "BOLT",
                boltConnector.enabled, "true",
                boltConnector.encryption_level, BoltConnector.EncryptionLevel.DISABLED.name(),
                GraphDatabaseSettings.auth_enabled, "false" );
    }

    private Map<Setting<?>,String> getSettingsWithoutTransactionTimeout()
    {
        return MapUtil.genericMap();
    }

    private String transactionUri( CommunityNeoServer neoServer )
    {
        return neoServer.baseUri().toString() + "db/data/transaction";
    }

    private URL prepareTestImportFile( int lines ) throws IOException
    {
        File tempFile = File.createTempFile( "testImport", ".csv" );
        try ( PrintWriter writer = FileUtils.newFilePrintWriter( tempFile, StandardCharsets.UTF_8 ) )
        {
            for ( int i = 0; i < lines; i++ )
            {
                writer.println( "a,b,c" );
            }
        }
        return tempFile.toURI().toURL();
    }

    private int findFreePort()
    {
        return freePort( 8000, 8100 );
    }

    private int freePort( int startRange, int endRange )
    {
        try
        {
            return Ports.findFreePort( Ports.INADDR_LOCALHOST, new int[]{startRange, endRange} ).getPort();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to find an available port: " + e.getMessage(), e );
        }
    }

    private Response execute( GraphDatabaseShellServer shellServer,
            CollectingOutput output, Serializable clientId, String command ) throws ShellException
    {
        return shellServer.interpretLine( clientId, command, output );
    }

    private SameJvmClient getShellClient( GraphDatabaseShellServer shellServer ) throws ShellException, RemoteException
    {
        SameJvmClient client = new SameJvmClient( new HashMap<>(), shellServer,
                new CollectingOutput(), InterruptSignalHandler.getHandler() );
        cleanupRule.add( client );
        return client;
    }

    private GraphDatabaseShellServer getGraphDatabaseShellServer( GraphDatabaseAPI database ) throws RemoteException
    {
        GraphDatabaseShellServer shellServer = new GraphDatabaseShellServer( database );
        cleanupRule.add( shellServer );
        return shellServer;
    }

    private void assertDatabaseDoesNotHaveNodes( GraphDatabaseAPI database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            assertEquals( 0, database.getAllNodes().stream().count() );
        }
    }

    private GraphDatabaseAPI startCustomGuardedDatabase( File storeDir, Map<Setting<?>,
            String> configMap )
    {
        CustomClockCommunityFacadeFactory guardCommunityFacadeFactory = new CustomClockGuardedCommunityFacadeFactory();
        GraphDatabaseBuilder databaseBuilder =
                new CustomGuardTestTestGraphDatabaseFactory( guardCommunityFacadeFactory )
                        .newImpermanentDatabaseBuilder( storeDir );
        configMap.forEach( databaseBuilder::setConfig );

        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseBuilder.newGraphDatabase();
        cleanupRule.add( database );
        return database;
    }

    private GraphDatabaseAPI startCustomDatabase( File storeDir, Map<Setting<?>,String> configMap )
    {
        CustomClockCommunityFacadeFactory customClockCommunityFacadeFactory = new CustomClockCommunityFacadeFactory();
        GraphDatabaseBuilder databaseBuilder = new CustomGuardTestTestGraphDatabaseFactory(
                customClockCommunityFacadeFactory )
                .newImpermanentDatabaseBuilder( storeDir );
        configMap.forEach( databaseBuilder::setConfig );

        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseBuilder.newGraphDatabase();
        cleanupRule.add( database );
        return database;
    }

    private class GuardingServerBuilder extends CommunityServerBuilder
    {
        private GraphDatabaseFacade graphDatabaseFacade;
        final LifecycleManagingDatabase.GraphFactory PRECREATED_FACADE_FACTORY =
                ( config, dependencies ) -> graphDatabaseFacade;

        protected GuardingServerBuilder( GraphDatabaseFacade graphDatabaseAPI )
        {
            super( NullLogProvider.getInstance() );
            this.graphDatabaseFacade = graphDatabaseAPI;
        }

        @Override
        protected CommunityNeoServer build( Optional<File> configFile, Config config,
                GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            return new GuardTestServer( config, dependencies, NullLogProvider.getInstance() );
        }

        private class GuardTestServer extends CommunityNeoServer
        {
            GuardTestServer( Config config,
                    GraphDatabaseFacadeFactory.Dependencies dependencies, LogProvider logProvider )
            {
                super( config, LifecycleManagingDatabase.lifecycleManagingDatabase( PRECREATED_FACADE_FACTORY ),
                        dependencies, logProvider );
            }
        }
    }

    private class CustomGuardTestTestGraphDatabaseFactory extends TestGraphDatabaseFactory
    {

        private GraphDatabaseFacadeFactory customFacadeFactory;

        CustomGuardTestTestGraphDatabaseFactory( GraphDatabaseFacadeFactory customFacadeFactory )
        {
            this.customFacadeFactory = customFacadeFactory;
        }

        @Override
        protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator( File storeDir,
                TestGraphDatabaseFactoryState state )
        {
            return new GraphDatabaseBuilder.DatabaseCreator()
            {
                @Override
                public GraphDatabaseService newDatabase( Map<String,String> config )
                {
                    return newDatabase( Config.embeddedDefaults( config ) );
                }

                @Override
                public GraphDatabaseService newDatabase( Config config )
                {
                    return customFacadeFactory.newFacade( storeDir, config,
                            GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
                }
            };
        }
    }

    private class CustomClockCommunityFacadeFactory extends GraphDatabaseFacadeFactory
    {

        CustomClockCommunityFacadeFactory()
        {
            super( DatabaseInfo.COMMUNITY, CommunityEditionModule::new );
        }

        @Override
        protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies,
                GraphDatabaseFacade graphDatabaseFacade )
        {
            return new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade )
            {
                @Override
                protected SystemNanoClock createClock()
                {
                    return fakeClock;
                }
            };
        }

        @Override
        protected DataSourceModule createDataSource( PlatformModule platformModule, EditionModule editionModule,
                Supplier<QueryExecutionEngine> queryEngine )
        {
            return new CustomClockDataSourceModule( platformModule, editionModule, queryEngine );
        }
    }

    private class CustomClockDataSourceModule extends DataSourceModule
    {

        CustomClockDataSourceModule( PlatformModule platformModule, EditionModule editionModule,
                Supplier<QueryExecutionEngine> queryEngine )
        {
            super( platformModule, editionModule, queryEngine );
        }

    }

    private class CustomClockGuardedCommunityFacadeFactory extends CustomClockCommunityFacadeFactory
    {

        CustomClockGuardedCommunityFacadeFactory()
        {
            super();
        }

        @Override
        protected DataSourceModule createDataSource( PlatformModule platformModule, EditionModule editionModule,
                Supplier<QueryExecutionEngine> queryEngine )
        {
            return new GuardedCustomClockDataSourceModule( platformModule, editionModule, queryEngine );
        }
    }

    private class GuardedCustomClockDataSourceModule extends CustomClockDataSourceModule
    {

        GuardedCustomClockDataSourceModule( PlatformModule platformModule, EditionModule editionModule,
                Supplier<QueryExecutionEngine> queryEngine )
        {
            super( platformModule, editionModule, queryEngine );
        }

        @Override
        protected TimeoutGuard createGuard( Clock clock, LogService logging )
        {
            return tickingGuard;
        }
    }

    private class TickingGuard extends TimeoutGuard
    {
        private final FakeClock clock;
        private final long delta;
        private final TimeUnit unit;

        TickingGuard( Clock clock, Log log, long delta, TimeUnit unit )
        {
            super( clock, log );
            this.clock = (FakeClock) clock;
            this.delta = delta;
            this.unit = unit;
        }

        @Override
        public void check( KernelStatement statement )
        {
            super.check( statement );
            clock.forward( delta, unit );
        }
    }
}
