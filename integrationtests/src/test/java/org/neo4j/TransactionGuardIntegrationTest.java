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

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.impl.api.KernelTransactionTimeoutMonitor;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.LifecycleManagingDatabase;
import org.neo4j.server.enterprise.EnterpriseNeoServer;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
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
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionNotFound;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class TransactionGuardIntegrationTest
{
    @ClassRule
    public static CleanupRule cleanupRule = new CleanupRule();
    @ClassRule
    public static TestDirectory testDirectory = TestDirectory.testDirectory();

    private static final String BOLT_CONNECTOR_KEY = "bolt";

    private static final FakeClock fakeClock = Clocks.fakeClock();
    private static GraphDatabaseAPI databaseWithTimeout;
    private static GraphDatabaseAPI databaseWithoutTimeout;
    private static EnterpriseNeoServer neoServer;
    private static int boltPortDatabaseWithTimeout;
    private static final String DEFAULT_TIMEOUT = "2s";
    private static final KernelTransactionTimeoutMonitorSupplier monitorSupplier = new
            KernelTransactionTimeoutMonitorSupplier();
    private static final IdInjectionFunctionAction getIdInjectionFunction = new IdInjectionFunctionAction( monitorSupplier );

    @After
    public void tearDown()
    {
        monitorSupplier.clear();
    }

    @Test
    public void terminateLongRunningTransaction()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            transaction.success();
            timeoutMonitor.run();
            database.createNode();
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException e )
        {
            assertThat( e.getMessage(), startsWith( "The transaction has been terminated." ) );
            assertEquals( e.status(), Status.Transaction.TransactionTimedOut );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningTransactionWithPeriodicCommit() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        monitorSupplier.setTransactionTimeoutMonitor( timeoutMonitor );
        try
        {
            URL url = prepareTestImportFile( 8 );
            database.execute( "USING PERIODIC COMMIT 5 LOAD CSV FROM '" + url + "' AS line CREATE ();" );
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException ignored )
        {
        }
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateTransactionWithCustomTimeoutWithoutConfiguredDefault()
    {
        GraphDatabaseAPI database = startDatabaseWithoutTimeout();
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        try ( Transaction transaction = database.beginTx( 27, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 26, TimeUnit.SECONDS );
            timeoutMonitor.run();
            database.createNode();
            transaction.failure();
        }

        try ( Transaction transaction = database.beginTx( 27, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 28, TimeUnit.SECONDS );
            timeoutMonitor.run();
            database.createNode();
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException e )
        {
            assertThat( e.getMessage(), startsWith( "The transaction has been terminated." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningQueryTransaction()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        monitorSupplier.setTransactionTimeoutMonitor( timeoutMonitor );

        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.success();
            database.execute( "create (n)" );
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException e )
        {
            assertThat( e.getMessage(), startsWith( "The transaction has been terminated." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningQueryWithCustomTimeoutWithoutConfiguredDefault()
    {
        GraphDatabaseAPI database = startDatabaseWithoutTimeout();
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        try ( Transaction transaction = database.beginTx( 5, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 4, TimeUnit.SECONDS );
            timeoutMonitor.run();
            database.execute( "create (n)" );
            transaction.failure();
        }

        try ( Transaction transaction = database.beginTx( 6, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 7, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.success();
            database.execute( "create (n)" );
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException e )
        {
            assertThat( e.getMessage(), startsWith( "The transaction has been terminated." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningShellQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        GraphDatabaseShellServer shellServer = getGraphDatabaseShellServer( database );
        try
        {
            SameJvmClient client = getShellClient( shellServer );
            CollectingOutput commandOutput = new CollectingOutput();
            execute( shellServer, commandOutput, client.getId(), "begin Transaction" );
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
            execute( shellServer, commandOutput, client.getId(), "create (n);" );
            execute( shellServer, commandOutput, client.getId(), "commit" );
            fail( "Transaction should be already terminated." );
        }
        catch ( ShellException e )
        {
            assertThat( e.getMessage(), containsString( "The transaction has not completed within " +
                    "the specified timeout." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningShellPeriodicCommitQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        monitorSupplier.setTransactionTimeoutMonitor( timeoutMonitor );
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
            assertThat( e.getMessage(), containsString( "The transaction has been terminated." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningRestTransactionalEndpointQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        EnterpriseNeoServer neoServer = startNeoServer( (GraphDatabaseFacade) database );
        String transactionEndPoint = HTTP.POST( transactionUri( neoServer ) ).location();

        fakeClock.forward( 3, TimeUnit.SECONDS );
        timeoutMonitor.run();

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
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        EnterpriseNeoServer neoServer = startNeoServer( (GraphDatabaseFacade) database );
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
        timeoutMonitor.run();

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
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        EnterpriseNeoServer neoServer = startNeoServer( (GraphDatabaseFacade) database );

        org.neo4j.driver.v1.Config driverConfig = getDriverConfig();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:" + boltPortDatabaseWithTimeout, driverConfig );
                Session session = driver.session() )
        {
            org.neo4j.driver.v1.Transaction transaction = session.beginTransaction();
            transaction.run( "create (n)" ).consume();
            transaction.success();
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
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
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        monitorSupplier.setTransactionTimeoutMonitor( timeoutMonitor );
        EnterpriseNeoServer neoServer = startNeoServer( (GraphDatabaseFacade) database );

        org.neo4j.driver.v1.Config driverConfig = getDriverConfig();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:" + boltPortDatabaseWithTimeout, driverConfig );
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

    @Test
    public void changeTimeoutAtRuntime() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionTimeoutMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionTimeoutMonitor.class );
        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.success();
            database.execute( "create (n)" );
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException e )
        {
            assertThat( e.getMessage(), startsWith( "The transaction has been terminated." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );

        // Increase timeout
        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "CALL dbms.setConfigValue('" + transaction_timeout.name() + "', '5s')" );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.success();
            database.execute( "create (n)" );
        }

        // Assert node successfully created
        try ( Transaction ignored = database.beginTx() )
        {
            assertEquals( 1, database.getAllNodes().stream().count() );
        }

        // Reset timeout and cleanup
        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "CALL dbms.setConfigValue('" + transaction_timeout.name() + "', '" + DEFAULT_TIMEOUT + "')" );
            try ( Stream<Node> stream = database.getAllNodes().stream() )
            {
                stream.findFirst().map( node ->
                {
                    node.delete();
                    return node;
                } );
            }
            transaction.success();
        }
    }

    private GraphDatabaseAPI startDatabaseWithTimeout()
    {
        if ( databaseWithTimeout == null )
        {
            Map<Setting<?>,String> configMap = getSettingsWithTimeoutAndBolt();
            databaseWithTimeout = startCustomDatabase( testDirectory.directory( "dbWithTimeout" ), configMap );
            boltPortDatabaseWithTimeout = getBoltConnectorPort( databaseWithTimeout );
        }
        return databaseWithTimeout;
    }

    private int getBoltConnectorPort( GraphDatabaseAPI databaseAPI )
    {
        ConnectorPortRegister connectorPortRegister = databaseAPI.getDependencyResolver()
                .resolveDependency( ConnectorPortRegister.class );
        return connectorPortRegister.getLocalAddress( BOLT_CONNECTOR_KEY ).getPort();
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

    private EnterpriseNeoServer startNeoServer( GraphDatabaseFacade database ) throws IOException
    {
        if ( neoServer == null )
        {
            GuardingServerBuilder serverBuilder = new GuardingServerBuilder( database );
            BoltConnector boltConnector = new BoltConnector( BOLT_CONNECTOR_KEY );
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

    private Map<Setting<?>,String> getSettingsWithTimeoutAndBolt()
    {
        BoltConnector boltConnector = new BoltConnector( BOLT_CONNECTOR_KEY );
        return MapUtil.genericMap(
                transaction_timeout, DEFAULT_TIMEOUT,
                boltConnector.address, "localhost:0",
                boltConnector.type, "BOLT",
                boltConnector.enabled, "true",
                boltConnector.encryption_level, BoltConnector.EncryptionLevel.DISABLED.name(),
                GraphDatabaseSettings.auth_enabled, "false" );
    }

    private Map<Setting<?>,String> getSettingsWithoutTransactionTimeout()
    {
        return MapUtil.genericMap();
    }

    private String transactionUri( EnterpriseNeoServer neoServer )
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

    private GraphDatabaseAPI startCustomDatabase( File storeDir, Map<Setting<?>,String> configMap )
    {
        CustomClockEnterpriseFacadeFactory customClockEnterpriseFacadeFactory = new CustomClockEnterpriseFacadeFactory();
        GraphDatabaseBuilder databaseBuilder = new CustomGuardTestTestGraphDatabaseFactory(
                customClockEnterpriseFacadeFactory )
                .newImpermanentDatabaseBuilder( storeDir );
        configMap.forEach( databaseBuilder::setConfig );
        databaseBuilder.setConfig( GraphDatabaseSettings.record_id_batch_size, "1" );

        GraphDatabaseAPI database = (GraphDatabaseAPI) databaseBuilder.newGraphDatabase();
        cleanupRule.add( database );
        return database;
    }

    private static class KernelTransactionTimeoutMonitorSupplier implements Supplier<KernelTransactionTimeoutMonitor>
    {
        private volatile KernelTransactionTimeoutMonitor transactionTimeoutMonitor;

        void setTransactionTimeoutMonitor( KernelTransactionTimeoutMonitor transactionTimeoutMonitor )
        {
            this.transactionTimeoutMonitor = transactionTimeoutMonitor;
        }

        @Override
        public KernelTransactionTimeoutMonitor get()
        {
            return transactionTimeoutMonitor;
        }

        public void clear()
        {
            setTransactionTimeoutMonitor( null );
        }
    }

    private static class IdInjectionFunctionAction
    {
        private final Supplier<KernelTransactionTimeoutMonitor> monitorSupplier;

        IdInjectionFunctionAction( Supplier<KernelTransactionTimeoutMonitor> monitorSupplier )
        {
            this.monitorSupplier = monitorSupplier;
        }

        void tickAndCheck()
        {
            KernelTransactionTimeoutMonitor timeoutMonitor = monitorSupplier.get();
            if ( timeoutMonitor != null )
            {
                fakeClock.forward( 1, TimeUnit.SECONDS );
                timeoutMonitor.run();
            }
        }
    }

    private class GuardingServerBuilder extends EnterpriseServerBuilder
    {
        private GraphDatabaseFacade graphDatabaseFacade;
        final LifecycleManagingDatabase.GraphFactory PRECREATED_FACADE_FACTORY =
                ( config, dependencies ) -> graphDatabaseFacade;

        GuardingServerBuilder( GraphDatabaseFacade graphDatabaseAPI )
        {
            super( NullLogProvider.getInstance() );
            this.graphDatabaseFacade = graphDatabaseAPI;
        }

        @Override
        protected CommunityNeoServer build( File configFile, Config config,
                GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            return new GuardTestServer( config, dependencies, NullLogProvider.getInstance() );
        }

        private class GuardTestServer extends EnterpriseNeoServer
        {
            GuardTestServer( Config config, GraphDatabaseFacadeFactory.Dependencies dependencies, LogProvider logProvider )
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
                    return newDatabase( Config.defaults( config ) );
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

    private class TransactionGuardTerminationEditionModule extends EnterpriseEditionModule
    {
        TransactionGuardTerminationEditionModule( PlatformModule platformModule )
        {
            super( platformModule );
        }

        @Override
        protected IdGeneratorFactory createIdGeneratorFactory( FileSystemAbstraction fs,
                IdTypeConfigurationProvider idTypeConfigurationProvider )
        {
            IdGeneratorFactory generatorFactory = super.createIdGeneratorFactory( fs, idTypeConfigurationProvider );
            return new TerminationIdGeneratorFactory( generatorFactory );
        }
    }

    private class CustomClockEnterpriseFacadeFactory extends GraphDatabaseFacadeFactory
    {

        CustomClockEnterpriseFacadeFactory()
        {
            super( DatabaseInfo.ENTERPRISE, new Function<PlatformModule,EditionModule>()
            {
                @Override
                public EditionModule apply( PlatformModule platformModule )
                {
                    return new TransactionGuardTerminationEditionModule( platformModule );
                }
            } );
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
    }

    private class TerminationIdGeneratorFactory implements IdGeneratorFactory
    {
        private IdGeneratorFactory delegate;

        TerminationIdGeneratorFactory( IdGeneratorFactory delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public IdGenerator open( File filename, IdType idType, Supplier<Long> highIdSupplier, long maxId )
        {
            return delegate.open( filename, idType, highIdSupplier, maxId );
        }

        @Override
        public IdGenerator open( File filename, int grabSize, IdType idType, Supplier<Long> highIdSupplier, long maxId )
        {
            return new TerminationIdGenerator( delegate.open( filename, grabSize, idType, highIdSupplier, maxId ) );
        }

        @Override
        public void create( File filename, long highId, boolean throwIfFileExists )
        {
            delegate.create( filename, highId, throwIfFileExists );
        }

        @Override
        public IdGenerator get( IdType idType )
        {
            return delegate.get( idType );
        }
    }

    private class TerminationIdGenerator implements IdGenerator
    {

        private IdGenerator delegate;

        TerminationIdGenerator( IdGenerator delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            return delegate.nextIdBatch( size );
        }

        @Override
        public void setHighId( long id )
        {
            delegate.setHighId( id );
        }

        @Override
        public long getHighId()
        {
            return delegate.getHighId();
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return delegate.getHighestPossibleIdInUse();
        }

        @Override
        public void freeId( long id )
        {
            delegate.freeId( id );
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return delegate.getNumberOfIdsInUse();
        }

        @Override
        public long getDefragCount()
        {
            return delegate.getDefragCount();
        }

        @Override
        public void delete()
        {
            delegate.delete();
        }

        @Override
        public long nextId()
        {
            getIdInjectionFunction.tickAndCheck();
            return delegate.nextId();
        }
    }
}
