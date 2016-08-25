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
package org.neo4j;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.guard.GuardTimeoutException;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.LifecycleManagingDatabase;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.shell.InterruptSignalHandler;
import org.neo4j.shell.Response;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactoryState;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.server.HTTP;
import org.neo4j.time.FakeClock;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnector;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionNotFound;

public class TransactionGuardIntegrationTest
{
    @Rule
    public CleanupRule cleanupRule = new CleanupRule();
    private FakeClock clock;
    private GraphDatabaseAPI database;

    @Before
    public void setUp()
    {
        clock = new FakeClock();
        Map<Setting<?>,String> configMap = getSettingsMap();
        database = startCustomDatabase( clock, configMap );
    }

    @Test
    public void terminateLongRunningTransaction()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            clock.forward( 3, TimeUnit.SECONDS );
            transaction.success();
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
        try ( Transaction transaction = database.beginTx() )
        {
            clock.forward( 3, TimeUnit.SECONDS );
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
        GraphDatabaseShellServer shellServer = getGraphDatabaseShellServer( database );
        try
        {
            SameJvmClient client = getShellClient( shellServer );
            CollectingOutput commandOutput = new CollectingOutput();
            execute( shellServer, commandOutput, client.getId(), "begin Transaction" );
            clock.forward( 3, TimeUnit.SECONDS );
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
    public void terminateLongRunningRestQuery() throws Exception
    {
        CommunityNeoServer neoServer = startNeoServer( (GraphDatabaseFacade) database );
        String transactionEndPoint = HTTP.POST( transactionUri(neoServer), singletonList( map( "statement", "create (n)" ) ) ).location();

        clock.forward( 3, TimeUnit.SECONDS );
        HTTP.Response commitResponse = HTTP.POST( transactionEndPoint + "/commit" );
        assertEquals( "Transaction should be already closed and not found.", 404, commitResponse.status() );

        assertEquals( "Transaction should be forcefully closed.", TransactionNotFound.code().serialize(),
                commitResponse.get( "errors" ).findValue( "code" ).asText());
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningDriverQuery() throws Exception
    {
        CommunityNeoServer neoServer = startNeoServer( (GraphDatabaseFacade) database );

        org.neo4j.driver.v1.Config driverConfig = getDriverConfig();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost", driverConfig );
              Session session = driver.session() )
        {
            org.neo4j.driver.v1.Transaction transaction = session.beginTransaction();
            transaction.run( "create (n)" ).consume();
            transaction.success();
            clock.forward( 3, TimeUnit.SECONDS );
            try
            {
                transaction.run( "create (n)" ).consume();
                fail("Transaction should be already terminated by execution guard.");
            }
            catch ( Exception expected )
            {
                // ignored
            }
        }
        assertDatabaseDoesNotHaveNodes( database );
    }

    private org.neo4j.driver.v1.Config getDriverConfig()
    {
        return org.neo4j.driver.v1.Config.build()
                .withEncryptionLevel( org.neo4j.driver.v1.Config.EncryptionLevel.NONE )
                .toConfig();
    }

    private CommunityNeoServer startNeoServer( GraphDatabaseFacade database ) throws IOException
    {
        GuardingServerBuilder serverBuilder = new GuardingServerBuilder( database );
        GraphDatabaseSettings.BoltConnector boltConnector = boltConnector( "0" );
        serverBuilder.withProperty( boltConnector.type.name(), "BOLT" )
                .withProperty( boltConnector.enabled.name(), "true" )
                .withProperty( boltConnector.encryption_level.name(), GraphDatabaseSettings.BoltConnector.EncryptionLevel.DISABLED.name())
                .withProperty( GraphDatabaseSettings.auth_enabled.name(), "false" );
        CommunityNeoServer neoServer = serverBuilder.build();
        cleanupRule.add( neoServer );
        neoServer.start();
        return neoServer;
    }

    private Map<Setting<?>,String> getSettingsMap()
    {
        GraphDatabaseSettings.BoltConnector boltConnector = boltConnector( "0" );
        return MapUtil.genericMap(
                boltConnector.type, "BOLT",
                boltConnector.enabled, "true",
                boltConnector.encryption_level, GraphDatabaseSettings.BoltConnector.EncryptionLevel.DISABLED.name(),
                GraphDatabaseSettings.execution_guard_enabled, Settings.TRUE,
                GraphDatabaseSettings.transaction_timeout, "2s",
                GraphDatabaseSettings.auth_enabled, "false" );
    }

    private String transactionUri(CommunityNeoServer neoServer)
    {
        return neoServer.baseUri().toString() + "db/data/transaction";
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

    private GraphDatabaseAPI startCustomDatabase( Clock clock, Map<Setting<?>,String> configMap )
    {
        GuardCommunityFacadeFactory guardCommunityFacadeFactory = new GuardCommunityFacadeFactory( clock );
        GraphDatabaseAPI database =
                (GraphDatabaseAPI) new GuardTestGraphDatabaseFactory( guardCommunityFacadeFactory )
                        .newImpermanentDatabase( configMap );
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

    private class GuardTestGraphDatabaseFactory extends TestGraphDatabaseFactory
    {

        private GraphDatabaseFacadeFactory customFacadeFactory;

        GuardTestGraphDatabaseFactory( GraphDatabaseFacadeFactory customFacadeFactory )
        {
            this.customFacadeFactory = customFacadeFactory;
        }

        @Override
        protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator( File storeDir,
                TestGraphDatabaseFactoryState state )
        {
            return config -> customFacadeFactory.newFacade( storeDir, config,
                    GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
        }
    }

    private class GuardCommunityFacadeFactory extends GraphDatabaseFacadeFactory
    {

        private Clock clock;

        GuardCommunityFacadeFactory( Clock clock )
        {
            super( DatabaseInfo.COMMUNITY, CommunityEditionModule::new);
            this.clock = clock;
        }

        @Override
        protected DataSourceModule createDataSource( Dependencies dependencies,
                PlatformModule platformModule, EditionModule editionModule )
        {
            return new GuardDataSourceModule( dependencies, platformModule, editionModule, clock );
        }

        private class GuardDataSourceModule extends DataSourceModule
        {

            GuardDataSourceModule( GraphDatabaseFacadeFactory.Dependencies dependencies,
                    PlatformModule platformModule, EditionModule editionModule, Clock clock )
            {
                super( dependencies, platformModule, editionModule );
            }

            @Override
            protected Clock getClock()
            {
                return clock;
            }
        }
    }
}
