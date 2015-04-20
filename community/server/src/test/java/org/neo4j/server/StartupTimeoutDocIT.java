/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.helpers.Transactor;
import org.neo4j.server.helpers.UnitOfWork;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class StartupTimeoutDocIT
{
    @Test
    public void shouldTimeoutIfStartupTakesLongerThanTimeout() throws IOException
    {
        // GIVEN
        ConfigurationBuilder configurator = buildProperties().withStartupTimeout( 1 ).atPort( 7480 ).build();
        server = createSlowServer( configurator, true );

        // WHEN
        try
        {
            server.start();
            fail( "Should have been interrupted." );
        }
        catch ( ServerStartupException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( "Startup took longer than" ) );
        }
    }

    @Test
    public void shouldNotFailIfStartupTakesLessTimeThanTimeout() throws IOException
    {
        ConfigurationBuilder configurator = buildProperties().withStartupTimeout( 100 ).atPort( 7480 ).build();
        server = new CommunityNeoServer( configurator, GraphDatabaseDependencies.newDependencies().logging(
                DevNullLoggingService.DEV_NULL ) )
        {
            @Override
            protected Iterable<ServerModule> createServerModules()
            {
                return Arrays.asList();
            }
        };

        // When
        try
        {
            server.start();
        }
        catch ( ServerStartupException e )
        {
            fail( "Should not have been interupted." );
        }

        // Then
        InterruptThreadTimer timer = server.getDependencyResolver().resolveDependency( InterruptThreadTimer.class );

        assertThat( timer.getState(), is( InterruptThreadTimer.State.IDLE ) );
    }

    @Test
    public void shouldNotTimeOutIfTimeoutDisabled() throws IOException
    {
        ConfigurationBuilder configurator = buildProperties().withStartupTimeout( 0 ).atPort( 7480 ).build();
        server = createSlowServer( configurator, false );

        // When
        server.start();

        // Then
        // No exceptions should have been thrown
    }

    private CommunityNeoServer createSlowServer( ConfigurationBuilder configurator,
            final boolean preventMovingFurtherThanStartingModules )
    {
        final AtomicReference<Runnable> timerStartSignal = new AtomicReference<>();
        CommunityNeoServer server = new CommunityNeoServer( configurator, GraphDatabaseDependencies.newDependencies()
                .logging( DevNullLoggingService.DEV_NULL ) )
        {
            @Override
            protected InterruptThreadTimer createInterruptStartupTimer()
            {
                /* Create an InterruptThreadTimer that won't start its count down until server modules have
                 * started to load (and in the case of these tests wait long enough for the timer to trigger.
                 * This makes it deterministic precisely where in the startup sequence the interrupt happens.
                 * Whereas this is a bit too deterministic compared to the real world, this is a test that must
                 * complete in the same way every time. Another test should verify that an interrupt happening
                 * anywhere in the startup sequence still aborts the startup and cleans up properly. */

                InterruptThreadTimer realTimer = super.createInterruptStartupTimer();
                return timerThatStartsWhenModulesStartsLoading( realTimer );
            }

            private InterruptThreadTimer timerThatStartsWhenModulesStartsLoading( final InterruptThreadTimer realTimer )
            {
                return new InterruptThreadTimer()
                {
                    @Override
                    public boolean wasTriggered()
                    {
                        return realTimer.wasTriggered();
                    }

                    @Override
                    public void stopCountdown()
                    {
                        realTimer.stopCountdown();
                    }

                    @Override
                    public void startCountdown()
                    {
                        timerStartSignal.set( new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                realTimer.startCountdown();
                            }
                        } );
                    }

                    @Override
                    public long getTimeoutMillis()
                    {
                        return realTimer.getTimeoutMillis();
                    }

                    @Override
                    public State getState()
                    {
                        return realTimer.getState();
                    }
                };
            }

            @Override
            protected Iterable<ServerModule> createServerModules()
            {
                ServerModule slowModule = new ServerModule()
                {
                    @Override
                    public void start()
                    {
                        timerStartSignal.get().run();
                        try
                        {
                            Thread.sleep( 1000 * 5 );
                        }
                        catch ( InterruptedException e )
                        {
                            throw new RuntimeException( e );
                        }

                        if ( preventMovingFurtherThanStartingModules )
                        {
                            fail( "Should not get here" );
                        }
                    }

                    @Override
                    public void stop()
                    {
                    }
                };
                return Arrays.asList( slowModule );
            }
        };
        return server;
    }

    private ConfiguratorBuilder buildProperties() throws IOException
    {
        new File( test.directory().getAbsolutePath() + DIRSEP + "conf" ).mkdirs();

        Properties databaseProperties = new Properties();
        String databasePropertiesFileName = test.directory().getAbsolutePath() + DIRSEP + "conf" + DIRSEP
                + "neo4j.properties";
        databaseProperties.store( new FileWriter( databasePropertiesFileName ), null );

        Properties serverProperties = new Properties();
        String serverPropertiesFilename = test.directory().getAbsolutePath() + DIRSEP + "conf" + DIRSEP
                + "neo4j-server.properties";
        serverProperties.setProperty( Configurator.DATABASE_LOCATION_PROPERTY_KEY, test.directory().getAbsolutePath()
                + DIRSEP + "data" + DIRSEP + "graph.db" );

        serverProperties.setProperty( Configurator.DB_TUNING_PROPERTY_FILE_KEY, databasePropertiesFileName );
        serverProperties.setProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY, serverPropertiesFilename );
        serverProperties.store( new FileWriter( serverPropertiesFilename ), null );

        return new ConfiguratorBuilder( new PropertyFileConfigurator( new File( serverPropertiesFilename ) ) );
    }

    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
    TargetDirectory target = TargetDirectory.forTest( StartupTimeoutDocIT.class );
    private static final String DIRSEP = File.separator;

    @Rule
    public TargetDirectory.TestDirectory test = target.testDirectory();
    
    public CommunityNeoServer server;
    public @Rule TestName testName = new TestName();

    @After
    public void stopServer()
    {
        if ( server != null )
        {
            server.stop();
            server = null;
        }
    }

    private void clearAll()
    {
        new Transactor( dbRule.getGraphDatabaseService(), new UnitOfWork()
        {
            @Override
            public void doWork()
            {
                deleteAllNodesAndRelationships( dbRule.getGraphDatabaseService() );
                
                deleteAllIndexes( dbRule.getGraphDatabaseService() );
            }

            private void deleteAllNodesAndRelationships( final GraphDatabaseService db )
            {
                Iterable<Node> allNodes = GlobalGraphOperations.at( db ).getAllNodes();
                for ( Node n : allNodes )
                {
                    Iterable<Relationship> relationships = n.getRelationships();
                    for ( Relationship rel : relationships )
                    {
                        rel.delete();
                    }
                    if ( n.getId() != 0 )
                    { // Don't delete the reference node - tests depend on it
                      // :-(
                        n.delete();
                    }
                    else
                    { // Remove all state from the reference node instead
                        for ( String key : n.getPropertyKeys() )
                        {
                            n.removeProperty( key );
                        }
                    }
                }
            }

            private void deleteAllIndexes( final GraphDatabaseService db )
            {
                IndexManager indexManager = db.index();
                
                for ( String indexName : indexManager.nodeIndexNames() )
                {
                    try
                    {
                        db.index().forNodes( indexName ).delete();
                    }
                    catch ( UnsupportedOperationException e )
                    {
                        // Encountered a read-only index.
                    }
                }
                
                for ( String indexName : indexManager.relationshipIndexNames() )
                {
                    try
                    {
                        db.index().forRelationships( indexName ).delete();
                    }
                    catch ( UnsupportedOperationException e )
                    {
                        // Encountered a read-only index.
                    }
                }
                
                for ( String k : indexManager.getNodeAutoIndexer().getAutoIndexedProperties() )
                {
                    indexManager.getNodeAutoIndexer().stopAutoIndexingProperty( k );
                }
                indexManager.getNodeAutoIndexer().setEnabled( false );
                
                for ( String k : indexManager.getRelationshipAutoIndexer().getAutoIndexedProperties() )
                {
                    indexManager.getRelationshipAutoIndexer().stopAutoIndexingProperty( k );
                }
                indexManager.getRelationshipAutoIndexer().setEnabled( false );
            }
        } ).execute();
    }

    /**
     * Produces more readable and understandable test code where this builder is used compared to raw Configurator.
     *
     * This is not a good way to add new properties as we should not allow adding properties once the configuration is created.
     * It might be okay if we just use this for testing.
     */
    @Deprecated
    private static class ConfiguratorBuilder
    {
        private final ConfigurationBuilder configurator;

        public ConfiguratorBuilder( ConfigurationBuilder initialConfigurator )
        {
            this.configurator = initialConfigurator;
        }

        public ConfiguratorBuilder atPort( int port )
        {
            setProperty( Configurator.WEBSERVER_PORT_PROPERTY_KEY, String.valueOf( port ) );
            return this;
        }

        public ConfiguratorBuilder withStartupTimeout( long seconds )
        {
            setProperty( Configurator.STARTUP_TIMEOUT, String.valueOf( seconds ) );
            return this;
        }

        public ConfigurationBuilder build()
        {
            return configurator;
        }

        private void setProperty( String key, String value )
        {
            Map<String,String> params = configurator.configuration().getParams();
            params.put( key, value );
            configurator.configuration().applyChanges( params );
        }
    }
}
