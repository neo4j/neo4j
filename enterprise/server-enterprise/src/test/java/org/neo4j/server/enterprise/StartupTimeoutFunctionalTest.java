/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.enterprise;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.server.ServerStartupException;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.fail;

public class StartupTimeoutFunctionalTest
{
    @Rule
    public TargetDirectory.TestDirectory target = TargetDirectory.testDirForTest( getClass() );

    public EnterpriseNeoServer server;

    @After
    public void stopServer()
    {
        if ( server != null )
        {
            server.stop();
            server = null;
        }
    }

    @Test
    public void shouldTimeoutIfStartupTakesLongerThanTimeout() throws IOException
    {
        ConfigurationBuilder configurator = buildProperties();
        setProperty( configurator.configuration(), Configurator.STARTUP_TIMEOUT, "1s" );
        server = createSlowServer( configurator );

        try
        {
            server.start();
            fail( "Should have been interrupted." );
        }
        catch ( ServerStartupException e )
        {
            // ok!
        }
    }

    @Test
    public void shouldNotFailIfStartupTakesLessTimeThanTimeout() throws IOException
    {
        ConfigurationBuilder configurator = buildProperties();
        setProperty( configurator.configuration(), Configurator.STARTUP_TIMEOUT, "20s" );
        server = new EnterpriseNeoServer( configurator, GraphDatabaseDependencies.newDependencies().logging(DevNullLoggingService.DEV_NULL ))
        {
            @Override
            protected Iterable<ServerModule> createServerModules()
            {
                return Arrays.asList();
            }
        };

        server.start();
    }

    @Test
    public void shouldNotTimeOutIfTimeoutDisabled() throws IOException
    {
        ConfigurationBuilder configurator = buildProperties();
        setProperty( configurator.configuration(), Configurator.STARTUP_TIMEOUT, "0" );
        server = createSlowServer( configurator );

        server.start();
    }

    private EnterpriseNeoServer createSlowServer( ConfigurationBuilder configurator )
    {
        return new EnterpriseNeoServer( configurator, GraphDatabaseDependencies.newDependencies().logging(DevNullLoggingService.DEV_NULL ))
        {
            @Override
            protected Iterable<ServerModule> createServerModules()
            {
                ServerModule slowModule = new ServerModule()
                {
                    @Override
                    public void start()
                    {
                        try
                        {
                            Thread.sleep( 1000 * 5 );
                        }
                        catch ( InterruptedException e )
                        {
                            throw new RuntimeException( e );
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
    }

    private ConfigurationBuilder buildProperties() throws IOException
    {
        //noinspection ResultOfMethodCallIgnored
        new File( target.directory(), "conf" ).mkdir();

        Properties databaseProperties = new Properties();
        String databasePropertiesFileName = new File( target.directory(), "conf/neo4j.properties" ).getAbsolutePath();
        databaseProperties.setProperty( ClusterSettings.server_id.name(), "1" );
        databaseProperties.setProperty( ClusterSettings.initial_hosts.name(), ":5001,:5002,:5003" );
        databaseProperties.store( new FileWriter( databasePropertiesFileName ), null );

        Properties serverProperties = new Properties();
        String serverPropertiesFilename = new File( target.directory(), "conf/neo4j-server.properties" ).getAbsolutePath();
        serverProperties.setProperty( Configurator.DATABASE_LOCATION_PROPERTY_KEY,
                new File( target.directory(), "data/graph.db" ).getAbsolutePath() );
        serverProperties.setProperty( Configurator.DB_TUNING_PROPERTY_FILE_KEY, databasePropertiesFileName );
        serverProperties.setProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY, serverPropertiesFilename );
        serverProperties.store( new FileWriter( serverPropertiesFilename ), null );

        return new PropertyFileConfigurator( new File( serverPropertiesFilename ) );
    }
    
    private void setProperty( Config config, String key, String value )
    {
        Map<String, String> params = config.getParams();
        params.put( key, value );
        config.applyChanges( params );
    }
}
