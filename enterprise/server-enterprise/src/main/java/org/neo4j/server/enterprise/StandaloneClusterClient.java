/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.jboss.netty.channel.ChannelException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.client.ClusterClientModule;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;

import static org.neo4j.helpers.Exceptions.peel;
import static org.neo4j.helpers.collection.MapUtil.loadStrictly;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.configuration.Configurator.DB_TUNING_PROPERTY_FILE_KEY;
import static org.neo4j.server.configuration.Configurator.NEO_SERVER_CONFIG_FILE_KEY;

/**
 * Wrapper around a {@link ClusterClient} to fit the environment of the Neo4j server,
 * mostly regarding the use of the server config file passed in from the script starting
 * this class. That server config file will be parsed and necessary parts passed on.
 * <p>
 * Configuration of the cluster client can be specified by
 * <ol>
 * <li>reading from a db tuning file (neo4j.properties) appointed by the neo4j server configuration file,
 * specified from org.neo4j.server.properties system property.</li>
 * <li>
 * </li>
 *
 * @author Mattias Persson
 */
public class StandaloneClusterClient
{
    private final LifeSupport life;
    private final Timer timer;

    private StandaloneClusterClient( LifeSupport life )
    {
        this.life = life;
        timer = new Timer( true );
        addShutdownHook();
        life.start();
    }

    protected void addShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                // ClusterJoin will block on a Future.get(), which will prevent it to shutdown.
                // Adding a timer here in case a shutdown is requested before cluster join has succeeded. Otherwise
                // the deadlock will prevent the shutdown from finishing.
                timer.schedule( new TimerTask()
                {
                    @Override
                    public void run()
                    {
                        System.err.println( "Failed to stop in a reasonable time, terminating..." );
                        Runtime.getRuntime().halt( 1 );
                    }
                },  4_000L);
                life.shutdown();
            }
        } );
    }


    public static void main( String[] args ) throws IOException
    {
        String propertiesFile = System.getProperty( NEO_SERVER_CONFIG_FILE_KEY );
        File dbProperties = extractDbTuningProperties( propertiesFile );
        Map<String, String> config = stringMap();
        if ( dbProperties != null )
        {
            if ( !dbProperties.exists() )
            {
                throw new IllegalArgumentException( dbProperties + " doesn't exist" );
            }
            config = readFromConfigConfig( config, dbProperties );
        }
        config.putAll( Args.parse( args ).asMap() );
        verifyConfig( config );
        try
        {
            JobScheduler jobScheduler = new Neo4jJobScheduler();
            LogService logService = logService( new DefaultFileSystemAbstraction() );

            LifeSupport life = new LifeSupport();
            life.add(jobScheduler);
            Dependencies dependencies = new Dependencies();
            ClusterClientModule clusterClientModule = new ClusterClientModule( life, dependencies, new Monitors(), new Config( config ), logService, new NotElectableElectionCredentialsProvider() );

            new StandaloneClusterClient( life );
        }
        catch ( LifecycleException e )
        {
            @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "unchecked"})
            Throwable cause = peel( e, Predicates.<Throwable>instanceOf( LifecycleException.class ) );
            if ( cause instanceof ChannelException )
            {
                System.err.println( "ERROR: " + cause.getMessage() +
                        (cause.getCause() != null ? ", caused by:" + cause.getCause().getMessage() : "") );
            }
            else
            {
                System.err.println( "ERROR: Unknown error" );
                throw e;
            }
        }
    }

    private static void verifyConfig( Map<String, String> config )
    {
        if ( !config.containsKey( ClusterSettings.initial_hosts.name() ) )
        {
            System.err.println( "No initial hosts to connect to supplied" );
            System.exit( 1 );
        }
        if ( !config.containsKey( ClusterSettings.server_id.name() ) )
        {
            System.err.println( "No server id specified" );
            System.exit( 1 );
        }
    }

    private static Map<String, String> readFromConfigConfig( Map<String, String> config, File propertiesFile )
    {
        Map<String, String> result = new HashMap<String, String>( config );
        Map<String, String> existingConfig = loadStrictly( propertiesFile );
        for ( Setting<?> setting : new Setting[]{
                ClusterSettings.initial_hosts,
                ClusterSettings.cluster_name,
                ClusterSettings.cluster_server,
                ClusterSettings.server_id} )
        // TODO add timeouts
        {
            moveOver( existingConfig, result, setting );
        }

        return result;
    }

    private static void moveOver( Map<String, String> from, Map<String, String> to, Setting<?> setting )
    {
        String key = setting.name();
        if ( from.containsKey( key ) )
        {
            to.put( key, from.get( key ) );
        }
    }

    private static LogService logService( FileSystemAbstraction fileSystem ) throws IOException
    {
        File home = new File( System.getProperty( "neo4j.home" ) );
        String logDir = System.getProperty( "org.neo4j.cluster.logdirectory",
                new File( new File( new File( home, "data" ), "log" ), "arbiter" ).getAbsolutePath() );
        return StoreLogService.withUserLogProvider( FormattedLogProvider.toOutputStream( System.out ) )
                .inStoreDirectory( fileSystem, new File( logDir ) );
    }

    private static File extractDbTuningProperties( String propertiesFile )
    {
        if ( propertiesFile == null )
        {
            return null;
        }
        File serverConfigFile = new File( propertiesFile );
        if ( !serverConfigFile.exists() )
        {
            return null;
        }

        Map<String, String> serverConfig = loadStrictly( serverConfigFile );
        String dbTuningFile = serverConfig.get( DB_TUNING_PROPERTY_FILE_KEY );
        if ( dbTuningFile == null )
        {
            return null;
        }
        File result = new File( dbTuningFile );
        return result.exists() ? result : null;
    }
}
