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
package org.neo4j.server.enterprise;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.jboss.netty.channel.ChannelException;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.client.ClusterClientModule;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.server.configuration.ServerSettings;

import static org.neo4j.helpers.Exceptions.peel;

/**
 * Wrapper around a {@link ClusterClient} to fit the environment of the Neo4j server.
 * <p>
 * Configuration of the cluster client can be specified in the neo4j configuration file, specified by the
 * org.neo4j.config.file system property.
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
        try
        {
            LifeSupport life = new LifeSupport();
            life.add( new Neo4jJobScheduler() );

            new ClusterClientModule(
                    life,
                    new Dependencies(),
                    new Monitors(),
                    getConfig( args ),
                    logService( new DefaultFileSystemAbstraction() ),
                    new NotElectableElectionCredentialsProvider() );

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

    private static Config getConfig( String[] args ) throws IOException
    {
        Map<String, String> config = new HashMap<>();
        String configPath = System.getProperty( ServerSettings.SERVER_CONFIG_FILE_KEY );
        if ( configPath != null )
        {
            File configFile = new File( configPath );
            if ( configFile.exists() )
            {
                config.putAll( MapUtil.load( configFile ) );
            }
            else
            {
                throw new IllegalArgumentException( configFile + " doesn't exist" );
            }
        }

        config.putAll( Args.parse( args ).asMap() );
        verifyConfig( config );
        return new Config( config );
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

    private static LogService logService( FileSystemAbstraction fileSystem ) throws IOException
    {
        String logDir = System.getProperty( "org.neo4j.cluster.logdirectory", "data/log" );
        return StoreLogService.withUserLogProvider( FormattedLogProvider.toOutputStream( System.out ) )
                .inStoreDirectory( fileSystem, new File( logDir ) );
    }
}
