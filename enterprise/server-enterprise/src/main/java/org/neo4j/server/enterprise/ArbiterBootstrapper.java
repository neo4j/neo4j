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
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

import org.jboss.netty.channel.ChannelException;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClientModule;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
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
import org.neo4j.server.Bootstrapper;

import static org.neo4j.helpers.Exceptions.peel;

public class ArbiterBootstrapper implements Bootstrapper, AutoCloseable
{
    private final LifeSupport life = new LifeSupport();
    private final Timer timer = new Timer( true );

    @SafeVarargs
    @Override
    public final int start( File homeDir, Optional<File> configFile, Pair<String, String>... configOverrides )
    {
        Config config = getConfig( configFile, configOverrides );
        try
        {
            life.add( new Neo4jJobScheduler() );

            new ClusterClientModule(
                    life,
                    new Dependencies(),
                    new Monitors(),
                    config,
                    logService( new DefaultFileSystemAbstraction(), config ),
                    new NotElectableElectionCredentialsProvider() );
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
        addShutdownHook();
        life.start();

        return 0;
    }

    @Override
    public int stop()
    {
        life.shutdown();
        return 0;
    }

    @Override
    public void close()
    {
        stop();
    }

    @SafeVarargs
    private static Config getConfig( Optional<File> configFile, Pair<String, String>... configOverrides )
    {
        Map<String, String> config = new HashMap<>();
        configFile.ifPresent( (file) -> {
            try
            {
                config.putAll( MapUtil.load( file ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to load config file " + configFile, e );
            }
        } );

        for ( Pair<String, String> configOverride : configOverrides )
        {
            config.put( configOverride.first(), configOverride.other() );
        }

        verifyConfig( config );
        return new Config( config );
    }

    private static void verifyConfig( Map<String, String> config )
    {
        if ( !config.containsKey( ClusterSettings.initial_hosts.name() ) )
        {
            throw new IllegalArgumentException( "No initial hosts to connect to supplied" );
        }
        if ( !config.containsKey( ClusterSettings.server_id.name() ) )
        {
            throw new IllegalArgumentException( "No server id specified" );
        }
    }

    private static LogService logService( FileSystemAbstraction fileSystem, Config config )
    {
        File logDir = config.get( GraphDatabaseSettings.logs_directory );
        try
        {
            return StoreLogService.withUserLogProvider( FormattedLogProvider.toOutputStream( System.out ) )
                    .inLogsDirectory( fileSystem, logDir );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void addShutdownHook()
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
                }, 4_000L );
                ArbiterBootstrapper.this.stop();
            }
        } );
    }
}
