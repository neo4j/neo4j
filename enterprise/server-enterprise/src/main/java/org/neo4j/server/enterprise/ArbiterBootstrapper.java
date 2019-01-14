/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.enterprise;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
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
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemLifecycleAdapter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.server.Bootstrapper;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.helpers.Exceptions.peel;

public class ArbiterBootstrapper implements Bootstrapper, AutoCloseable
{
    private final LifeSupport life = new LifeSupport();
    private final Timer timer = new Timer( true );

    @Override
    public final int start( File homeDir, Optional<File> configFile, Map<String, String> configOverrides )
    {
        Config config = getConfig( configFile, configOverrides );
        try
        {
            DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
            life.add( new FileSystemLifecycleAdapter( fileSystem ) );
            life.add( new CentralJobScheduler() );
            new ClusterClientModule(
                    life,
                    new Dependencies(),
                    new Monitors(),
                    config,
                    logService( fileSystem, config ),
                    new NotElectableElectionCredentialsProvider() );
        }
        catch ( LifecycleException e )
        {
            @SuppressWarnings( {"ThrowableResultOfMethodCallIgnored", "unchecked"} )
            Throwable cause = peel( e, Predicates.instanceOf( LifecycleException.class ) );
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

    private static Config getConfig( Optional<File> configFile, Map<String, String> configOverrides )
    {
        Config config = Config.builder().withFile( configFile ).withSettings( configOverrides ).build();
        verifyConfig( config.getRaw() );
        return config ;
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
        File logFile = config.get( store_internal_log_path );
        try
        {
            ZoneId zoneId = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
            FormattedLogProvider logProvider = FormattedLogProvider.withZoneId( zoneId ).toOutputStream( System.out );
            return StoreLogService.withUserLogProvider( logProvider )
                    .withInternalLog( logFile )
                    .build( fileSystem );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void addShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
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
        } ) );
    }
}
