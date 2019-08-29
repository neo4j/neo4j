/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.logging.Log;
import org.neo4j.server.database.CommunityGraphFactory;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.web.WebServer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.store_user_log_max_archives;
import static org.neo4j.configuration.GraphDatabaseSettings.store_user_log_rotation_delay;
import static org.neo4j.configuration.GraphDatabaseSettings.store_user_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.store_user_log_to_stdout;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.ServerBootstrapper.OK;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class ServerUserLogTest
{
    @Inject
    private SuppressOutput suppress;
    @Inject
    private TestDirectory homeDir;

    @Test
    void shouldLogToStdOutByDefault()
    {
        // given
        ServerBootstrapper serverBootstrapper = getServerBootstrapper();
        File dir = homeDir.directory();
        Log logBeforeStart = serverBootstrapper.getLog();

        // when
        try
        {
            int returnCode = serverBootstrapper.start( dir, Optional.empty(), connectorsConfig() );

            // then no exceptions are thrown and
            assertThat( getStdOut(), not( empty() ) );
            assertFalse( Files.exists( getUserLogFileLocation( dir ) ) );

            // then no exceptions are thrown and
            assertEquals( OK, returnCode );
            assertTrue( serverBootstrapper.getServer().getDatabaseService().isRunning() );
            assertThat( serverBootstrapper.getLog(), not( sameInstance( logBeforeStart ) ) );

            assertThat( getStdOut(), not( empty() ) );
            assertThat( getStdOut(), hasItem(containsString( "Started." ) ) );
        }
        finally
        {
            // stop the server so that resources are released and test teardown isn't flaky
            serverBootstrapper.stop();
        }
        assertFalse( Files.exists( getUserLogFileLocation( dir ) ) );
    }

    @Test
    void shouldLogToFileWhenConfigured() throws Exception
    {
        // given
        ServerBootstrapper serverBootstrapper = getServerBootstrapper();
        File dir = homeDir.directory();
        Log logBeforeStart = serverBootstrapper.getLog();

        // when
        try
        {
            Map<String,String> configOverrides = stringMap( store_user_log_to_stdout.name(), FALSE );
            configOverrides.putAll( connectorsConfig() );
            int returnCode = serverBootstrapper.start( dir, Optional.empty(), configOverrides );
            // then no exceptions are thrown and
            assertEquals( OK, returnCode );
            assertTrue( serverBootstrapper.getServer().getDatabaseService().isRunning() );
            assertThat( serverBootstrapper.getLog(), not( sameInstance( logBeforeStart ) ) );

        }
        finally
        {
            // stop the server so that resources are released and test teardown isn't flaky
            serverBootstrapper.stop();
        }
        assertThat( getStdOut(), empty() );
        assertTrue( Files.exists( getUserLogFileLocation( dir ) ) );
        assertThat( readUserLogFile( dir ), not( empty() ) );
        assertThat( readUserLogFile( dir ), hasItem(containsString( "Started." ) ) );
    }

    @Test
    void logShouldRotateWhenConfigured() throws Exception
    {
        // given
        ServerBootstrapper serverBootstrapper = getServerBootstrapper();
        File dir = homeDir.directory();
        Log logBeforeStart = serverBootstrapper.getLog();
        int maxArchives = 4;

        // when
        try
        {
            Map<String,String> configOverrides = stringMap( store_user_log_to_stdout.name(), FALSE,
                            store_user_log_rotation_delay.name(), "0",
                            store_user_log_rotation_threshold.name(), "16",
                            store_user_log_max_archives.name(), Integer.toString( maxArchives ) );
            configOverrides.putAll( connectorsConfig() );
            int returnCode = serverBootstrapper.start( dir, Optional.empty(),
                    configOverrides
            );

            // then
            assertEquals( OK, returnCode );
            assertThat( serverBootstrapper.getLog(), not( sameInstance( logBeforeStart ) ) );
            assertTrue( serverBootstrapper.getServer().getDatabaseService().isRunning() );

            // when we forcibly log some more stuff
            do
            {
                serverBootstrapper.getLog().info( "testing 123. This string should contain more than 16 bytes\n" );
                Thread.sleep( 2000 );
            }
            while ( allUserLogFiles( dir ).size() <= 4 );
        }
        finally
        {
            // stop the server so that resources are released and test teardown isn't flaky
            serverBootstrapper.stop();
        }

        // then no exceptions are thrown and
        assertThat( getStdOut(), empty() );
        assertTrue( Files.exists( getUserLogFileLocation( dir ) ) );
        assertThat( readUserLogFile( dir ), not( empty() ) );
        List<String> userLogFiles = allUserLogFiles( dir );
        assertThat( userLogFiles, containsInAnyOrder( "neo4j.log", "neo4j.log.1", "neo4j.log.2", "neo4j.log.3", "neo4j.log.4" ) );
        assertEquals( maxArchives + 1, userLogFiles.size() );
    }

    private static Map<String,String> connectorsConfig()
    {
        return Map.of( HttpConnector.listen_address.name(), "localhost:0" ,
                BoltConnector.listen_address.name(), "localhost:0",
                HttpsConnector.listen_address.name(), "localhost:0" );
    }

    private List<String> getStdOut()
    {
        List<String> lines = suppress.getOutputVoice().lines();
        // Remove empty lines
        return lines.stream().filter( line -> !line.equals( "" ) ).collect( Collectors.toList() );
    }

    private static ServerBootstrapper getServerBootstrapper()
    {
        return new ServerBootstrapper()
        {
            @Override
            protected GraphFactory createGraphFactory( Config config )
            {
                return new CommunityGraphFactory();
            }

            @Override
            protected NeoServer createNeoServer( GraphFactory graphFactory, Config config, GraphDatabaseDependencies dependencies )
            {
                dependencies.userLogProvider();
                return new AbstractNeoServer( config, graphFactory, dependencies )
                {
                    @Override
                    protected Iterable<ServerModule> createServerModules()
                    {
                        return new ArrayList<>( 0 );
                    }

                    @Override
                    protected void configureWebServer()
                    {
                    }

                    @Override
                    protected void startWebServer()
                    {
                    }

                    @Override
                    protected WebServer createWebServer()
                    {
                        return null;
                    }
                };
            }
        };
    }

    private static List<String> readUserLogFile( File homeDir ) throws IOException
    {
        return Files.readAllLines( getUserLogFileLocation( homeDir ) ).stream().filter( line -> !line.equals( "" ) ).collect( Collectors.toList() );
    }

    private static Path getUserLogFileLocation( File homeDir )
    {
        return Paths.get( homeDir.getAbsolutePath(), "logs", "neo4j.log" );
    }

    private static List<String> allUserLogFiles( File homeDir ) throws IOException
    {
        try ( Stream<String> stream = Files.list( Paths.get( homeDir.getAbsolutePath(), "logs" ) )
                .map( x -> x.getFileName().toString() )
                .filter( x -> x.contains( "neo4j.log" ) ) )
        {
            return stream.collect( Collectors.toList() );
        }
    }
}
