/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.server.database.CommunityGraphFactory;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.web.WebServer;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_user_log_max_archives;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_user_log_rotation_delay;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_user_log_rotation_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_user_log_to_stdout;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.ServerBootstrapper.OK;

public class ServerUserLogTest
{
    @Rule
    public final SuppressOutput suppress = SuppressOutput.suppress( SuppressOutput.System.out );

    @Rule
    public TestDirectory homeDir = TestDirectory.testDirectory();

    @Test
    public void shouldLogToStdOutByDefault()
    {
        // given
        ServerBootstrapper serverBootstrapper = getServerBootstrapper();
        File dir = homeDir.directory();
        Log logBeforeStart = serverBootstrapper.getLog();

        // when
        try
        {
            int returnCode = serverBootstrapper.start( dir, Optional.empty(),
                    stringMap(
                            database_path.name(), homeDir.absolutePath().getAbsolutePath()
                    )
            );

            // then no exceptions are thrown and
            assertEquals( OK, returnCode );
            assertTrue( serverBootstrapper.getServer().getDatabase().isRunning() );
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
    public void shouldLogToFileWhenConfigured() throws Exception
    {
        // given
        ServerBootstrapper serverBootstrapper = getServerBootstrapper();
        File dir = homeDir.directory();
        Log logBeforeStart = serverBootstrapper.getLog();

        // when
        try
        {
            int returnCode = serverBootstrapper.start( dir, Optional.empty(),
                    stringMap(
                            database_path.name(), homeDir.absolutePath().getAbsolutePath(),
                            store_user_log_to_stdout.name(), "false"
                    )
            );
            // then no exceptions are thrown and
            assertEquals( OK, returnCode );
            assertTrue( serverBootstrapper.getServer().getDatabase().isRunning() );
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
    public void logShouldRotateWhenConfigured() throws Exception
    {
        // given
        ServerBootstrapper serverBootstrapper = getServerBootstrapper();
        File dir = homeDir.directory();
        Log logBeforeStart = serverBootstrapper.getLog();
        int maxArchives = 4;

        // when
        try
        {
            int returnCode = serverBootstrapper.start( dir, Optional.empty(),
                    stringMap(
                            database_path.name(), homeDir.absolutePath().getAbsolutePath(),
                            store_user_log_to_stdout.name(), "false",
                            store_user_log_rotation_delay.name(), "0",
                            store_user_log_rotation_threshold.name(), "16",
                            store_user_log_max_archives.name(), Integer.toString( maxArchives )
                    )
            );

            // then
            assertEquals( OK, returnCode );
            assertThat( serverBootstrapper.getLog(), not( sameInstance( logBeforeStart ) ) );
            assertTrue( serverBootstrapper.getServer().getDatabase().isRunning() );

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

    private List<String> getStdOut()
    {
        List<String> lines = suppress.getOutputVoice().lines();
        // Remove empty lines
        return lines.stream().filter( line -> !line.equals( "" ) ).collect( Collectors.toList() );
    }

    private ServerBootstrapper getServerBootstrapper()
    {
        return new ServerBootstrapper()
        {
            @Override
            protected GraphFactory createGraphFactory( Config config )
            {
                config.augment( stringMap(
                        "dbms.connector.bolt.listen_address", ":0",
                        "dbms.connector.http.listen_address", ":0",
                        "dbms.connector.https.listen_address", ":0"
                ) );
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

                    @Override
                    public Iterable<AdvertisableService> getServices()
                    {
                        return new ArrayList<>( 0 );
                    }
                };
            }
        };
    }

    private List<String> readUserLogFile( File homeDir ) throws IOException
    {
        return Files.readAllLines( getUserLogFileLocation( homeDir ) ).stream().filter( line -> !line.equals( "" ) ).collect( Collectors.toList() );
    }

    private Path getUserLogFileLocation( File homeDir )
    {
        return Paths.get( homeDir.getAbsolutePath(), "logs", "neo4j.log" );
    }

    private List<String> allUserLogFiles( File homeDir ) throws IOException
    {
        try ( Stream<String> stream = Files.list( Paths.get( homeDir.getAbsolutePath(), "logs" ) )
                .map( x -> x.getFileName().toString() )
                .filter( x -> x.contains( "neo4j.log" ) ) )
        {
            return stream.collect( Collectors.toList() );
        }
    }
}
