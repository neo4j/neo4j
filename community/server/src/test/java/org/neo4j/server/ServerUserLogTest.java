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

import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.helpers.Strings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.database.CommunityGraphFactory;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.web.WebServer;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_user_log_to_stdout;

public class ServerUserLogTest
{
    @Rule
    public final SuppressOutput suppress = SuppressOutput.suppress( SuppressOutput.System.out );

    @Rule
    public TestDirectory homeDir = TestDirectory.testDirectory();

    @Test
    public void shouldLogToStdOutByDefault() throws Exception
    {
        // given
        ServerBootstrapper serverBootstrapper = getServerBootstrapper();
        File dir = homeDir.directory();

        // when
        serverBootstrapper.start( dir, Optional.empty(), MapUtil.stringMap( database_path.name(), homeDir.absolutePath().getAbsolutePath() ) );

        // then no exceptions are thrown and
        assertThat( getStdOut(), not( empty() ) );
        assertTrue( !Files.exists( getUserLogFileLocation( dir ) ) );

        // stop the server so that resources are released and test teardown isn't flaky
        serverBootstrapper.stop();
    }

    @Test
    public void shouldLogToFileWhenConfigured() throws Exception
    {
        // given
        ServerBootstrapper serverBootstrapper = getServerBootstrapper();
        File dir = homeDir.directory();

        // when
        serverBootstrapper.start( dir, Optional.empty(),
                MapUtil.stringMap(
                        database_path.name(), homeDir.absolutePath().getAbsolutePath(),
                        store_user_log_to_stdout.name(), "false"
                )
        );

        // then no exceptions are thrown and
        assertThat( getStdOut(), empty() );
        assertTrue( Files.exists( getUserLogFileLocation( dir ) ) );
        assertThat( readUserLogFile( dir ), not( empty() ) );

        // stop the server so that resources are released and test teardown isn't flaky
        serverBootstrapper.stop();
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
                return new CommunityGraphFactory();
            }

            @Override
            protected NeoServer createNeoServer( GraphFactory graphFactory, Config config, GraphDatabaseDependencies dependencies )
            {
                return new AbstractNeoServer( config, graphFactory, dependencies )
                {
                    @Override
                    protected Iterable<ServerModule> createServerModules()
                    {
                        return new ArrayList<>( 0 );
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
}
