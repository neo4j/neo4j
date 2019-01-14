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
package org.neo4j.server.web;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class Jetty9WebServerIT extends ExclusiveServerTestBase
{
    @Rule
    public SuppressOutput suppressOutput = suppressAll();
    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private Jetty9WebServer webServer;

    @Test
    public void shouldBeAbleToUsePortZero() throws Exception
    {
        // Given
        webServer = new Jetty9WebServer( NullLogProvider.getInstance(), Config.defaults() );

        webServer.setAddress( new ListenSocketAddress( "localhost", 0 ) );

        // When
        webServer.start();

        // Then no exception
    }

    @Test
    public void shouldBeAbleToRestart() throws Throwable
    {
        // given
        webServer = new Jetty9WebServer( NullLogProvider.getInstance(), Config.defaults() );
        webServer.setAddress( new ListenSocketAddress( "127.0.0.1", 7878 ) );

        // when
        webServer.start();
        webServer.stop();
        webServer.start();

        // then no exception
    }

    @Test
    public void shouldStopCleanlyEvenWhenItHasntBeenStarted()
    {
        new Jetty9WebServer( NullLogProvider.getInstance(), Config.defaults() ).stop();
    }

    @After
    public void cleanup()
    {
        if ( webServer != null )
        {
            webServer.stop();
        }
    }

}
