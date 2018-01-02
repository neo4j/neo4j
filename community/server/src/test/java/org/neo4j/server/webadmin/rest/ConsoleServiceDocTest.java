/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.webadmin.rest;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import javax.ws.rs.core.Response;

import org.junit.Test;

import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.webadmin.console.ConsoleSessionFactory;
import org.neo4j.server.webadmin.console.ScriptSession;
import org.neo4j.server.rest.management.console.ConsoleService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class ConsoleServiceDocTest
{
    private final URI uri = URI.create( "http://peteriscool.com:6666/" );

    @Test
    public void correctRepresentation() throws URISyntaxException, UnsupportedEncodingException
    {
        ConsoleService consoleService = new ConsoleService( new ShellOnlyConsoleSessionFactory(), mock( Database.class ),
                NullLogProvider.getInstance(), new OutputFormat( new JsonFormat(), uri, null ) );

        Response consoleResponse = consoleService.getServiceDefinition();

        assertEquals( 200, consoleResponse.getStatus() );
        String response = decode( consoleResponse );
        assertThat( response, containsString( "resources" ) );
        assertThat( response, containsString( uri.toString() ) );
    }

    @Test
    public void advertisesAvailableConsoleEngines() throws URISyntaxException, UnsupportedEncodingException
    {
        ConsoleService consoleServiceWithJustShellEngine = new ConsoleService( new ShellOnlyConsoleSessionFactory(),
                mock( Database.class ), NullLogProvider.getInstance(), new OutputFormat( new JsonFormat(), uri, null ) );

        String response = decode( consoleServiceWithJustShellEngine.getServiceDefinition());

        assertThat( response, containsString( "\"engines\" : [ \"shell\" ]" ) );

    }

    private String decode( final Response response ) throws UnsupportedEncodingException
    {
        return new String( (byte[]) response.getEntity(), "UTF-8" );
    }

    private static class ShellOnlyConsoleSessionFactory implements ConsoleSessionFactory
    {
        @Override
        public ScriptSession createSession( String engineName, Database database, LogProvider logProvider )
        {
            return null;
        }

        @Override
        public Iterable<String> supportedEngines()
        {
            return new ArrayList<String>(){{
                add("shell");
            }};
        }
    }
}
