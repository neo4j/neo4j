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
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappedDatabase;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.management.console.ConsoleService;
import org.neo4j.server.rest.management.console.ShellSession;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.webadmin.console.ConsoleSessionFactory;
import org.neo4j.server.webadmin.console.ScriptSession;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.System.lineSeparator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

public class Neo4jShellConsoleSessionDocTest implements ConsoleSessionFactory
{
    private ConsoleService consoleService;
    private Database database;
    private final URI uri = URI.create( "http://peteriscool.com:6666/" );

    @Before
    public void setUp() throws Exception
    {
        this.database = new WrappedDatabase( (GraphDatabaseAPI) new TestGraphDatabaseFactory().
                newImpermanentDatabaseBuilder().
                setConfig( ShellSettings.remote_shell_enabled, Settings.TRUE ).
                newGraphDatabase() );
        this.consoleService = new ConsoleService(
                this,
                database,
                NullLogProvider.getInstance(),
                new OutputFormat( new JsonFormat(), uri, null ) );
    }

    @After
    public void shutdownDatabase()
    {
        this.database.getGraph().shutdown();
    }

    @Override
    public ScriptSession createSession( String engineName, Database database, LogProvider logProvider )
    {
        return new ShellSession( database.getGraph() );
    }

    @Test
    public void doesntMangleNewlines() throws Exception
    {
        Response response = consoleService.exec( new JsonFormat(),
                "{ \"command\" : \"create (n) return n;\", \"engine\":\"shell\" }" );


        assertEquals( 200, response.getStatus() );
        String result = decode( response ).get( 0 );

        String expected = "+-----------+" + lineSeparator()
                + "| n         |" + lineSeparator()
                + "+-----------+" + lineSeparator()
                + "| Node[0]{} |" + lineSeparator()
                + "+-----------+" + lineSeparator()
                + "1 row";

        assertThat( result, containsString( expected ) );
    }

    private List<String> decode( final Response response ) throws UnsupportedEncodingException, JsonParseException
    {
        return (List<String>) JsonHelper.readJson( new String( (byte[]) response.getEntity(), "UTF-8" ) );
    }

    @Override
    public Iterable<String> supportedEngines()
    {
        return new ArrayList<String>()
        {{
                add( "shell" );
            }};
    }
}
