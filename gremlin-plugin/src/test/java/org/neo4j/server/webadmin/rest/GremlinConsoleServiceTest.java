/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappingDatabase;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.webadmin.console.ConsoleSessionFactory;
import org.neo4j.server.webadmin.console.GremlinSession;
import org.neo4j.server.webadmin.console.ScriptSession;
import org.neo4j.server.webadmin.rest.console.ConsoleService;
import org.neo4j.test.ImpermanentGraphDatabase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

public class GremlinConsoleServiceTest implements ConsoleSessionFactory
{
    private ConsoleService consoleService;
    private Database database;
    private final URI uri = URI.create( "http://peteriscool.com:6666/" );

    @Test
    public void retrievesTheReferenceNode() throws UnsupportedEncodingException
    {
        Response evaluatedGremlinResponse = consoleService.exec( new JsonFormat(), "{ \"command\" : \"g.addVertex(null);g.v(1)\" }" );

        assertEquals( 200, evaluatedGremlinResponse.getStatus() );
        String response = decode( evaluatedGremlinResponse );
        MatcherAssert.assertThat(response, Matchers.containsString("v[1]"));
    }

    private String decode( final Response response ) throws UnsupportedEncodingException
    {
        return new String( (byte[]) response.getEntity(), "UTF-8" );
    }

    @Test
    public void canCreateNodesInGremlinLand() throws UnsupportedEncodingException
    {
        Response evaluatedGremlinResponse = consoleService.exec( new JsonFormat(),
                "{ \"command\" : \"g.addVertex(null)\" }" );

        assertEquals( 200, evaluatedGremlinResponse.getStatus() );
        String response = decode( evaluatedGremlinResponse );
        MatcherAssert.assertThat(response, Matchers.containsString("v[1]"));

        evaluatedGremlinResponse = consoleService.exec( new JsonFormat(), "{ \"command\" : \"g.addVertex(null)\" }" );
        response = decode( evaluatedGremlinResponse );
        assertEquals( 200, evaluatedGremlinResponse.getStatus() );
        MatcherAssert.assertThat(response, Matchers.containsString("v[2]"));
    }
    
    @Test
    public void advertisesAvailableConsoleEngines() throws URISyntaxException, UnsupportedEncodingException
    {
        
        ConsoleService consoleServiceWithShellAndGremlin = new ConsoleService( new GremlinAndShellConsoleSessionFactory(), database, new OutputFormat( new JsonFormat(), uri, null ) );
        
        String response = decode( consoleServiceWithShellAndGremlin.getServiceDefinition());

        assertThat( response, containsString( "\"engines\" : [ \"shell\", \"gremlin\" ]" ) );
    }
    
    
    private static class GremlinAndShellConsoleSessionFactory implements ConsoleSessionFactory
    {
        @Override
        public ScriptSession createSession(String engineName, Database database)
        {
            return null;
        }
    
        @Override
        public Iterable<String> supportedEngines()
        {
            return new ArrayList<String>(){{
                add("shell");
                add("gremlin");
            }};
        }
    }
    
    @Before
    public void setUp() throws Exception
    {
        this.database = new WrappingDatabase( new ImpermanentGraphDatabase() );
        this.consoleService = new ConsoleService( this, database, new OutputFormat( new JsonFormat(), uri, null ) );
    }

    @After
    public void shutdownDatabase()
    {
        this.database.getGraph().shutdown();
    }

    @Override
    public ScriptSession createSession( String engineName, Database database )
    {
        return new GremlinSession( database );
    }
    
    @Override
    public Iterable<String> supportedEngines()
    {
        return new ArrayList<String>(){{
            add("gremlin");
        }};
    }
}
