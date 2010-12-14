/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.rest;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.webadmin.console.GremlinSession;
import org.neo4j.server.webadmin.console.ScriptSession;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

public class ConsoleServiceTest implements SessionFactory
{
    public ConsoleService consoleService;

    @Test
    public void retrievesTheReferenceNode() throws UnsupportedEncodingException
    {
        Response evaluatedGremlinResponse = consoleService.exec( new JsonFormat(),
                "{ \"command\" : \"$_\" }" );

        assertEquals( 200, evaluatedGremlinResponse.getStatus() );
        String response = decode( evaluatedGremlinResponse );
        assertThat( response, containsString( "v[0]" ) );
    }

    private String decode( final Response evaluatedGremlinResponse )
            throws UnsupportedEncodingException
    {
        return new String( (byte[])evaluatedGremlinResponse.getEntity(), "UTF-8" );
    }

    @Test
    public void canCreateNodesInGremlinLand() throws UnsupportedEncodingException
    {
        Response evaluatedGremlinResponse = consoleService.exec( new JsonFormat(),
                "{ \"command\" : \"g:add-v()\" }" );

        assertEquals( 200, evaluatedGremlinResponse.getStatus() );
        String response = decode( evaluatedGremlinResponse );
        assertThat( response, containsString( "v[1]" ) );

        evaluatedGremlinResponse = consoleService.exec( new JsonFormat(), "{ \"command\" : \"g:add-v()\" }" );
        response = decode( evaluatedGremlinResponse );
        assertEquals( 200, evaluatedGremlinResponse.getStatus() );
        assertThat( response, containsString( "v[2]" ) );
    }

    @Test
    public void correctRepresentation() throws URISyntaxException, UnsupportedEncodingException
    {
        URI uri = new URI( "http://peteriscool.com:6666/" );
        UriInfo mockUri = new FakeUriInfo( uri );
        Response consoleResponse = consoleService.getServiceDefinition( mockUri );

        assertEquals( 200, consoleResponse.getStatus() );
        String response = decode( consoleResponse );
        assertThat( response, containsString( "resources" ) );
        assertThat( response, containsString( uri.toString() ) );
    }

    @Before
    public void setUp() throws Exception
    {
        Database database = new Database( new ImpermanentGraphDatabase() );
        this.consoleService = new ConsoleService( this, database, new OutputFormat(
                new JsonFormat(), null, null ) );
    }

    @Override
    public ScriptSession createSession( String engineName, Database database )
    {
        return new GremlinSession( database );
    }
}
