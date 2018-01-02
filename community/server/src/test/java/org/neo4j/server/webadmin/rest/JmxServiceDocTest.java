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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.rest.management.JmxService;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;

public class JmxServiceDocTest
{
    public JmxService jmxService;
    private final URI uri = URI.create( "http://peteriscool.com:6666/" );

    @Test
    public void correctRepresentation() throws URISyntaxException, UnsupportedEncodingException
    {
        Response resp = jmxService.getServiceDefinition();

        assertEquals( 200, resp.getStatus() );

        String json = new String( (byte[]) resp.getEntity(), "UTF-8" );
        assertThat( json, containsString( "resources" ) );
        assertThat( json, containsString( uri.toString() ) );
        assertThat( json, containsString( "jmx/domain/{domain}/{objectName}" ) );
    }

    @Test
    public void shouldListDomainsCorrectly() throws Exception
    {
        Response resp = jmxService.listDomains();

        assertEquals( 200, resp.getStatus() );
    }

    @Test
    public void testwork() throws Exception
    {
        jmxService.queryBeans( "[\"*:*\"]" );
    }

    @Before
    public void setUp() throws Exception
    {
        this.jmxService = new JmxService( new OutputFormat( new JsonFormat(), uri, null ), null );
    }

}
