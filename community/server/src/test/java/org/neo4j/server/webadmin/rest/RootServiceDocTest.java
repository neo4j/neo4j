/*
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

import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.junit.Test;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.rest.management.RootService;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.test.server.EntityOutputFormat;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RootServiceDocTest
{
    @Test
    public void shouldAdvertiseServicesWhenAsked() throws Exception
    {
        UriInfo uriInfo = mock( UriInfo.class );
        URI uri = new URI( "http://example.org:7474/" );
        when( uriInfo.getBaseUri() ).thenReturn( uri );

        ConfigurationBuilder configBuilder = mock( ConfigurationBuilder.class );
        when(configBuilder.configuration()).thenReturn( new Config() );

        RootService svc = new RootService( new CommunityNeoServer( configBuilder,
                GraphDatabaseDependencies.newDependencies().logging(DevNullLoggingService.DEV_NULL).monitors(new Monitors())) );

        EntityOutputFormat output = new EntityOutputFormat( new JsonFormat(), null, null );
        Response serviceDefinition = svc.getServiceDefinition( uriInfo, output );

        assertEquals( 200, serviceDefinition.getStatus() );
        Map<String, Object> result = (Map<String, Object>) output.getResultAsMap()
                .get( "services" );

        assertThat( result.get( "console" )
                .toString(), containsString( String.format( "%sserver/console", uri.toString() ) ) );
        assertThat( result.get( "jmx" )
                .toString(), containsString( String.format( "%sserver/jmx", uri.toString() ) ) );
        assertThat( result.get( "monitor" )
                .toString(), containsString( String.format( "%sserver/monitor", uri.toString() ) ) );
    }
}
