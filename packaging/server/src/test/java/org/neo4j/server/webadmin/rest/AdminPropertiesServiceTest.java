/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;

public class AdminPropertiesServiceTest {
    @Test
    public void simpleQueryShouldReturn200AndTheExpectedValue() throws Exception
    {
        UriInfo mockUri = mock( UriInfo.class );
        when( mockUri.getBaseUri() ).thenReturn( new URI( "http://peteriscool.com:6666/" ) );
        
        NeoServer mockServer = mock(NeoServer.class);
        when(mockServer.managementApiUri()).thenReturn(new URI( "http://peteriscool.com:6666/management" ));
        when(mockServer.restApiUri()).thenReturn(new URI( "http://peteriscool.com:6666/data" ));
        
        Configuration dummyConfig = mock(Configuration.class);
        when(dummyConfig.getProperty("foo")).thenReturn("bar");
        when(dummyConfig.containsKey("foo")).thenReturn(true);
        
        when(mockServer.getConfiguration()).thenReturn(dummyConfig);

        AdminPropertiesService adminPropertiesService = new AdminPropertiesService( mockUri,
                mockServer, new OutputFormat( new JsonFormat(), new URI( "http://peteriscool.com:6666/" ), null ) );

        Response response = adminPropertiesService.getValue( "foo" );
        assertThat( response.getStatus(), is( 200 ) );
        assertThat( new String( (byte[]) response.getEntity(), "UTF-8" ), containsString( "bar" ) );
    }

    @Test
    public void shouldSupportLegacyWebAdminUris() throws URISyntaxException, JsonParseException, UnsupportedEncodingException {
        UriInfo mockUri = mock(UriInfo.class);
        URI baseUri = new URI("http://peteriscool.com:6666/foo/bar?awesome=true");
        when(mockUri.getBaseUri()).thenReturn(baseUri);
        
        NeoServer mockServer = mock(NeoServer.class);
        when(mockServer.managementApiUri()).thenReturn(new URI( "http://peteriscool.com:6666/management" ));
        when(mockServer.restApiUri()).thenReturn(new URI( "http://peteriscool.com:6666/data" ));

        AdminPropertiesService adminPropertiesService = new AdminPropertiesService(mockUri, mockServer, new OutputFormat(new JsonFormat(), baseUri, null));

        Response response = adminPropertiesService.getValue("neo4j-servers");

        String entity = new String((byte[]) response.getEntity(), "UTF-8");
        assertIsValidJson(entity);
        assertThat(entity, containsString(mockServer.managementApiUri().toString()));
        assertThat(entity, containsString(mockServer.restApiUri().toString()));
    }

    private void assertIsValidJson(String entity) throws JsonParseException {
        JsonHelper.jsonToMap(entity);
    }

    @Test
    public void shouldYieldUndefinedForUnknownProperties() throws URISyntaxException, UnsupportedEncodingException {
        UriInfo mockUri = mock(UriInfo.class);
        URI baseUri = new URI("http://peteriscool.com:6666/");
        when(mockUri.getBaseUri()).thenReturn(baseUri);
        
        NeoServer mockServer = mock(NeoServer.class);
        when(mockServer.managementApiUri()).thenReturn(new URI( "http://peteriscool.com:6666/management" ));
        when(mockServer.restApiUri()).thenReturn(new URI( "http://peteriscool.com:6666/data" ));
        
        Configuration dummyConfig = mock(Configuration.class);
        when(mockServer.getConfiguration()).thenReturn(dummyConfig);

        AdminPropertiesService adminPropertiesService = new AdminPropertiesService(mockUri, mockServer, new OutputFormat(new JsonFormat(), baseUri, null));

        Response response = adminPropertiesService.getValue("foo");

        assertThat(new String((byte[]) response.getEntity(), "UTF-8"), containsString("undefined"));
    }
}
