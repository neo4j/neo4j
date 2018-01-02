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
package org.neo4j.server.rest;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import javax.ws.rs.core.MediaType;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.formats.StreamingJsonFormat;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class GetNodePropertiesDocIT extends AbstractRestFunctionalDocTestBase
{
    private static FunctionalTestHelper functionalTestHelper;
    private RestRequest req = RestRequest.req();

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
    }

    @Documented( "Get properties for node." )
    @Test
    public void shouldGet200ForProperties() throws JsonParseException {
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        JaxRsResponse createResponse = req.post(functionalTestHelper.dataUri() + "node/", entity);
        gen.get()
                .expectedStatus(200)
                .get(createResponse.getLocation()
                        .toString() + "/properties");
    }

    @Test
    public void shouldGetContentLengthHeaderForRetrievingProperties() throws JsonParseException
    {
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        final RestRequest request = req;
        JaxRsResponse createResponse = request.post(functionalTestHelper.dataUri() + "node/", entity);
        JaxRsResponse response = request.get(createResponse.getLocation().toString() + "/properties");
        assertNotNull( response.getHeaders().get("Content-Length") );
    }

    @Test
    public void shouldGetCorrectContentEncodingRetrievingProperties() throws JsonParseException
    {
        String asianText = "\u4f8b\u5b50";
        String germanText = "öäüÖÄÜß";

        String complicatedString = asianText + germanText;


        String entity = JsonHelper.createJsonFrom( Collections.singletonMap( "foo", complicatedString ));
        final RestRequest request = req;
        JaxRsResponse createResponse = request.post(functionalTestHelper.dataUri() + "node/", entity);
        String response = (String) JsonHelper.readJson( request.get( getPropertyUri( createResponse.getLocation()
                .toString(), "foo" ) ).getEntity() );
        assertEquals( complicatedString, response );
    }
    @Test
    public void shouldGetCorrectContentEncodingRetrievingPropertiesWithStreaming() throws JsonParseException
    {
        String asianText = "\u4f8b\u5b50";
        String germanText = "öäüÖÄÜß";

        String complicatedString = asianText + germanText;

        String entity = JsonHelper.createJsonFrom( Collections.singletonMap( "foo", complicatedString ) );
        final RestRequest request = req.header( StreamingJsonFormat.STREAM_HEADER,"true");
        JaxRsResponse createResponse = request.post(functionalTestHelper.dataUri() + "node/", entity);
        String response = (String) JsonHelper.readJson( request.get( getPropertyUri( createResponse.getLocation()
                .toString(), "foo" ), new MediaType( "application", "json", stringMap( "stream", "true" ) ) ).getEntity() );
        assertEquals( complicatedString, response );
    }

    @Test
    public void shouldGet404ForPropertiesOnNonExistentNode() {
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.dataUri() + "node/999999/properties");
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertiesResponse() throws JsonParseException
    {
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        JaxRsResponse createResource = req.post(functionalTestHelper.dataUri() + "node/", entity);
        JaxRsResponse response = req.get(createResource.getLocation().toString() + "/properties");
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
    }

    @Test
    public void shouldGet404ForNoProperty()
    {
        final JaxRsResponse createResponse = req.post(functionalTestHelper.dataUri() + "node/", "");
        JaxRsResponse response = req.get(getPropertyUri(createResponse.getLocation().toString(), "foo"));
        assertEquals(404, response.getStatus());
    }

    @Documented( "Get property for node.\n" +
                 "\n" +
                 "Get a single node property from a node." )
    @Test
    public void shouldGet200ForProperty() throws JsonParseException
    {
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        JaxRsResponse createResponse = req.post(functionalTestHelper.dataUri() + "node/", entity);
        JaxRsResponse response = req.get(getPropertyUri(createResponse.getLocation().toString(), "foo"));
        assertEquals(200, response.getStatus());

        gen.get()
                .expectedStatus( 200 )
                .get(getPropertyUri(createResponse.getLocation()
                        .toString(), "foo"));
    }

    @Test
    public void shouldGet404ForPropertyOnNonExistentNode() {
        JaxRsResponse response = RestRequest.req().get(getPropertyUri(functionalTestHelper.dataUri() + "node/" + "999999", "foo"));
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertyResponse() throws JsonParseException {
        String entity = JsonHelper.createJsonFrom( Collections.singletonMap( "foo", "bar" ) );

        JaxRsResponse createResponse = req.post(functionalTestHelper.dataUri() + "node/", entity);

        JaxRsResponse response = req.get(getPropertyUri(createResponse.getLocation().toString(), "foo"));

        assertThat( response.getType().toString(), containsString(MediaType.APPLICATION_JSON) );

        createResponse.close();
        response.close();
    }

    @Test
    public void shouldReturnEmptyMapForEmptyProperties() throws Exception
    {
        // Given
        String location = HTTP.POST( server().baseUri().resolve( "db/data/node" ).toString() ).location();

        // When
        HTTP.Response res = HTTP.GET( location + "/properties" );

        // Then
        assertThat(res.rawContent(), equalTo("{ }"));
    }

    private String getPropertyUri( final String baseUri, final String key )
    {
        return baseUri + "/properties/" + key;
    }
}
