/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest;

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

public class GetNodePropertiesDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalDocTestBase
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
        org.neo4j.doc.server.rest.JaxRsResponse
                createResponse = req.post( functionalTestHelper.dataUri() + "node/", entity);
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
        org.neo4j.doc.server.rest.JaxRsResponse
                createResponse = request.post( functionalTestHelper.dataUri() + "node/", entity);
        org.neo4j.doc.server.rest.JaxRsResponse
                response = request.get( createResponse.getLocation().toString() + "/properties");
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
        org.neo4j.doc.server.rest.JaxRsResponse
                createResponse = request.post( functionalTestHelper.dataUri() + "node/", entity);
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
        org.neo4j.doc.server.rest.JaxRsResponse
                createResponse = request.post( functionalTestHelper.dataUri() + "node/", entity);
        String response = (String) JsonHelper.readJson( request.get( getPropertyUri( createResponse.getLocation()
                .toString(), "foo" ), new MediaType( "application", "json", stringMap( "stream", "true" ) ) ).getEntity() );
        assertEquals( complicatedString, response );
    }

    @Test
    public void shouldGet404ForPropertiesOnNonExistentNode() {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().get( functionalTestHelper.dataUri() + "node/999999/properties");
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertiesResponse() throws JsonParseException
    {
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        org.neo4j.doc.server.rest.JaxRsResponse
                createResource = req.post( functionalTestHelper.dataUri() + "node/", entity);
        org.neo4j.doc.server.rest.JaxRsResponse
                response = req.get( createResource.getLocation().toString() + "/properties");
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
    }

    @Test
    public void shouldGet404ForNoProperty()
    {
        final org.neo4j.doc.server.rest.JaxRsResponse
                createResponse = req.post( functionalTestHelper.dataUri() + "node/", "");
        org.neo4j.doc.server.rest.JaxRsResponse
                response = req.get(getPropertyUri(createResponse.getLocation().toString(), "foo"));
        assertEquals(404, response.getStatus());
    }

    @Documented( "Get property for node.\n" +
                 "\n" +
                 "Get a single node property from a node." )
    @Test
    public void shouldGet200ForProperty() throws JsonParseException
    {
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        org.neo4j.doc.server.rest.JaxRsResponse
                createResponse = req.post( functionalTestHelper.dataUri() + "node/", entity);
        org.neo4j.doc.server.rest.JaxRsResponse
                response = req.get(getPropertyUri(createResponse.getLocation().toString(), "foo"));
        assertEquals(200, response.getStatus());

        gen.get()
                .expectedStatus( 200 )
                .get(getPropertyUri(createResponse.getLocation()
                        .toString(), "foo"));
    }

    @Test
    public void shouldGet404ForPropertyOnNonExistentNode() {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().get(getPropertyUri( functionalTestHelper.dataUri() + "node/" + "999999", "foo"));
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertyResponse() throws JsonParseException {
        String entity = JsonHelper.createJsonFrom( Collections.singletonMap( "foo", "bar" ) );

        org.neo4j.doc.server.rest.JaxRsResponse
                createResponse = req.post( functionalTestHelper.dataUri() + "node/", entity);

        org.neo4j.doc.server.rest.JaxRsResponse
                response = req.get(getPropertyUri(createResponse.getLocation().toString(), "foo"));

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
