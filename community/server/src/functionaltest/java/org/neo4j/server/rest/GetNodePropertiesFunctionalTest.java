/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collections;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.TestData;

public class GetNodePropertiesFunctionalTest
{
    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;
    private RestRequest req;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
        req = RestRequest.req();
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    /**
     * Get properties for node (empty result).
     * 
     * If there are no properties, there will be an HTTP 204 response.
     */
    @Documented
    @Test
    public void shouldGet204ForNoProperties() {
        JaxRsResponse createResponse = req.post(functionalTestHelper.dataUri() + "node/", "");
        gen.get()
                .expectedStatus(204)
                .get(createResponse.getLocation()
                        .toString() + "/properties");
    }

    /**
     * Get properties for node.
     */
    @Documented
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
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );
    }

    @Test
    public void shouldGet404ForNoProperty()
    {
        final JaxRsResponse createResponse = req.post(functionalTestHelper.dataUri() + "node/", "");
        JaxRsResponse response = req.get(getPropertyUri(createResponse.getLocation().toString(), "foo"));
        assertEquals(404, response.getStatus());
    }

    /**
     * Get property for node.
     * 
     * Get a single node property from a node.
     */
    @Documented
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
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));

        JaxRsResponse createResponse = req.post(functionalTestHelper.dataUri() + "node/", entity);

        JaxRsResponse response = req.get(getPropertyUri(createResponse.getLocation().toString(), "foo"));
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

        createResponse.close();
        response.close();
    }

    private String getPropertyUri( final String baseUri, final String key )
    {
        return baseUri + "/properties/" + key;
    }
}
