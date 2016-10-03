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

import com.sun.jersey.api.client.Client;
import org.junit.Test;

import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.server.rest.domain.JsonHelper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DiscoveryServiceDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
{
    @Test
    public void shouldRespondWith200WhenRetrievingDiscoveryDocument() throws Exception
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = getDiscoveryDocument();
        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGetContentLengthHeaderWhenRetrievingDiscoveryDocument() throws Exception
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = getDiscoveryDocument();
        assertNotNull( response.getHeaders()
                .get( "Content-Length" ) );
        response.close();
    }

    @Test
    public void shouldHaveJsonMediaTypeWhenRetrievingDiscoveryDocument() throws Exception
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = getDiscoveryDocument();
        assertThat( response.getType().toString(), containsString(MediaType.APPLICATION_JSON) );
        response.close();
    }

    @Test
    public void shouldHaveJsonDataInResponse() throws Exception
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = getDiscoveryDocument();

        Map<String, Object> map = JsonHelper.jsonToMap( response.getEntity() );

        String managementKey = "management";
        assertTrue( map.containsKey( managementKey ) );
        assertNotNull( map.get( managementKey ) );

        String dataKey = "data";
        assertTrue( map.containsKey( dataKey ) );
        assertNotNull( map.get( dataKey ) );
        response.close();
    }

    @Test
    public void shouldRedirectToWebadminOnHtmlRequest() throws Exception
    {
        Client nonRedirectingClient = Client.create();
        nonRedirectingClient.setFollowRedirects( false );

        org.neo4j.doc.server.rest.JaxRsResponse
                clientResponse = new RestRequest(null,nonRedirectingClient).get(server().baseUri().toString(),MediaType.TEXT_HTML_TYPE);

        assertEquals( 303, clientResponse.getStatus() );
    }

    private org.neo4j.doc.server.rest.JaxRsResponse getDiscoveryDocument() throws Exception
    {
        return new RestRequest(server().baseUri()).get();
    }

}
