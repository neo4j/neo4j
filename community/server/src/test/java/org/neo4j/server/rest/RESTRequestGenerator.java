/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientRequest.Builder;
import com.sun.jersey.api.client.ClientResponse;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Predicate;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.neo4j.function.Predicates;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.test.GraphDefinition;
import org.neo4j.test.TestData.Producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RESTRequestGenerator
{
    private static final Builder REQUEST_BUILDER = ClientRequest.create();

    private static final List<String> RESPONSE_HEADERS = Arrays.asList( "Content-Type", "Location" );

    private static final List<String> REQUEST_HEADERS = Arrays.asList( "Content-Type", "Accept" );

    public static final Producer<RESTRequestGenerator> PRODUCER = new Producer<RESTRequestGenerator>()
    {
        @Override
        public RESTRequestGenerator create( GraphDefinition graph, String title, String documentation )
        {
            return new RESTRequestGenerator();
        }

        @Override
        public void destroy( RESTRequestGenerator product, boolean successful )
        {
        }
    };

    private int expectedResponseStatus = -1;
    private MediaType expectedMediaType = MediaType.valueOf( "application/json; charset=UTF-8" );
    private MediaType payloadMediaType = MediaType.APPLICATION_JSON_TYPE;
    private final List<Pair<String,Predicate<String>>> expectedHeaderFields = new ArrayList<>();
    private String payload;
    private final Map<String, String> addedRequestHeaders = new TreeMap<>(  );

    private RESTRequestGenerator()
    {
    }

    /**
     * Set the expected status of the response. The test will fail if the
     * response has a different status. Defaults to HTTP 200 OK.
     *
     * @param expectedResponseStatus the expected response status
     */
    public RESTRequestGenerator expectedStatus( final int expectedResponseStatus )
    {
        this.expectedResponseStatus = expectedResponseStatus;
        return this;
    }

    /**
     * Set the expected status of the response. The test will fail if the
     * response has a different status. Defaults to HTTP 200 OK.
     *
     * @param expectedStatus the expected response status
     */
    public RESTRequestGenerator expectedStatus( final ClientResponse.Status expectedStatus )
    {
        this.expectedResponseStatus = expectedStatus.getStatusCode();
        return this;
    }

    /**
     * Set the expected media type of the response. The test will fail if the
     * response has a different media type. Defaults to application/json.
     *
     * @param expectedMediaType the expected media tyupe
     */
    public RESTRequestGenerator expectedType( final MediaType expectedMediaType )
    {
        this.expectedMediaType = expectedMediaType;
        return this;
    }

    /**
     * The media type of the request payload. Defaults to application/json.
     *
     * @param payloadMediaType the media type to use
     */
    public RESTRequestGenerator payloadType( final MediaType payloadMediaType )
    {
        this.payloadMediaType = payloadMediaType;
        return this;
    }

    /**
     * The additional headers for the request
     *
     * @param key header key
     * @param value header value
     */
    public RESTRequestGenerator withHeader( final String key, final String value )
    {
        this.addedRequestHeaders.put(key,value);
        return this;
    }

    /**
     * Set the payload of the request.
     *
     * @param payload the payload
     */
    public RESTRequestGenerator payload( final String payload )
    {
        this.payload = payload;
        return this;
    }

    /**
     * Add an expected response header. If the heading is missing in the
     * response the test will fail. The header and its value are also included
     * in the documentation.
     *
     * @param expectedHeaderField the expected header
     */
    public RESTRequestGenerator expectedHeader( final String expectedHeaderField )
    {
        this.expectedHeaderFields.add( Pair.of(expectedHeaderField, Predicates.notNull()) );
        return this;
    }

    /**
     * Add an expected response header. If the heading is missing in the
     * response the test will fail. The header and its value are also included
     * in the documentation.
     *
     * @param expectedHeaderField the expected header
     * @param expectedValue the expected header value
     */
    public RESTRequestGenerator expectedHeader( final String expectedHeaderField, String expectedValue )
    {
        this.expectedHeaderFields.add( Pair.of(expectedHeaderField, Predicate.isEqual( expectedValue )) );
        return this;
    }

    /**
     * Send a GET request.
     *
     * @param uri the URI to use.
     */
    public ResponseEntity get( final String uri )
    {
        return retrieveResponseFromRequest( "GET", uri, expectedResponseStatus, expectedMediaType,
                expectedHeaderFields );
    }

    /**
     * Send a POST request.
     *
     * @param uri the URI to use.
     */
    public ResponseEntity post( final String uri )
    {
        return retrieveResponseFromRequest( "POST", uri, payload, payloadMediaType,
                expectedResponseStatus, expectedMediaType, expectedHeaderFields );
    }

    /**
     * Send a PUT request.
     *
     * @param uri the URI to use.
     */
    public ResponseEntity put( final String uri )
    {
        return retrieveResponseFromRequest( "PUT", uri, payload, payloadMediaType,
                expectedResponseStatus, expectedMediaType, expectedHeaderFields );
    }

    /**
     * Send a DELETE request.
     *
     * @param uri the URI to use.
     */
    public ResponseEntity delete( final String uri )
    {
        return retrieveResponseFromRequest( "DELETE", uri, payload, payloadMediaType,
                expectedResponseStatus, expectedMediaType, expectedHeaderFields );
    }

    /**
     * Send a request with no payload.
     */
    private ResponseEntity retrieveResponseFromRequest( final String method, final String uri, final int responseCode,
            final MediaType accept, final List<Pair<String,Predicate<String>>> headerFields )
    {
        ClientRequest request;
        try
        {
            request = withHeaders( REQUEST_BUILDER ).accept( accept ).build( new URI( uri ), method );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
        return retrieveResponse( uri, responseCode, accept, headerFields, request );
    }

    /**
     * Send a request with payload.
     */
    private ResponseEntity retrieveResponseFromRequest( final String method, final String uri, final String payload,
            final MediaType payloadType, final int responseCode, final MediaType accept,
            final List<Pair<String,Predicate<String>>> headerFields )
    {
        ClientRequest request;
        try
        {
            if ( payload != null )
            {
                request = withHeaders( REQUEST_BUILDER ).type( payloadType ).accept( accept ).entity( payload )
                        .build( new URI( uri ), method );
            }
            else
            {
                request = withHeaders( REQUEST_BUILDER ).accept( accept ).build( new URI( uri ), method );
            }
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
        return retrieveResponse( uri, responseCode, accept, headerFields, request );
    }

    private <T extends Builder> T withHeaders( T builder )
    {
        for ( Entry<String,String> entry : addedRequestHeaders.entrySet() )
        {
            builder.header(entry.getKey(),entry.getValue());
        }
        return builder;
    }

    /**
     * Send the request and create the documentation.
     */
    private ResponseEntity retrieveResponse( final String uri,
                                             final int responseCode, final MediaType type,
                                             final List<Pair<String, Predicate<String>>> headerFields,
                                             final ClientRequest request )
    {
        RequestData data = new RequestData();
        getRequestHeaders( data, request.getHeaders() );
        if ( request.getEntity() != null )
        {
            data.setPayload( String.valueOf( request.getEntity() ) );
        }
        Client client = new Client();
        ClientResponse response = client.handle( request );
        if ( response.hasEntity() && response.getStatus() != 204 )
        {
            data.setEntity( response.getEntity( String.class ) );
        }
        if ( response.getType() != null )
        {
            assertTrue( "wrong response type: " + data.entity, response.getType().isCompatible( type ) );
        }
        for ( Pair<String,Predicate<String>> headerField : headerFields )
        {
            assertTrue( "wrong headers: " + response.getHeaders(), headerField.other().test( response.getHeaders()
                    .getFirst( headerField.first() ) ) );
        }
        data.setMethod( request.getMethod() );
        data.setUri( uri );
        data.setStatus( responseCode );
        assertEquals( "Wrong response status. response: " + data.entity, responseCode, response.getStatus() );
        getResponseHeaders( data, response.getHeaders(), headerNames(headerFields) );
        return new ResponseEntity( response, data.entity );
    }

    private List<String> headerNames( List<Pair<String, Predicate<String>>> headerPredicates )
    {
        List<String> names = new ArrayList<>();
        for ( Pair<String, Predicate<String>> headerPredicate : headerPredicates )
        {
            names.add( headerPredicate.first() );
        }
        return names;
    }

    private void getResponseHeaders( final RequestData data, final MultivaluedMap<String, String> headers,
                                     final List<String> additionalFilter )
    {
        data.setResponseHeaders( getHeaders( headers, RESPONSE_HEADERS, additionalFilter ) );
    }

    private void getRequestHeaders( final RequestData data, final MultivaluedMap<String, Object> headers )
    {
        data.setRequestHeaders( getHeaders( headers, REQUEST_HEADERS,
                addedRequestHeaders.keySet() ) );
    }

    private <T> Map<String, String> getHeaders( final MultivaluedMap<String, T> headers, final List<String> filter,
            final Collection<String> additionalFilter )
    {
        Map<String, String> filteredHeaders = new TreeMap<>();
        for ( Entry<String, List<T>> header : headers.entrySet() )
        {
            String key = header.getKey();
            if ( filter.contains( key ) || additionalFilter.contains( key ) )
            {
                String values = "";
                for ( T value : header.getValue() )
                {
                    if ( !values.isEmpty() )
                    {
                        values += ", ";
                    }
                    values += String.valueOf( value );
                }
                filteredHeaders.put( key, values );
            }
        }
        return filteredHeaders;
    }

    /**
     * Wraps a response, to give access to the response entity as well.
     */
    public static class ResponseEntity
    {
        private final String entity;
        private final JaxRsResponse response;

        public ResponseEntity( ClientResponse response, String entity )
        {
            this.response = new JaxRsResponse( response, entity );
            this.entity = entity;
        }

        /**
         * The response entity as a String.
         */
        public String entity()
        {
            return entity;
        }

        /**
         * Note that the response object returned does not give access to the
         * response entity.
         */
        public JaxRsResponse response()
        {
            return response;
        }
    }
}
