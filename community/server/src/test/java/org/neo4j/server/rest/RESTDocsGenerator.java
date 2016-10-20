/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientRequest.Builder;
import com.sun.jersey.api.client.ClientResponse;

import org.neo4j.doc.tools.AsciiDocGenerator;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.test.GraphDefinition;
import org.neo4j.test.TestData.Producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Generate asciidoc-formatted documentation from HTTP requests and responses.
 * The status and media type of all responses is checked as well as the
 * existence of any expected headers.
 *
 * The filename of the resulting ASCIIDOC test file is derived from the title.
 *
 * The title is determined by either a JavaDoc period terminated first title line,
 * the @Title annotation or the method name, where "_" is replaced by " ".
 */
public class RESTDocsGenerator extends AsciiDocGenerator
{
    private static final Builder REQUEST_BUILDER = ClientRequest.create();

    private static final List<String> RESPONSE_HEADERS = Arrays.asList( "Content-Type", "Location" );

    private static final List<String> REQUEST_HEADERS = Arrays.asList( "Content-Type", "Accept" );

    public static final Producer<RESTDocsGenerator> PRODUCER = new Producer<RESTDocsGenerator>()
    {
        @Override
        public RESTDocsGenerator create( GraphDefinition graph, String title, String documentation )
        {
            RESTDocsGenerator gen = RESTDocsGenerator.create( title );
            gen.description(documentation);
            return gen;
        }

        @Override
        public void destroy( RESTDocsGenerator product, boolean successful )
        {
            // TODO: invoke some complete method here?
        }
    };

    private int expectedResponseStatus = -1;
    private MediaType expectedMediaType = MediaType.valueOf( "application/json; charset=UTF-8" );
    private MediaType payloadMediaType = MediaType.APPLICATION_JSON_TYPE;
    private final List<Pair<String,Predicate<String>>> expectedHeaderFields = new ArrayList<>();
    private String payload;
    private final Map<String, String> addedRequestHeaders = new TreeMap<>(  );

    /**
     * Creates a documented test case. Finish building it by using one of these:
     * {@link #get(String)}, {@link #post(String)}, {@link #put(String)},
     * {@link #delete(String)}. To access the
     * response, use {@link ResponseEntity#entity} to get the entity or
     * {@link ResponseEntity#response} to get the rest of the response
     * (excluding the entity).
     *
     * @param title title of the test
     */
    public static RESTDocsGenerator create( final String title )
    {
        if ( title == null )
        {
            throw new IllegalArgumentException( "The title can not be null" );
        }
        return new RESTDocsGenerator( title );
    }

    private RESTDocsGenerator( String title )
    {
        super( title, "rest-api" );
    }

    /**
     * Set the expected status of the response. The test will fail if the
     * response has a different status. Defaults to HTTP 200 OK.
     *
     * @param expectedResponseStatus the expected response status
     */
    public RESTDocsGenerator expectedStatus( final int expectedResponseStatus )
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
    public RESTDocsGenerator expectedStatus( final ClientResponse.Status expectedStatus)
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
    public RESTDocsGenerator expectedType( final MediaType expectedMediaType )
    {
        this.expectedMediaType = expectedMediaType;
        return this;
    }

    /**
     * The media type of the request payload. Defaults to application/json.
     *
     * @param payloadMediaType the media type to use
     */
    public RESTDocsGenerator payloadType( final MediaType payloadMediaType )
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
    public RESTDocsGenerator withHeader( final String key, final String value )
    {
        this.addedRequestHeaders.put(key,value);
        return this;
    }

    /**
     * Set the payload of the request.
     *
     * @param payload the payload
     */
    public RESTDocsGenerator payload( final String payload )
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
    public RESTDocsGenerator expectedHeader( final String expectedHeaderField )
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
    public RESTDocsGenerator expectedHeader( final String expectedHeaderField, String expectedValue )
    {
        this.expectedHeaderFields.add( Pair.of(expectedHeaderField, Predicate.isEqual( expectedValue )) );
        return this;
    }

    @Override
    public RESTDocsGenerator description( String description )
    {
        return (RESTDocsGenerator) super.description( description );
    }

    /**
     * Send a GET request.
     *
     * @param uri the URI to use.
     */
    public ResponseEntity get( final String uri )
    {
        return retrieveResponseFromRequest( title, description, "GET", uri, expectedResponseStatus, expectedMediaType,
                expectedHeaderFields );
    }

    /**
     * Send a POST request.
     *
     * @param uri the URI to use.
     */
    public ResponseEntity post( final String uri )
    {
        return retrieveResponseFromRequest( title, description, "POST", uri, payload, payloadMediaType,
                expectedResponseStatus, expectedMediaType, expectedHeaderFields );
    }

    /**
     * Send a PUT request.
     *
     * @param uri the URI to use.
     */
    public ResponseEntity put( final String uri )
    {
        return retrieveResponseFromRequest( title, description, "PUT", uri, payload, payloadMediaType,
                expectedResponseStatus, expectedMediaType, expectedHeaderFields );
    }

    /**
     * Send a DELETE request.
     *
     * @param uri the URI to use.
     */
    public ResponseEntity delete( final String uri )
    {
        return retrieveResponseFromRequest( title, description, "DELETE", uri, payload, payloadMediaType,
                expectedResponseStatus, expectedMediaType, expectedHeaderFields );
    }

    /**
     * Send a request with no payload.
     */
    private ResponseEntity retrieveResponseFromRequest( final String title, final String description,
            final String method, final String uri, final int responseCode, final MediaType accept,
            final List<Pair<String,Predicate<String>>> headerFields )
    {
        ClientRequest request;
        try
        {
            request = withHeaders(REQUEST_BUILDER)
                    .accept(accept)
                    .build( new URI( uri ), method );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
        return retrieveResponse( title, description, uri, responseCode, accept, headerFields, request );
    }

    /**
     * Send a request with payload.
     */
    private ResponseEntity retrieveResponseFromRequest( final String title, final String description,
            final String method, final String uri, final String payload, final MediaType payloadType,
            final int responseCode, final MediaType accept, final List<Pair<String,Predicate<String>>> headerFields )
    {
        ClientRequest request;
        try
        {
            if ( payload != null )
            {
                request = withHeaders(REQUEST_BUILDER)
                        .type(payloadType)
                        .accept(accept)
                        .entity(payload)
                        .build( new URI( uri ), method );
            }
            else
            {
                request = withHeaders(REQUEST_BUILDER).accept( accept )
                        .build(new URI(uri), method);
            }
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
        return retrieveResponse( title, description, uri, responseCode, accept, headerFields, request );
    }

    private <T extends Builder> T withHeaders(T builder) {
        for (Entry<String, String> entry : addedRequestHeaders.entrySet()) {
            builder.header(entry.getKey(),entry.getValue());
        }
        return builder;
    }

    /**
     * Send the request and create the documentation.
     */
    private ResponseEntity retrieveResponse( final String title, final String description, final String uri,
            final int responseCode, final MediaType type, final List<Pair<String,Predicate<String>>> headerFields, final ClientRequest request )
    {
        DocumentationData data = new DocumentationData();
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
            assertTrue( "wrong response type: "+ data.entity, response.getType().isCompatible( type ) );
        }
        for ( Pair<String,Predicate<String>> headerField : headerFields )
        {
            assertTrue( "wrong headers: " + response.getHeaders(), headerField.other().test( response.getHeaders()
                    .getFirst( headerField.first() ) ) );
        }
        data.setTitle( title );
        data.setDescription( description );
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

    private void getResponseHeaders( final DocumentationData data, final MultivaluedMap<String, String> headers,
            final List<String> additionalFilter )
    {
        data.setResponseHeaders( getHeaders( headers, RESPONSE_HEADERS, additionalFilter ) );
    }

    private void getRequestHeaders( final DocumentationData data, final MultivaluedMap<String, Object> headers )
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
            this.response = new JaxRsResponse(response,entity);
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
