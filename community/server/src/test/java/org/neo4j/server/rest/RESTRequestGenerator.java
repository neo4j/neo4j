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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import javax.ws.rs.core.MediaType;

import org.neo4j.function.Predicates;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.test.TestData.Producer;

import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpRequest.BodyPublishers.noBody;
import static java.net.http.HttpRequest.BodyPublishers.ofString;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RESTRequestGenerator
{
    public static final Producer<RESTRequestGenerator> PRODUCER = ( graph, title, documentation ) -> new RESTRequestGenerator();

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
     * Set the expected media type of the response. The test will fail if the
     * response has a different media type. Defaults to application/json.
     *
     * @param expectedMediaType the expected media type
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
        var request = newRequestBuilder( uri )
                .method( method, noBody() )
                .header( ACCEPT, accept.toString() )
                .build();

        return retrieveResponse( responseCode, accept, headerFields, request );
    }

    /**
     * Send a request with payload.
     */
    private ResponseEntity retrieveResponseFromRequest( final String method, final String uri, final String payload,
            final MediaType payloadType, final int responseCode, final MediaType accept,
            final List<Pair<String,Predicate<String>>> headerFields )
    {
        HttpRequest request;
        if ( payload != null )
        {
            request = newRequestBuilder( uri )
                    .method( method, ofString( payload ) )
                    .header( CONTENT_TYPE, payloadType.toString() )
                    .header( ACCEPT, accept.toString() )
                    .build();
        }
        else
        {
            request = newRequestBuilder( uri )
                    .method( method, noBody() )
                    .header( ACCEPT, accept.toString() )
                    .build();
        }
        return retrieveResponse( responseCode, accept, headerFields, request );
    }

    private HttpRequest.Builder newRequestBuilder( String uri )
    {
        var builder = HttpRequest.newBuilder( URI.create( uri ) );
        for ( var entry : addedRequestHeaders.entrySet() )
        {
            builder.header( entry.getKey(), entry.getValue() );
        }
        return builder;
    }

    /**
     * Send the request and create the documentation.
     */
    private static ResponseEntity retrieveResponse( final int responseCode, final MediaType type,
            final List<Pair<String,Predicate<String>>> headerFields, final HttpRequest request )
    {
        try
        {
            var response = newHttpClient().send( request, BodyHandlers.ofString() );

            var responseContentType = response.headers().firstValue( CONTENT_TYPE );
            responseContentType.ifPresent( responseType -> assertThat( responseType, equalToIgnoringCase( type.toString() ) ) );

            var responseHeaders = response.headers();
            for ( var headerField : headerFields )
            {
                var name = headerField.first();
                var verifier = headerField.other();
                assertTrue( "Wrong headers: " + responseHeaders, verifier.test( responseHeaders.firstValue( name ).orElseThrow() ) );
            }

            assertEquals( responseCode, response.statusCode() );
            return new ResponseEntity( response );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( e );
        }
    }

    /**
     * Wraps a response, to give access to the response entity as well.
     */
    public static class ResponseEntity
    {
        private final String entity;
        private final JaxRsResponse response;

        public ResponseEntity( HttpResponse<String> response )
        {
            this.response = new JaxRsResponse( response );
            this.entity = response.body();
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
