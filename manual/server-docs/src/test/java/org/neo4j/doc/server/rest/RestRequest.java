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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;

public class RestRequest {

    private final URI baseUri;
    private final static Client DEFAULT_CLIENT = Client.create();
    private final Client client;
    private MediaType accept = MediaType.APPLICATION_JSON_TYPE;
    private Map<String, String> headers=new HashMap<String, String>();

    public RestRequest( URI baseUri ) {
        this( baseUri, null, null );
    }

    public RestRequest( URI baseUri, String username, String password ) {
        this.baseUri = uriWithoutSlash( baseUri );
        if ( username != null )
        {
            client = Client.create();
            client.addFilter( new HTTPBasicAuthFilter( username, password ) );
        }
        else
        {
            client = DEFAULT_CLIENT;
        }
    }

    public RestRequest(URI uri, Client client) {
        this.baseUri = uriWithoutSlash( uri );
        this.client = client;
    }

    public RestRequest() {
        this( null );
    }

    private URI uriWithoutSlash( URI uri ) {
        if (uri == null) return null;
        String uriString = uri.toString();
        return uriString.endsWith( "/" ) ? uri( uriString.substring( 0, uriString.length() - 1 ) ) : uri;
    }

    public static String encode( Object value ) {
        if ( value == null ) return "";
        try {
            return URLEncoder.encode( value.toString(), StandardCharsets.UTF_8.name() ).replaceAll( "\\+", "%20" );
        } catch ( UnsupportedEncodingException e ) {
            throw new RuntimeException( e );
        }
    }


    private Builder builder( String path ) {
        return builder( path, accept );
    }

    private Builder builder( String path, final MediaType accept ) {
        WebResource resource = client.resource( uri( pathOrAbsolute( path ) ) );
        Builder builder = resource.accept( accept );
        if ( !headers.isEmpty() ) {
            for ( Map.Entry<String, String> header : headers.entrySet() ) {
                builder = builder.header( header.getKey(),header.getValue() );
            }
        }
        return builder;
    }

    private String pathOrAbsolute( String path ) {
        if ( path.startsWith( "http://" ) ) return path;
        return baseUri + "/" + path;
    }

    public org.neo4j.doc.server.rest.JaxRsResponse get( String path ) {
        return org.neo4j.doc.server.rest.JaxRsResponse.extractFrom( HTTP.sanityCheck( builder( path ).get( ClientResponse.class ) ) );
    }

    public org.neo4j.doc.server.rest.JaxRsResponse get(String path, String data) {
        return get( path, data, MediaType.APPLICATION_JSON_TYPE );
    }

    public org.neo4j.doc.server.rest.JaxRsResponse get( String path, String data, final MediaType mediaType ) {
        Builder builder = builder( path );
        if ( data != null ) {
            builder = builder.entity( data, mediaType);
        } else {
            builder = builder.type( mediaType );
        }
        return org.neo4j.doc.server.rest.JaxRsResponse.extractFrom( HTTP.sanityCheck( builder.get( ClientResponse.class ) ) );
    }

    public org.neo4j.doc.server.rest.JaxRsResponse delete(String path) {
        return org.neo4j.doc.server.rest.JaxRsResponse.extractFrom( HTTP.sanityCheck( builder( path ).delete( ClientResponse.class ) ) );
    }

    public org.neo4j.doc.server.rest.JaxRsResponse post(String path, String data) {
        return post(path, data, MediaType.APPLICATION_JSON_TYPE);
    }

    public org.neo4j.doc.server.rest.JaxRsResponse post(String path, String data, final MediaType mediaType) {
        Builder builder = builder( path );
        if ( data != null ) {
            builder = builder.entity( data, mediaType);
        } else {
            builder = builder.type(mediaType);
        }
        return org.neo4j.doc.server.rest.JaxRsResponse.extractFrom( HTTP.sanityCheck( builder.post( ClientResponse.class ) ) );
    }

    public org.neo4j.doc.server.rest.JaxRsResponse put(String path, String data) {
        Builder builder = builder( path );
        if ( data != null ) {
            builder = builder.entity( data, MediaType.APPLICATION_JSON_TYPE );
        }
        return new org.neo4j.doc.server.rest.JaxRsResponse( HTTP.sanityCheck( builder.put( ClientResponse.class ) ) );
    }


    public Object toEntity( org.neo4j.doc.server.rest.JaxRsResponse JaxRsResponse ) throws JsonParseException {
        return JsonHelper.readJson( entityString( JaxRsResponse ) );
    }

    public Map<?, ?> toMap( org.neo4j.doc.server.rest.JaxRsResponse JaxRsResponse) throws JsonParseException {
        final String json = entityString( JaxRsResponse );
        return JsonHelper.jsonToMap(json);
    }

    private String entityString( org.neo4j.doc.server.rest.JaxRsResponse JaxRsResponse) {
        return JaxRsResponse.getEntity();
    }

    public boolean statusIs( org.neo4j.doc.server.rest.JaxRsResponse JaxRsResponse, Response.StatusType status ) {
        return JaxRsResponse.getStatus() == status.getStatusCode();
    }

    public boolean statusOtherThan( org.neo4j.doc.server.rest.JaxRsResponse JaxRsResponse, Response.StatusType status ) {
        return !statusIs(JaxRsResponse, status );
    }

    public RestRequest with( String uri ) {
        return new RestRequest( uri( uri ), client );
    }

    private URI uri( String uri ) {
        try {
            return new URI( uri );
        } catch ( URISyntaxException e ) {
            throw new RuntimeException( e );
        }
    }

    public URI getUri() {
        return baseUri;
    }

    public org.neo4j.doc.server.rest.JaxRsResponse get() {
        return get( "" );
    }

    public org.neo4j.doc.server.rest.JaxRsResponse get(String path, final MediaType acceptType) {
        Builder builder = builder(path, acceptType);
        return org.neo4j.doc.server.rest.JaxRsResponse.extractFrom( HTTP.sanityCheck( builder.get( ClientResponse.class ) ) );
    }

    public static RestRequest req() {
        return new RestRequest();
    }

    public org.neo4j.doc.server.rest.JaxRsResponse delete(URI location) {
        return delete(location.toString());
    }

    public org.neo4j.doc.server.rest.JaxRsResponse put(URI uri, String data) {
        return put(uri.toString(),data);
    }

    public RestRequest accept( MediaType accept )
    {
        this.accept = accept;
        return this;
    }

    public RestRequest header(String header, String value) {
        this.headers.put(header,value);
        return this;
    }
}
