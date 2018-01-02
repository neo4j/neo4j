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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
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
            return URLEncoder.encode( value.toString(), "utf-8" ).replaceAll( "\\+", "%20" );
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

    public JaxRsResponse get( String path ) {
        return JaxRsResponse.extractFrom( HTTP.sanityCheck( builder( path ).get( ClientResponse.class ) ) );
    }

    public JaxRsResponse get(String path, String data) {
        return get( path, data, MediaType.APPLICATION_JSON_TYPE );
    }

    public JaxRsResponse get( String path, String data, final MediaType mediaType ) {
        Builder builder = builder( path );
        if ( data != null ) {
            builder = builder.entity( data, mediaType);
        } else {
            builder = builder.type( mediaType );
        }
        return JaxRsResponse.extractFrom( HTTP.sanityCheck( builder.get( ClientResponse.class ) ) );
    }

    public JaxRsResponse delete(String path) {
        return JaxRsResponse.extractFrom( HTTP.sanityCheck( builder( path ).delete( ClientResponse.class ) ) );
    }

    public JaxRsResponse post(String path, String data) {
        return post(path, data, MediaType.APPLICATION_JSON_TYPE);
    }

    public JaxRsResponse post(String path, String data, final MediaType mediaType) {
        Builder builder = builder( path );
        if ( data != null ) {
            builder = builder.entity( data, mediaType);
        } else {
            builder = builder.type(mediaType);
        }
        return JaxRsResponse.extractFrom( HTTP.sanityCheck( builder.post( ClientResponse.class ) ) );
    }

    public JaxRsResponse put(String path, String data) {
        Builder builder = builder( path );
        if ( data != null ) {
            builder = builder.entity( data, MediaType.APPLICATION_JSON_TYPE );
        }
        return new JaxRsResponse( HTTP.sanityCheck( builder.put( ClientResponse.class ) ) );
    }


    public Object toEntity( JaxRsResponse JaxRsResponse ) throws JsonParseException {
        return JsonHelper.readJson( entityString( JaxRsResponse ) );
    }

    public Map<?, ?> toMap( JaxRsResponse JaxRsResponse) throws JsonParseException {
        final String json = entityString( JaxRsResponse );
        return JsonHelper.jsonToMap(json);
    }

    private String entityString( JaxRsResponse JaxRsResponse) {
        return JaxRsResponse.getEntity();
    }

    public boolean statusIs( JaxRsResponse JaxRsResponse, Response.StatusType status ) {
        return JaxRsResponse.getStatus() == status.getStatusCode();
    }

    public boolean statusOtherThan( JaxRsResponse JaxRsResponse, Response.StatusType status ) {
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

    public JaxRsResponse get() {
        return get( "" );
    }

    public JaxRsResponse get(String path, final MediaType acceptType) {
        Builder builder = builder(path, acceptType);
        return JaxRsResponse.extractFrom( HTTP.sanityCheck( builder.get( ClientResponse.class ) ) );
    }

    public static RestRequest req() {
        return new RestRequest();
    }

    public JaxRsResponse delete(URI location) {
        return delete(location.toString());
    }

    public JaxRsResponse put(URI uri, String data) {
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
