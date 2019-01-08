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
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.test.server.HTTP;

public class RestRequest
{

    private final URI baseUri;
    private static final Client DEFAULT_CLIENT = Client.create();
    private final Client client;
    private MediaType accept = MediaType.APPLICATION_JSON_TYPE;
    private Map<String,String> headers = new HashMap<>();

    public RestRequest( URI baseUri )
    {
        this( baseUri, null, null );
    }

    public RestRequest( URI baseUri, String username, String password )
    {
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

    public RestRequest( URI uri, Client client )
    {
        this.baseUri = uriWithoutSlash( uri );
        this.client = client;
    }

    public RestRequest()
    {
        this( null );
    }

    private URI uriWithoutSlash( URI uri )
    {
        if ( uri == null )
        {
            return null;
        }
        String uriString = uri.toString();
        return uriString.endsWith( "/" ) ? uri( uriString.substring( 0, uriString.length() - 1 ) ) : uri;
    }

    private Builder builder( String path )
    {
        return builder( path, accept );
    }

    private Builder builder( String path, final MediaType accept )
    {
        WebResource resource = client.resource( uri( pathOrAbsolute( path ) ) );
        Builder builder = resource.accept( accept );
        if ( !headers.isEmpty() )
        {
            for ( Map.Entry<String,String> header : headers.entrySet() )
            {
                builder = builder.header( header.getKey(), header.getValue() );
            }
        }

        return builder;
    }

    private String pathOrAbsolute( String path )
    {
        if ( path.startsWith( "http://" ) )
        {
            return path;
        }
        return baseUri + "/" + path;
    }

    public JaxRsResponse get( String path )
    {
        return JaxRsResponse.extractFrom( HTTP.sanityCheck( builder( path ).get( ClientResponse.class ) ) );
    }

    public JaxRsResponse delete( String path )
    {
        return JaxRsResponse.extractFrom( HTTP.sanityCheck( builder( path ).delete( ClientResponse.class ) ) );
    }

    public JaxRsResponse post( String path, String data )
    {
        return post( path, data, MediaType.APPLICATION_JSON_TYPE );
    }

    public JaxRsResponse post( String path, String data, final MediaType mediaType )
    {
        Builder builder = builder( path );
        if ( data != null )
        {
            builder = builder.entity( data, mediaType );
        }
        else
        {
            builder = builder.type( mediaType );
        }
        return JaxRsResponse.extractFrom( HTTP.sanityCheck( builder.post( ClientResponse.class ) ) );
    }

    public JaxRsResponse put( String path, String data )
    {
        Builder builder = builder( path );
        if ( data != null )
        {
            builder = builder.entity( data, MediaType.APPLICATION_JSON_TYPE );
        }
        return new JaxRsResponse( HTTP.sanityCheck( builder.put( ClientResponse.class ) ) );
    }

    private URI uri( String uri )
    {
        try
        {
            return new URI( uri );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

    public JaxRsResponse get()
    {
        return get( "" );
    }

    public JaxRsResponse get( String path, final MediaType acceptType )
    {
        Builder builder = builder( path, acceptType );
        return JaxRsResponse.extractFrom( HTTP.sanityCheck( builder.get( ClientResponse.class ) ) );
    }

    public static RestRequest req()
    {
        return new RestRequest();
    }

    public JaxRsResponse delete( URI location )
    {
        return delete( location.toString() );
    }

    public JaxRsResponse put( URI uri, String data )
    {
        return put( uri.toString(), data );
    }

    public RestRequest accept( MediaType accept )
    {
        this.accept = accept;
        return this;
    }

    public RestRequest header( String header, String value )
    {
        this.headers.put( header, value );
        return this;
    }

    public RestRequest host( String hostname )
    {
        // 'host' is one of a handful of so-called restricted headers (wrongly!).
        // Need to rectify that with a property change.
        header( "Host", hostname );
        return this;
    }

    public Map<String,String> getHeaders()
    {
        return headers;
    }
}
