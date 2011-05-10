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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class DocumentationOutput implements MethodRule
{

    public ClientResponse get( final String title, final String uri,
            final Response.Status responseCode, String headerField )
    {
        WebResource resource = Client.create().resource( uri );
        ClientResponse response = resource.accept( applicationJsonType ).get(
                ClientResponse.class );
        assertEquals( responseCode.getStatusCode(), response.getStatus() );
        assertEquals( applicationJsonType, response.getType() );
        if ( headerField != null )
        {
            assertNotNull( response.getHeaders().get( headerField ) );
        }
        data.setTitle( title );
        data.setMethod( "GET" );
        data.setRelUri( uri.substring( functionalTestHelper.dataUri().length() - 9 ) );
        data.setUri( uri );
        data.setResponse( responseCode );
        data.setResponseBody( response.getEntity( String.class ) );
        data.headers = response.getHeaders();
        data.headerField = headerField;

        document();
        return response;
    }

    // public ClientResponse post( final String title, final Map parameters,
    // final String uri, final Response.Status responseCode )
    // {
    // String payload = JsonHelper.createJsonFrom( parameters );
    // return post(title, payload, uri, responseCode);
    //
    // }

    public ClientResponse post( String title, String payload, String uri,
            Status responseCode, String headerField )
    {
        ClientResponse response = Client.create().resource( uri ).type(
                applicationJsonType ).accept( applicationJsonType ).entity(
                payload ).post( ClientResponse.class );
        assertEquals( responseCode.getStatusCode(), response.getStatus() );
        assertEquals( applicationJsonType, response.getType() );
        if ( headerField != null )
        {
            assertNotNull( response.getHeaders().get( headerField ) );
        }
        data.setTitle( title );
        data.setMethod( "POST" );
        data.setRelUri( uri.substring( functionalTestHelper.dataUri().length() - 9 ) );
        data.setUri( uri );
        data.setResponse( responseCode );
        data.setResponseBody( response.getEntity( String.class ) );
        data.payload = payload;
        data.payloadencoding = applicationJsonType;
        data.headers = response.getHeaders();
        data.headerField = headerField;
        document();
        return response;
    }

    public ClientResponse put( String title, String payload, String uri,
            Status responseCode, String headerField )
    {
        ClientResponse response = Client.create().resource( uri ).type(
                applicationJsonType ).accept( applicationJsonType ).entity(
                payload ).put( ClientResponse.class );
        assertEquals( responseCode.getStatusCode(), response.getStatus() );
        if ( headerField != null )
        {
            assertNotNull( response.getHeaders().get( headerField ) );
        }
        data.setTitle( title );
        data.setMethod( "PUT" );
        data.setRelUri( uri.substring( functionalTestHelper.dataUri().length() - 9 ) );
        data.setUri( uri );
        data.setResponse( responseCode );
        // data.setResponseBody( response.getEntity( String.class ) );
        data.payload = payload;
        data.payloadencoding = applicationJsonType;
        data.headers = response.getHeaders();
        data.headerField = headerField;
        document();
        return response;
    }

    
    public ClientResponse delete( String title, String payload, String uri,
            Status responseCode, String headerField )
    {
        ClientResponse response = Client.create().resource( uri ).type(
                applicationJsonType ).accept( applicationJsonType ).entity(
                payload ).delete( ClientResponse.class );
        assertEquals( responseCode.getStatusCode(), response.getStatus() );
        if ( headerField != null )
        {
            assertNotNull( response.getHeaders().get( headerField ) );
        }
        data.setTitle( title );
        data.setMethod( "DELETE" );
        data.setRelUri( uri.substring( functionalTestHelper.dataUri().length() - 9 ) );
        data.setUri( uri );
        data.setResponse( responseCode );
        // data.setResponseBody( response.getEntity( String.class ) );
        data.payload = payload;
        data.payloadencoding = applicationJsonType;
        data.headers = response.getHeaders();
        data.headerField = headerField;
        document();
        return response;
    }
    protected DocuementationData data = new DocuementationData();

    public class DocuementationData
    {

        public String headerField;
        public MultivaluedMap<String, String> headers;
        public MediaType payloadencoding;
        public String payload;
        public String title;
        public String uri;
        public String method;
        public Status status;
        public String entity;
        public String relUri;

        public void setTitle( final String title )
        {
            this.title = title;

        }

        public void setUri( final String uri )
        {
            this.uri = uri;

        }

        public void setMethod( final String method )
        {
            this.method = method;
            // TODO Auto-generated method stub

        }

        public void setResponse( final Status responseCode )
        {
            this.status = responseCode;

        }

        public void setResponseBody( final String entity )
        {
            this.entity = entity;

        }

        public void setRelUri( final String relUri )
        {
            this.relUri = relUri;

        }

    }

    private FileWriter fw;

    private final FunctionalTestHelper functionalTestHelper;

    private final MediaType applicationJsonType;

    public DocumentationOutput(
            final FunctionalTestHelper functionalTestHelper,
            final MediaType applicationJsonType )
    {
        this.functionalTestHelper = functionalTestHelper;
        this.applicationJsonType = applicationJsonType;
        data = new DocuementationData();

    }
    public DocumentationOutput(
            final FunctionalTestHelper functionalTestHelper)
    {
        this(functionalTestHelper, MediaType.APPLICATION_JSON_TYPE);

    }

    protected void document()
    {
        try
        {
            if ( data.title == null )
            {
                return;
            }
            File dirs = new File( "target/rest-api" );
            if ( !dirs.exists() )
            {
                dirs.mkdirs();
            }
            String name = data.title.replace( " ", "-" ).toLowerCase();
            File out = new File( dirs, name + ".txt" );
            out.createNewFile();

            fw = new FileWriter( out, false );

            line( "[[rest-api-" + name + "]]" );
            line( "== " + data.title + " ==" );
            line( "" );
            line( "*+" + data.method + " " + data.relUri + "+*" );
            line( "" );
            line( "_Example request URI_" );
            line( "" );
            line( data.uri );
            line( "" );
            if ( data.payload != null && !data.payload.equals( "null" ) )
            {
                line( "_Example request payload (payload encoding = \""
                      + data.payloadencoding + "\")_" );
                line( "" );
                line( "[source,javascript]" );
                line( "----" );
                line( data.payload );
                line( "----" );
                line( "" );
            }
            line( "_Example response_" );
            line( "" );
            line( "*+" + data.status.getStatusCode() + ": "
                  + data.status.name() + "+*" );
            line( "" );
            if ( data.headerField != null )
            {
                line( "" );
                line( "+Header field : " + data.headerField + ": "
                      + data.headers.get( data.headerField ) + "+" );

            }
            line( "" );
            if ( data.entity != null )
            {
                line( "[source,javascript]" );
                line( "----" );
                line( data.entity );
                line( "----" );
                line( "" );
            }
            fw.flush();
            fw.close();
        }
        catch ( IOException e )
        {
            fail();
            e.printStackTrace();
        }

    }

    private void line( final String string )
    {
        try
        {
            fw.append( string + "\n" );
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Override
    public Statement apply( final Statement base, final FrameworkMethod method,
            final Object target )
    {
        System.out.println( String.format( "%s %s %s", base, method.getName(),
                target ) );
        return new Statement()
        {

            @Override
            public void evaluate() throws Throwable
            {
                // TODO Auto-generated method stub

            }
        };
    }

}
