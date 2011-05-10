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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class DocumentationOutput implements MethodRule
{

    public ClientResponse get( String title, String uri,
            Response.Status responseCode )
    {
        WebResource resource = Client.create().resource( uri );
        ClientResponse response = resource.accept( applicationJsonType ).get(
                ClientResponse.class );
        assertEquals( responseCode.getStatusCode(), response.getStatus() );
        data.setTitle( title );
        data.setMethod( "GET" );
        data.setRelUri( uri.substring( functionalTestHelper.dataUri().length() - 9 ) );
        data.setUri( uri );
        data.setResponse( responseCode );
        data.setResponseBody( response.getEntity( String.class ) );
        document();
        return response;
    }

    public ClientResponse post( String title, Map parameters, String uri,
            Response.Status responseCode )
    {
        String payload = JsonHelper.createJsonFrom( parameters );
        ClientResponse response = Client.create().resource( uri ).type(
                applicationJsonType ).accept( applicationJsonType ).entity(
                payload ).post( ClientResponse.class );
        data.setTitle( title );
        data.setMethod( "POST" );
        data.setRelUri( uri.substring( functionalTestHelper.dataUri().length() - 1 ) );
        data.setUri( uri );
        data.setResponse( responseCode );
        data.setResponseBody( response.getEntity( String.class ) );
        data.payload = payload;
        data.payloadencoding = applicationJsonType;
        document();
        return response;
    }

    protected DocuementationData data = new DocuementationData();

    public class DocuementationData
    {

        public MediaType payloadencoding;
        public String payload;
        String title;
        String uri;
        String method;
        private Status status;
        private String entity;
        private String relUri;

        public void setTitle( final String title )
        {
            this.title = title;

        }

        public void setUri( String uri )
        {
            this.uri = uri;

        }

        public void setMethod( String method )
        {
            this.method = method;
            // TODO Auto-generated method stub

        }

        public void setResponse( Status responseCode )
        {
            this.status = responseCode;

        }

        public void setResponseBody( String entity )
        {
            this.entity = entity;
            // TODO Auto-generated method stub

        }

        public void setRelUri( String relUri )
        {
            this.relUri = relUri;

        }

    }

    private FileWriter fw;

    private final FunctionalTestHelper functionalTestHelper;

    private final MediaType applicationJsonType;

    public DocumentationOutput( FunctionalTestHelper functionalTestHelper,
            MediaType applicationJsonType )
    {
        this.functionalTestHelper = functionalTestHelper;
        this.applicationJsonType = applicationJsonType;
        data = new DocuementationData();

    }

    protected void document()
    {
        try
        {
            if ( data.title == null )
            {
                return;
            }
            File dirs = new File("target/rest-api");
            if(!dirs.exists()) dirs.mkdirs();
            File out = new File( dirs, data.title.replace( " ", "_" )
                                           + ".txt" );
            out.createNewFile();
            
            fw = new FileWriter( out, false );

            line( "[[" + data.title + "]]" );
            line( "== " + data.title + " ==" );
            line( "" );
            if ( data.payload != null )
            {
                
                line( "_Example request (payload encoding = \"" + data.payloadencoding
                        + "\")_" );
                line( "" );
                line( data.payload );
            } else {
                line( "_Example request_" );
            }
            line( "" );
            line( "*+" + data.method + " " + data.relUri + "+*" );
            line( "" );
            line( "_Example Response_" );
            line( "" );
            line( "*+" + data.status.getStatusCode() + ": "
                  + data.status.name() + "+*" );
            line( "" );
            line( "[source,javascript]" );
            line( "----" );
            line( data.entity );
            line( "----" );
            line( "" );
            fw.flush();
            fw.close();
        }
        catch ( IOException e )
        {
            fail();
            e.printStackTrace();
        }

    }

    private void line( String string )
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
    public Statement apply( Statement base, FrameworkMethod method,
            Object target )
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
