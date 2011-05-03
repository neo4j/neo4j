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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import javax.ws.rs.core.Response.Status;

public class DocumentationOutput
{
    protected DocuementationData data = new DocuementationData();

    private String START = "[[server-rest-api]]";

    public class DocuementationData
    {

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

    private File out = new File( "target/rest-api.txt" );

    private FileWriter fw;

    public DocumentationOutput()
    {
        data = new DocuementationData();

        openFile( out );
    }

    private void openFile( File out2 )
    {
        try
        {
            if ( !out.exists() )
            {
                out.createNewFile();
            }
            BufferedReader is = new BufferedReader( new FileReader( out ) );
            String line = is.readLine();
            if ( line == null || !line.equals( START ) )
            {
                is.close();
                fw = new FileWriter( out, false );
                line( START );
                line( "REST API\n========" );
                fw.close();
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }

    }

    protected void document()
    {
        try
        {
            if ( data.title == null )
            {
                return;
            }
            fw = new FileWriter( out, true );
            line( "" );
            line( "== " + data.title + " ==" );
            line( "" );
            line( "*+" + data.method + " " + data.relUri + "+*" );
            line( "" );
//            line( "_Example using curl_" );
//            line( "" );
//            line( "[source,bash]" );
//            line( "----" );
//            line( "curl -H Accept:application/json " + data.uri );
//            line( "----" );
//            line( "" );
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
            // TODO Auto-generated catch block
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

}
