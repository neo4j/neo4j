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
package org.neo4j.server.rest.web;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

@Path( "/resource" )
public class ResourcesService
{
    final static String JAVASCRIPT_BODY;
    static
    {
        // FIXME This is so very ugly, it's because when running it with maven
        // it won't add the src/main/resources to the classpath
        String body = null;
        try
        {
            body = readResourceAsString( "htmlbrowse.js" );
        }
        catch ( Exception e )
        {
            body = readFileAsString( "src/main/resources/htmlbrowse.js" );
        }
        JAVASCRIPT_BODY = body;
    }

    public ResourcesService( @Context UriInfo uriInfo )
    {
    }

    @GET
    @Path( "htmlbrowse.js" )
    public String getHtmlBrowseJavascript()
    {
        return JAVASCRIPT_BODY;
    }

    private static String readFileAsString( String file )
    {
        try
        {
            return readAsString( new FileInputStream( file ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static String readResourceAsString( String resource )
    {
        return readAsString( ClassLoader.getSystemResourceAsStream( resource ) );
    }

    private static String readAsString( InputStream input )
    {
        final char[] buffer = new char[0x10000];
        StringBuilder out = new StringBuilder();
        Reader reader = null;
        try
        {
            reader = new InputStreamReader( input, "UTF-8" );
            int read;
            do
            {
                read = reader.read( buffer, 0, buffer.length );
                if ( read > 0 )
                {
                    out.append( buffer, 0, read );
                }
            }
            while ( read >= 0 );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            if ( reader != null )
            {
                try
                {
                    reader.close();
                }
                catch ( IOException e )
                {
                    // OK
                }
            }
        }
        return out.toString();
    }
}
