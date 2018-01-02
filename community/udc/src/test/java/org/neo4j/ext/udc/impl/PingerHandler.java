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
package org.neo4j.ext.udc.impl;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

public class PingerHandler implements HttpRequestHandler
{
    private final Map<String, String> queryMap = new HashMap<String, String>();

    @Override
    public void handle( HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext )
            throws HttpException, IOException
    {
        final String requestUri = httpRequest.getRequestLine().getUri();
        final int offset = requestUri.indexOf( "?" );
        if ( offset > -1 )
        {
            String query = requestUri.substring( offset + 1 );
            String[] params = query.split( "\\+" );
            if ( params.length > 0 )
            {
                for ( String param : params )
                {
                    String[] pair = param.split( "=" );
                    String key = URLDecoder.decode( pair[0], "UTF-8" );
                    String value = URLDecoder.decode( pair[1], "UTF-8" );
                    queryMap.put( key, value );
                }
            }
        }
    }

    public Map<String, String> getQueryMap()
    {
        return queryMap;
    }
}
