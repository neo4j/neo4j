/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.core.util.StringKeyObjectValueIgnoreCaseMultivaluedMap;
import com.sun.net.httpserver.Headers;

@SuppressWarnings("restriction")
public class JaxRsResponse extends Response
{

    private ClientResponse jettyResponse;

    public JaxRsResponse( ClientResponse jettyResponse )
    {
        this.jettyResponse = jettyResponse;
    }

    @Override
    public Object getEntity()
    {
        return jettyResponse.getEntity( Object.class );
    }

    @Override
    public int getStatus()
    {
        return jettyResponse.getStatus();
    }

    @Override
    public MultivaluedMap<String, Object> getMetadata()
    {
        MultivaluedMap<String, Object> metadata = new StringKeyObjectValueIgnoreCaseMultivaluedMap();
        for ( Map.Entry<String, List<String>> header : jettyResponse.getHeaders().entrySet() )
        {
            for ( Object value : header.getValue() )
            {
                metadata.putSingle( header.getKey(), value );
            }
        }
        return metadata;
    }

    public Map<String, List<String>> getHeaders()
    {
        Headers headers = new Headers();
        headers.putAll( jettyResponse.getHeaders() );
        return headers;
    }

    public <T> T getEntity( Class<T> asType )
    {
        return jettyResponse.getEntity(asType);
    }

    public URI getLocation() throws URISyntaxException
    {
        return new URI(getHeaders().get( HttpHeaders.LOCATION).get(0));
    }
}
