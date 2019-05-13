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

import java.lang.annotation.Annotation;
import java.net.URI;
import java.net.http.HttpResponse;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

public class JaxRsResponse extends Response
{
    private final HttpResponse<String> response;

    public JaxRsResponse( HttpResponse<String> response )
    {
        this.response = response;
    }

    @Override
    public String getEntity()
    {
        return response.body();
    }

    @Override
    public <T> T readEntity( Class<T> entityType )
    {
        return null;
    }

    @Override
    public <T> T readEntity( GenericType<T> entityType )
    {
        return null;
    }

    @Override
    public <T> T readEntity( Class<T> entityType, Annotation[] annotations )
    {
        return null;
    }

    @Override
    public <T> T readEntity( GenericType<T> entityType, Annotation[] annotations )
    {
        return null;
    }

    @Override
    public boolean hasEntity()
    {
        return false;
    }

    @Override
    public boolean bufferEntity()
    {
        return false;
    }

    @Override
    public int getStatus()
    {
        return response.statusCode();
    }

    @Override
    public StatusType getStatusInfo()
    {
        return null;
    }

    @Override
    public MultivaluedMap<String,Object> getMetadata()
    {
        var metadata = new MultivaluedHashMap<String,Object>();
        for ( var entry : response.headers().map().entrySet() )
        {
            metadata.addAll( entry.getKey(), entry.getValue() );
        }
        return metadata;
    }

    @Override
    public MultivaluedMap<String,Object> getHeaders()
    {
        return getMetadata();
    }

    @Override
    public MultivaluedMap<String,String> getStringHeaders()
    {
        return null;
    }

    @Override
    public String getHeaderString( String name )
    {
        return response.headers().firstValue( name ).orElseThrow();
    }

    @Override
    public URI getLocation()
    {
        return response.uri();
    }

    @Override
    public Set<Link> getLinks()
    {
        return null;
    }

    @Override
    public boolean hasLink( String relation )
    {
        return false;
    }

    @Override
    public Link getLink( String relation )
    {
        return null;
    }

    @Override
    public Link.Builder getLinkBuilder( String relation )
    {
        return null;
    }

    @Override
    public void close()
    {

    }

    @Override
    public MediaType getMediaType()
    {
        return null;
    }

    @Override
    public Locale getLanguage()
    {
        return null;
    }

    @Override
    public int getLength()
    {
        return 0;
    }

    @Override
    public Set<String> getAllowedMethods()
    {
        return null;
    }

    @Override
    public Map<String,NewCookie> getCookies()
    {
        return null;
    }

    @Override
    public EntityTag getEntityTag()
    {
        return null;
    }

    @Override
    public Date getDate()
    {
        return null;
    }

    @Override
    public Date getLastModified()
    {
        return null;
    }

    public MediaType getType()
    {
        return MediaType.valueOf( getHeaderString( CONTENT_TYPE ) );
    }
}
