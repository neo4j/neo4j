/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.rest;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;

class FakeUriInfo implements UriInfo
{
    private URI path;

    FakeUriInfo( URI path )
    {
        this.path = path;
    }

    @Override
    public String getPath()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPath( boolean decode )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PathSegment> getPathSegments()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PathSegment> getPathSegments( boolean decode )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public URI getRequestUri()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public UriBuilder getRequestUriBuilder()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public URI getAbsolutePath()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public UriBuilder getAbsolutePathBuilder()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public URI getBaseUri()
    {
        return path;
    }

    @Override
    public UriBuilder getBaseUriBuilder()
    {
        return UriBuilder.fromUri( path );
    }

    @Override
    public MultivaluedMap<String, String> getPathParameters()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MultivaluedMap<String, String> getPathParameters( boolean decode )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MultivaluedMap<String, String> getQueryParameters()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MultivaluedMap<String, String> getQueryParameters( boolean decode )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getMatchedURIs()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getMatchedURIs( boolean decode )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> getMatchedResources()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return path.toString();
    }
}
