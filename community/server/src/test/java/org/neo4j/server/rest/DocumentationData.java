/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Map;

class DocumentationData
{
    private String payload;
    public String title;
    public String description;
    public String uri;
    public String method;
    public int status;
    public String entity;
    public Map<String, String> requestHeaders;
    public Map<String, String> responseHeaders;

    public void setPayload( final String payload )
    {
        this.payload = payload;
    }

    public void setDescription( final String description )
    {
        this.description = description;
    }

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
    }

    public void setStatus( final int responseCode )
    {
        this.status = responseCode;

    }

    public void setEntity( final String entity )
    {
        this.entity = entity;
    }

    public void setResponseHeaders( final Map<String, String> response )
    {
        responseHeaders = response;
    }

    public void setRequestHeaders( final Map<String, String> request )
    {
        requestHeaders = request;
    }

    @Override
    public String toString()
    {
        return "DocumentationData [payload=" + payload + ", title=" + title + ", description=" + description
               + ", uri=" + uri + ", method=" + method + ", status=" + status + ", entity=" + entity
               + ", requestHeaders=" + requestHeaders + ", responseHeaders=" + responseHeaders + "]";
    }
}
