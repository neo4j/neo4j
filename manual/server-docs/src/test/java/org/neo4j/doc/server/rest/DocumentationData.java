/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest;

import java.util.Map;

import javax.ws.rs.core.MediaType;

class DocumentationData
{
    private String payload;
    private MediaType payloadType = MediaType.APPLICATION_JSON_TYPE;
    public String title;
    public String description;
    public String uri;
    public String method;
    public int status;
    public String entity;
    public Map<String, String> requestHeaders;
    public Map<String, String> responseHeaders;
    public boolean ignore;

    public void setPayload( final String payload )
    {
        this.payload = payload;
    }

    public String getPayload()
    {
        if ( this.payload != null && !this.payload.trim()
                .isEmpty()
             && MediaType.APPLICATION_JSON_TYPE.equals( payloadType ) )
        {
            return JSONPrettifier.parse( this.payload );
        }
        else
        {
            return this.payload;
        }
    }

    public String getPrettifiedEntity()
    {
        return JSONPrettifier.parse( entity );
    }

    public void setPayloadType( final MediaType payloadType )
    {
        this.payloadType = payloadType;
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

    public void setIgnore() {
        this.ignore = true;
    }

    @Override
    public String toString()
    {
        return "DocumentationData [payload=" + payload + ", title=" + title + ", description=" + description
               + ", uri=" + uri + ", method=" + method + ", status=" + status + ", entity=" + entity
               + ", requestHeaders=" + requestHeaders + ", responseHeaders=" + responseHeaders + "]";
    }
}