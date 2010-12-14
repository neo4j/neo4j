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

package org.neo4j.server.rest.repr;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.neo4j.helpers.Service;

public final class RepresentationFormatRepository
{
    private final Map<MediaType, RepresentationFormat> formats;
    private final ExtensionInjector injector;

    public RepresentationFormatRepository( ExtensionInjector injector )
    {
        this.injector = injector;
        this.formats = new HashMap<MediaType, RepresentationFormat>();
        for ( RepresentationFormat format : Service.load( RepresentationFormat.class ) )
        {
            formats.put( format.mediaType, format );
        }
    }

    public OutputFormat outputFormat( List<MediaType> acceptable, URI baseUri )
            throws MediaTypeNotSupportedException
    {
        for ( MediaType type : acceptable )
        {
            RepresentationFormat format = formats.get( type );
            if ( format != null )
            {
                return new OutputFormat( format, baseUri, injector );
            }
        }

        throw new MediaTypeNotSupportedException( formats.keySet(),
                acceptable.toArray( new MediaType[acceptable.size()] ) );
    }

    public InputFormat inputFormat( MediaType type ) throws MediaTypeNotSupportedException
    {
        if ( type == null ) return NULL_FORMAT;

        RepresentationFormat format = formats.get( type );
        if ( format != null )
        {
            return format;
        }

        format = formats.get( new MediaType( type.getType(), type.getSubtype() ) );
        if ( format != null )
        {
            return format;
        }

        throw new MediaTypeNotSupportedException( formats.keySet(), type );
    }

    private static final InputFormat NULL_FORMAT = new InputFormat()
    {
        public Object readValue( String input )
        {
            if ( empty( input ) ) return null;
            throw new WebApplicationException( noMediaType() );
        }

        public URI readUri( String input )
        {
            if ( empty( input ) ) return null;
            throw new WebApplicationException( noMediaType() );
        }

        public Map<String, Object> readMap( String input )
        {
            if ( empty( input ) ) return Collections.emptyMap();
            throw new WebApplicationException( noMediaType() );
        }

        public List<Object> readList( String input )
        {
            if ( empty( input ) ) return Collections.emptyList();
            throw new WebApplicationException( noMediaType() );
        }

        private boolean empty( String input )
        {
            return input == null || "".equals( input.trim() );
        }

        private Response noMediaType()
        {
            return Response.status( Status.UNSUPPORTED_MEDIA_TYPE ).entity(
                    "Supplied data has no media type." ).build();
        }
    };
}
