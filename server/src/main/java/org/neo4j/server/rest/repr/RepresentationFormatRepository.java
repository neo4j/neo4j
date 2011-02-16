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
package org.neo4j.server.rest.repr;

import org.neo4j.helpers.Service;
import org.neo4j.server.rest.repr.formats.JsonFormat;

import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    {
        for ( MediaType type : acceptable )
        {
            RepresentationFormat format = formats.get( type );
            if ( format != null )
            {
                return new OutputFormat( format, baseUri, injector );
            }
        }

        return new OutputFormat( useDefault( acceptable ), baseUri, injector );
    }

    public InputFormat inputFormat( MediaType type )
    {
        if ( type == null )
        {
            return useDefault();
        }

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

        return useDefault( type );
    }

    private DefaultFormat useDefault( final List<MediaType> acceptable )
    {
        return  useDefault( acceptable.toArray( new MediaType[ acceptable.size() ] ) );
    }

    private DefaultFormat useDefault( final MediaType... type )
    {
        return new DefaultFormat( new JsonFormat(), formats.keySet(), type );
    }
}
