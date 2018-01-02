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
package org.neo4j.server.rest.transactional;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.codehaus.jackson.JsonGenerator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.PathRepresentation;
import org.neo4j.server.rest.repr.RelationshipRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationFormat;
import org.neo4j.server.rest.repr.formats.StreamingJsonFormat;

class RestRepresentationWriter implements ResultDataContentWriter
{
    private final URI baseUri;

    RestRepresentationWriter( URI baseUri )
    {
        this.baseUri = baseUri;
    }

    @Override
    public void write( JsonGenerator out, Iterable<String> columns, Result.ResultRow row ) throws IOException
    {
        RepresentationFormat format = new StreamingJsonFormat.StreamingRepresentationFormat( out, null );
        out.writeArrayFieldStart( "rest" );
        try
        {
            for ( String key : columns )
            {
                write( out, format, row.get( key ) );
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void write( JsonGenerator out, RepresentationFormat format, Object value ) throws IOException
    {
        if ( value instanceof Map<?, ?> )
        {
            out.writeStartObject();
            try
            {
                for ( Map.Entry<String, ?> entry : ((Map<String, ?>) value).entrySet() )
                {
                    out.writeFieldName( entry.getKey() );
                    write( out, format, entry.getValue() );
                }
            }
            finally
            {
                out.writeEndObject();
            }
        }
        else if ( value instanceof Path )
        {
            write( format, new PathRepresentation<>( (Path) value ) );
        }
        else if ( value instanceof Iterable<?> )
        {
            out.writeStartArray();
            try
            {
                for ( Object item : (Iterable<?>) value )
                {
                    write( out, format, item );
                }
            }
            finally
            {
                out.writeEndArray();
            }
        }
        else if ( value instanceof Node )
        {
            write( format, new NodeRepresentation( (Node) value ) );
        }
        else if ( value instanceof Relationship )
        {
            write( format, new RelationshipRepresentation( (Relationship) value ) );
        }
        else
        {
            out.writeObject( value );
        }
    }

    private void write( RepresentationFormat format, Representation representation )
    {
        OutputFormat.write( representation, format, baseUri );
    }
}
