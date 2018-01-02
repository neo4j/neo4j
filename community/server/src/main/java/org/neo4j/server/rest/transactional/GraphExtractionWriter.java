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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonGenerator;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.IterableWrapper;

class GraphExtractionWriter implements ResultDataContentWriter
{
    @Override
    public void write( JsonGenerator out, Iterable<String> columns, Result.ResultRow row ) throws IOException
    {
        Set<Node> nodes = new HashSet<>();
        Set<Relationship> relationships = new HashSet<>();
        extract( nodes, relationships, map( columns, row ) );

        out.writeObjectFieldStart( "graph" );
        try
        {
            writeNodes( out, nodes );
            writeRelationships( out, relationships );
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writeNodes( JsonGenerator out, Iterable<Node> nodes ) throws IOException
    {
        out.writeArrayFieldStart( "nodes" );
        try
        {
            for ( Node node : nodes )
            {
                out.writeStartObject();
                try
                {
                    out.writeStringField( "id", Long.toString( node.getId() ) );
                    out.writeArrayFieldStart( "labels" );
                    try
                    {
                        for ( Label label : node.getLabels() )
                        {
                            out.writeString( label.name() );
                        }
                    }
                    finally
                    {
                        out.writeEndArray();
                    }
                    writeProperties( out, node );
                }
                finally
                {
                    out.writeEndObject();
                }
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void writeRelationships( JsonGenerator out, Iterable<Relationship> relationships ) throws IOException
    {
        out.writeArrayFieldStart( "relationships" );
        try
        {
            for ( Relationship relationship : relationships )
            {
                out.writeStartObject();
                try
                {
                    out.writeStringField( "id", Long.toString( relationship.getId() ) );
                    out.writeStringField( "type", relationship.getType().name() );
                    out.writeStringField( "startNode", Long.toString( relationship.getStartNode().getId() ) );
                    out.writeStringField( "endNode", Long.toString( relationship.getEndNode().getId() ) );
                    writeProperties( out, relationship );
                }
                finally
                {
                    out.writeEndObject();
                }
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void writeProperties( JsonGenerator out, PropertyContainer container ) throws IOException
    {
        out.writeObjectFieldStart( "properties" );
        try
        {
            for ( Map.Entry<String, Object> property : container.getAllProperties().entrySet() )
            {
                out.writeObjectField( property.getKey(), property.getValue() );

            }
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void extract( Set<Node> nodes, Set<Relationship> relationships, Iterable<?> source )
    {
        for ( Object item : source )
        {
            if ( item instanceof Node )
            {
                nodes.add( (Node) item );
            }
            else if ( item instanceof Relationship )
            {
                Relationship relationship = (Relationship) item;
                relationships.add( relationship );
                nodes.add( relationship.getStartNode() );
                nodes.add( relationship.getEndNode() );
            }
            if ( item instanceof Path )
            {
                Path path = (Path) item;
                for ( Node node : path.nodes() )
                {
                    nodes.add( node );
                }
                for ( Relationship relationship : path.relationships() )
                {
                    relationships.add( relationship );
                }
            }
            else if ( item instanceof Map<?, ?> )
            {
                extract( nodes, relationships, ((Map<?, ?>) item).values() );
            }
            else if ( item instanceof Iterable<?> )
            {
                extract( nodes, relationships, (Iterable<?>) item );
            }
        }
    }

    private static Iterable<?> map( Iterable<String> columns, final Result.ResultRow row )
    {
        return new IterableWrapper<Object, String>( columns )
        {
            @Override
            protected Object underlyingObjectToObject( String key )
            {
                return row.get( key );
            }
        };
    }
}
