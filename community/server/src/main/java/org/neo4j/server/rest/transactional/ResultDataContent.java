/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonGenerator;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

public enum ResultDataContent
{
    row( FieldType.ARRAY )
    {
        @Override
        void formatRow( JsonGenerator out, Iterable<String> columns, Map<String, Object> row ) throws IOException
        {
            for ( String key : columns )
            {
                out.writeObject( row.get( key ) );
            }
        }
    },
    graph( FieldType.OBJECT )
    {
        @Override
        void formatRow( JsonGenerator out, Iterable<String> columns, Map<String, Object> row ) throws IOException
        {
            Set<Node> nodes = new HashSet<>();
            Set<Relationship> relationships = new HashSet<>();
            extract( nodes, relationships, row.values() );
            writeNodes( out, nodes );
            writeRelationships( out, relationships );
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

        private void writeProperties( JsonGenerator out, PropertyContainer container )  throws IOException
        {
            out.writeObjectFieldStart( "properties" );
            try
            {
                for ( String key : container.getPropertyKeys() )
                {
                    out.writeObjectField( key, container.getProperty( key ) );
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
    };

    private static final ResultDataContent[] DEFAULTS = {row};

    private final FieldType fieldType;

    private ResultDataContent( FieldType fieldType )
    {
        this.fieldType = fieldType;
    }

    public final void write( JsonGenerator out, Iterable<String> columns, Map<String, Object> row ) throws IOException
    {
        fieldType.write( this, out, columns, row );
    }

    abstract void formatRow( JsonGenerator out, Iterable<String> columns, Map<String, Object> row ) throws IOException;

    public static ResultDataContent[] fromNamesOrDefault( List<?> names )
    {
        if ( names == null || names.isEmpty() )
        {
            return DEFAULTS;
        }
        ResultDataContent[] result = new ResultDataContent[names.size()];
        Iterator<?> name = names.iterator();
        for ( int i = 0; i < result.length; i++ )
        {
            Object contentName = name.next();
            if ( contentName instanceof String )
            {
                try
                {
                    result[i] = valueOf( ((String) contentName).toLowerCase() );
                }
                catch ( IllegalArgumentException e )
                {
                    throw new IllegalArgumentException( "Invalid result data content specifier: " + contentName );
                }
            }
            else
            {
                throw new IllegalArgumentException( "Invalid result data content specifier: " + contentName );
            }
        }
        return result;
    }

    public static ResultDataContent[] atLeastDefault( ResultDataContent... formats )
    {
        return formats == null || formats.length == 0 ? DEFAULTS : formats;
    }

    private enum FieldType
    {
        OBJECT
        {
            @Override
            void write( ResultDataContent format, JsonGenerator out, Iterable<String> columns, Map<String, Object> row )
                    throws IOException
            {
                out.writeObjectFieldStart( format.name() );
                try
                {
                    format.formatRow( out, columns, row );
                }
                finally
                {
                    out.writeEndObject();
                }
            }
        },
        ARRAY
        {
            @Override
            void write( ResultDataContent format, JsonGenerator out, Iterable<String> columns, Map<String, Object> row )
                    throws IOException
            {
                out.writeArrayFieldStart( format.name() );
                try
                {
                    format.formatRow( out, columns, row );
                }
                finally
                {
                    out.writeEndArray();
                }
            }
        };

        abstract void write( ResultDataContent format, JsonGenerator out, Iterable<String> columns, Map<String, Object> row )
                throws IOException;
    }
}
