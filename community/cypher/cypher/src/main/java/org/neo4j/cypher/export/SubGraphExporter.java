/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.export;

import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;

public class SubGraphExporter
{
    private final SubGraph graph;

    public SubGraphExporter( SubGraph graph )
    {
        this.graph = graph;
    }

    public void export( PrintWriter out )
    {
        export(out, null, null);
    }

    public void export( PrintWriter out, String begin, String commit )
    {
        output( out, begin );
        appendIndexes( out );
        appendConstraints( out );
        output( out, commit, begin );
        long nodes = appendNodes( out );
        long relationships = appendRelationships( out );
        if ( nodes + relationships > 0 )
        {
            out.println( ";" );
        }
        output( out, commit );
    }

    private void output( PrintWriter out, String ... commands )
    {
        for ( String command : commands )
        {
            if ( command == null )
            {
                continue;
            }
            out.println( command );
        }
    }
    private Collection<String> exportIndexes()
    {
        final List<String> result = new ArrayList<>();
        for ( IndexDefinition index : graph.getIndexes() )
        {
            if ( !index.isConstraintIndex() )
            {
                Iterator<String> propertyKeys = index.getPropertyKeys().iterator();
                if ( !propertyKeys.hasNext() )
                {
                    throw new IllegalStateException( "Indexes should have at least one property key" );
                }
                String key = quote( propertyKeys.next() );
                if ( propertyKeys.hasNext() )
                {
                    throw new RuntimeException( "Exporting compound indexes is not implemented yet" );
                }

                String label = quote( index.getLabel().name() );
                result.add( "create index on :" + label + "(" + key + ")" );
            }
        }
        Collections.sort( result );
        return result;
    }

    private Collection<String> exportConstraints()
    {
        final List<String> result = new ArrayList<>();
        for ( ConstraintDefinition constraint : graph.getConstraints() )
        {
            if ( !constraint.isConstraintType( ConstraintType.UNIQUENESS ) )
            {
                throw new RuntimeException( "Exporting constraints other than uniqueness is not implemented yet" );
            }

            Iterator<String> propertyKeys = constraint.getPropertyKeys().iterator();
            if ( !propertyKeys.hasNext() )
            {
                throw new IllegalStateException( "Constraints should have at least one property key" );
            }
            String key = quote( propertyKeys.next() );
            if ( propertyKeys.hasNext() )
            {
                throw new RuntimeException( "Exporting compound constraints is not implemented yet" );
            }

            String label = quote( constraint.getLabel().name() );
            result.add( "create constraint on (n:" + label + ") assert n." + key + " is unique" );
        }
        Collections.sort( result );
        return result;
    }

    private String quote( String id )
    {
        return "`" + id + "`";
    }

    private String labelString( Node node )
    {
        Iterator<Label> labels = node.getLabels().iterator();
        if ( !labels.hasNext() )
        {
            return "";
        }

        StringBuilder result = new StringBuilder();
        while ( labels.hasNext() )
        {
            Label next = labels.next();
            result.append( ":" ).append( quote( next.name() ) );
        }
        return result.toString();
    }

    private String identifier( Node node )
    {
        return "_" + node.getId();
    }

    private void appendIndexes( PrintWriter out )
    {
        for ( String line : exportIndexes() )
        {
            out.print( line );
            out.println( ";" );
        }
    }

    private void appendConstraints( PrintWriter out )
    {
        for ( String line : exportConstraints() )
        {
            out.print( line );
            out.println( ";" );
        }
    }

    private long appendRelationships( PrintWriter out )
    {
        long relationships = 0;
        for ( Node node : graph.getNodes() )
        {
            for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
            {
                appendRelationship( out, rel );
                relationships++;
            }
        }
        return relationships;
    }

    private void appendRelationship( PrintWriter out, Relationship rel )
    {
        out.print( "create (" );
        out.print( identifier( rel.getStartNode() ) );
        out.print( ")-[:" );
        out.print( quote( rel.getType().name() ) );
        formatProperties( out, rel );
        out.print( "]->(" );
        out.print( identifier( rel.getEndNode() ) );
        out.println( ")" );
    }

    private long appendNodes( PrintWriter out )
    {
        long nodes = 0;
        for ( Node node : graph.getNodes() )
        {
            appendNode( out, node );
            nodes++;
        }
        return nodes;
    }

    private void appendNode( PrintWriter out, Node node )
    {
        out.print( "create (" );
        out.print( identifier( node ) );
        String labels = labelString( node );
        if ( !labels.isEmpty() )
        {
            out.print( labels );
        }
        formatProperties( out, node );
        out.println( ")" );
    }

    private void formatProperties( PrintWriter out, PropertyContainer pc )
    {
        if ( !pc.getPropertyKeys().iterator().hasNext() )
        {
            return;
        }
        out.print( " " );
        final String propertyString = formatProperties( pc );
        out.print( propertyString );
    }

    private String formatProperties( PropertyContainer pc )
    {
        StringBuilder result = new StringBuilder();
        List<String> keys = Iterables.asList( pc.getPropertyKeys() );
        Collections.sort( keys );
        for ( String prop : keys )
        {
            if ( result.length() > 0 )
            {
                result.append( ", " );
            }
            result.append( quote( prop ) ).append( ":" );
            Object value = pc.getProperty( prop );
            result.append( toString( value ) );
        }
        return "{" + result + "}";
    }

    private String toString( Iterator<?> iterator )
    {
        StringBuilder result = new StringBuilder();
        while ( iterator.hasNext() )
        {
            if ( result.length() > 0 )
            {
                result.append( ", " );
            }
            Object value = iterator.next();
            result.append( toString( value ) );
        }
        return "[" + result + "]";
    }

    private String arrayToString( Object value )
    {
        StringBuilder result = new StringBuilder();
        int length = Array.getLength( value );
        for ( int i = 0; i < length; i++ )
        {
            if ( i > 0 )
            {
                result.append( ", " );
            }
            result.append( toString( Array.get( value, i ) ) );
        }
        return "[" + result + "]";
    }

    private String escapeString( String value )
    {
        return "\"" + value.replaceAll( "\\\\", "\\\\\\\\" ).replaceAll( "\"", "\\\\\"" ) + "\"";
    }

    private String toString( Object value )
    {
        if ( value == null )
        {
            return "null";
        }
        if ( value instanceof String )
        {
            return escapeString( (String) value );
        }
        if ( value instanceof Float || value instanceof Double )
        {
            return String.format( Locale.ENGLISH, "%f", value );
        }
        if ( value instanceof Iterator )
        {
            return toString( (Iterator) value );
        }
        if ( value instanceof Iterable )
        {
            return toString( ((Iterable) value).iterator() );
        }
        if ( value.getClass().isArray() )
        {
            return arrayToString( value );
        }
        return value.toString();
    }
}
