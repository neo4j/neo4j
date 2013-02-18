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
package org.neo4j.shell.kernel.apps.cypher;

import org.neo4j.cypher.javacompat.QueryStatistics;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.shell.Output;

import java.lang.reflect.Array;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ResultPrinter
{

    public void outputResults( List<String> columns, Iterable<Map<String, Object>> result, long time, QueryStatistics queryStatistics, Output out ) throws RemoteException
    {
        Collection<Map<String, Object>> rows = ( result instanceof Collection ) ? (Collection<Map<String, Object>>) result : IteratorUtil.asCollection( result );

        Map<String, Integer> columnSizes = calculateColumnSizes( columns, rows );
        int totalWidth = totalWith( columnSizes.values() );

        final boolean hasData = !columns.isEmpty();
        if ( hasData )
        {
            String _________ = "+" + repeat( '-', totalWidth ) + "+";

            out.println( _________ );
            out.println( row( columns, columnSizes ) );
            out.println( _________ );
            for ( Map<String, Object> row : rows )
            {
                out.println( row( columns, columnSizes, row ) );
            }
            out.println( _________ );
        }
        out.println( rowsTime( time, rows.size() ) );
        final String stats = info( queryStatistics, hasData );
        if ( !stats.isEmpty() )
        {
            out.println( stats );
        }
    }

    private String rowsTime( long time, int rowCount )
    {
        return rowCount + ( rowCount == 1 ? " row" : " rows" ) + "\n" + time + " ms";
    }

    private String row( List<String> columns, Map<String, Integer> columnSizes )
    {
        StringBuilder sb = new StringBuilder( "| " );
        final int size = columns.size();
        for ( int i = 0; i < size; i++ )
        {
            String column = columns.get( i );
            Integer width = columnSizes.get( column );
            String padding = repeat( ' ', width - column.length() - 1 );
            sb.append( column ).append( padding ).append( "|" );
            if ( i < size - 1 )
            {
                sb.append( " " );
            }
        }
        return sb.toString();
    }

    private String row( List<String> columns, Map<String, Integer> columnSizes, Map<String, Object> row )
    {
        StringBuilder sb = new StringBuilder( "| " );
        final int size = columns.size();
        for ( int i = 0; i < size; i++ )
        {
            String column = columns.get( i );
            Integer width = columnSizes.get( column );
            String text = text( row.get( column ) );
            String padding = repeat( ' ', width - text.length() - 1 );
            sb.append( text ).append( padding ).append( "|" );
            if ( i < size - 1 )
            {
                sb.append( " " );
            }
        }
        return sb.toString();
    }

    private String repeat( char s, int width )
    {
        if ( width < 0 )
        {
            return "";
        }
        char[] chars = new char[width];
        Arrays.fill( chars, s );
        return String.valueOf( chars );
    }

    private int totalWith( Collection<Integer> values )
    {
        int sum = values.size() - 1; // borders
        for ( Integer value : values )
        {
            sum += value;
        }
        return sum;
    }

    private Map<String, Integer> calculateColumnSizes( List<String> columns, Collection<Map<String, Object>> rows )
    {
        Map<String, Integer> sizes = new HashMap<String, Integer>();
        for ( String column : columns )
        {
            sizes.put( column, column.length() + 2 );
        }
        for ( Map<String, Object> row : rows )
        {
            for ( String column : columns )
            {
                sizes.put( column, Math.max( sizes.get( column ), text( row.get( column ) ).length() + 2 ) );
            }
        }
        return sizes;
    }

    private String text( Object value )
    {
        if ( value == null )
        {
            return "<null>";
        }
        if ( value instanceof String )
        {
            return "\"" + value + "\"";
        }
        if ( value instanceof Node )
        {
            return value.toString() + props( (PropertyContainer) value );
        }
        if ( value instanceof Relationship )
        {
            Relationship rel = (Relationship) value;
            return ":" + rel.getType().name() + "[" + rel.getId() + "] " + props( rel );
        }
        if ( value instanceof Iterable )
        {
            return formatIterator( ( (Iterable) value ).iterator() );
        }
        if ( value.getClass().isArray() )
        {
            return formatArray( value );
        }
        return value.toString();
    }

    private String formatArray( Object array )
    {
        final StringBuilder sb = new StringBuilder( "[" );
        final int size = Array.getLength( array );
        for ( int i = 0; i < size; i++ )
        {
            sb.append( text( Array.get( array, i ) ) );
            if ( i < size - 1 )
            {
                sb.append( "," );
            }
        }
        sb.append( "]" );
        return sb.toString();
    }

    private String formatIterator( Iterator it )
    {
        final StringBuilder sb = new StringBuilder( "[" );
        while ( it.hasNext() )
        {
            sb.append( text( it.next() ) );
            if ( it.hasNext() )
            {
                sb.append( "," );
            }
        }
        sb.append( "]" );
        return sb.toString();
    }

    private String props( PropertyContainer pc )
    {
        final StringBuilder sb = new StringBuilder( "{" );
        final Iterator<String> keys = pc.getPropertyKeys().iterator();
        while ( keys.hasNext() )
        {
            String prop = keys.next();
            sb.append( prop ).append( ":" );
            final Object value = pc.getProperty( prop );
            sb.append( text( value ) );
            if ( keys.hasNext() )
            {
                sb.append( "," );
            }
        }
        sb.append( "}" );
        return sb.toString();
    }

    private String info( QueryStatistics queryStatistics, boolean hasData )
    {
        boolean hasStatistics = queryStatistics != null && queryStatistics.containsUpdates();
        if ( hasData )
        {
            if ( hasStatistics )
            {
                return toString( queryStatistics );
            }
            return "";
        }
        else
        {
            if ( hasStatistics )
            {
                return
                        "+-------------------+\n" +
                                "| No data returned. |\n" +
                                "+-------------------+\n" +
                                toString( queryStatistics );
            }
            else
            {
                return
                        "+--------------------------------------------+\n" +
                                "| No data returned, and nothing was changed. |\n" +
                                "+--------------------------------------------+\n";
            }

        }
    }

    private String toString( QueryStatistics queryStatistics )
    {
        if ( !queryStatistics.containsUpdates() )
        {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        addIfNonZero( sb, "Nodes created: ",
                queryStatistics.getNodesCreated() );
        addIfNonZero( sb, "Relationships created: ",
                queryStatistics.getRelationshipsCreated() );
        addIfNonZero( sb, "Properties set: ",
                queryStatistics.getPropertiesSet() );
        addIfNonZero( sb, "Nodes deleted: ",
                queryStatistics.getDeletedNodes() );
        addIfNonZero( sb, "Relationships deleted: ",
                queryStatistics.getDeletedRelationships() );

        return sb.toString();
    }

    private void addIfNonZero( StringBuilder builder, String message, int count )
    {
        if ( count > 0 )
        {
            builder.append( message ).append( count ).append( "\n" );
        }
    }
}
