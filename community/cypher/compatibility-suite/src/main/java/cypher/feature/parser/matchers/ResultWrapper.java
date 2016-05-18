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
package cypher.feature.parser.matchers;

import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.traversal.Paths;

public class ResultWrapper implements Result
{
    private final Result inner;
    private final StringBuilder builder;
    private int rowCounter = 1;

    public ResultWrapper( Result inner )
    {
        this.inner = inner;
        this.builder = new StringBuilder( "Actual result of:\n" );
    }

    @Override
    public String toString()
    {
        return builder.toString();
    }

    @Override
    public QueryExecutionType getQueryExecutionType()
    {
        return inner.getQueryExecutionType();
    }

    @Override
    public List<String> columns()
    {
        return inner.columns();
    }

    @Override
    public <T> ResourceIterator<T> columnAs( String name )
    {
        return inner.columnAs( name );
    }

    @Override
    public boolean hasNext()
    {
        return inner.hasNext();
    }

    @Override
    public Map<String,Object> next()
    {
        Map<String,Object> next = inner.next();
        builder.append( "[" ).append( rowCounter++ ).append( "]   " );
        builder.append( "actualRow:" );
        builder.append( serialize( next ) );
        builder.append( "\n" );
        return next;
    }

    private String serialize( Object obj )
    {
        StringBuilder sb = new StringBuilder();
        if ( obj == null )
        {
            sb.append( "null" );
        }
        else if ( obj instanceof Node )
        {
            Node n = (Node) obj;
            sb.append( "(" );
            n.getLabels().forEach( ( l ) -> sb.append( ":" ).append( l.name() ) );
            sb.append( " {" );
            String[] comma = new String[]{ "" };
            n.getAllProperties().forEach( ( k, v ) -> {
                sb.append( comma[0] ).append( k ).append( ": " ).append( serialize( v ) );
                comma[0] = ", ";
            } );
            sb.append( "})" );
        }
        else if ( obj instanceof Relationship )
        {
            Relationship r = (Relationship) obj;
            sb.append( "[:" );
            sb.append( r.getType().name() );
            sb.append( " {" );
            String[] comma = new String[]{ "" };
            r.getAllProperties().forEach( ( k, v ) -> {
                sb.append( comma[0] ).append( k ).append( ": " ).append( serialize( v ) );
                comma[0] = ", ";
            } );
            sb.append( "}]" );
        }
        else if ( obj instanceof Path )
        {
            Path p = (Path) obj;
            sb.append( "<" );
            sb.append( Paths.pathToString( p, new Paths.PathDescriptor<Path>()
            {
                @Override
                public String nodeRepresentation( Path path, Node node )
                {
                    return serialize( node );
                }

                @Override
                public String relationshipRepresentation( Path path,
                        Node from, Relationship relationship )
                {
                    String prefix = "-", suffix = "-";
                    if ( from.equals( relationship.getEndNode() ) )
                    {
                        prefix = "<-";
                    }
                    else
                    {
                        suffix = "->";
                    }
                    return prefix + serialize( relationship ) + suffix;
                }
            } ) );
            sb.append( ">" );
        }
        else if ( obj instanceof String )
        {
            sb.append( "'" ).append( obj.toString() ).append( "'" );
        }
        else if ( obj instanceof List )
        {
            List<?> list = (List) obj;
            List<String> output = new ArrayList<>( list.size() );
            list.forEach( item -> output.add( serialize( item ) ) );
            sb.append( output );
        }
        else if ( obj.getClass().isArray() )
        {
            List<Object> list = new ArrayList<>();
            for ( int i = 0; i < Array.getLength( obj ); ++i )
            {
                list.add( Array.get( obj, i ) );
            }
            sb.append( serialize( list ) );
        }
        else if ( obj instanceof Map )
        {
            Map<?,?> map = (Map) obj;
            Map<String,String> output = new HashMap<>( map.size() );
            map.forEach( ( k, v ) -> output.put( k.toString(), serialize( v ) ) );
            sb.append( output );
        }
        else
        {
            sb.append( obj.toString() );
        }

        return sb.toString();
    }

    @Override
    public void close()
    {
        inner.close();
    }

    @Override
    public QueryStatistics getQueryStatistics()
    {
        return inner.getQueryStatistics();
    }

    @Override
    public ExecutionPlanDescription getExecutionPlanDescription()
    {
        return inner.getExecutionPlanDescription();
    }

    @Override
    public String resultAsString()
    {
        return inner.resultAsString();
    }

    @Override
    public void writeAsStringTo( PrintWriter writer )
    {
        inner.writeAsStringTo( writer );
    }

    @Override
    public void remove()
    {
        inner.remove();
    }

    @Override
    public Iterable<Notification> getNotifications()
    {
        return inner.getNotifications();
    }

    @Override
    public <VisitationException extends Exception> void accept(
            ResultVisitor<VisitationException> visitor ) throws VisitationException
    {
        inner.accept( visitor );
    }

    @Override
    public Stream<Map<String,Object>> stream()
    {
        return inner.stream();
    }

    @Override
    public <R> ResourceIterator<R> map(
            Function<Map<String,Object>,R> map )
    {
        return inner.map( map );
    }

    @Override
    public void forEachRemaining(
            Consumer<? super Map<String,Object>> action )
    {
        inner.forEachRemaining( action );
    }
}
