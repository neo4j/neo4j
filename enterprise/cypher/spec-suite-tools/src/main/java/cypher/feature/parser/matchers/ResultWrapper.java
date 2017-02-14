/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package cypher.feature.parser.matchers;

import java.io.PrintWriter;
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
        builder.append( TckSerializer.serialize( next ) );
        builder.append( "\n" );
        return next;
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
        inner.accept( new ResultVisitorWrapper<>( visitor ) );
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

    private class ResultVisitorWrapper<T extends Exception> implements ResultVisitor<T> {

        private final ResultVisitor<T> inner;

        private ResultVisitorWrapper( ResultVisitor<T> visitor )
        {
            this.inner = visitor;
        }

        @Override
        public boolean visit( ResultRow row ) throws T
        {
            builder.append( "[" ).append( rowCounter++ ).append( "]   " );
            builder.append( "actualRow: {" );
            boolean result = inner.visit( new ResultRowWrapper( row ) );
            builder.delete( builder.length() - 2, builder.length() );
            builder.append( "}\n" );
            return result;
        }
    }

    private class ResultRowWrapper implements ResultRow {

        private final ResultRow inner;

        private ResultRowWrapper( ResultRow inner )
        {
            this.inner = inner;
        }

        @Override
        public Node getNode( String key )
        {
            Node node = inner.getNode( key );
            builder.append( key ).append( "=" ).append( TckSerializer.serialize( node ) ).append(", ");
            return node;
        }

        @Override
        public Relationship getRelationship( String key )
        {
            Relationship relationship = inner.getRelationship( key );
            builder.append( key ).append( "=" ).append( TckSerializer.serialize( relationship ) ).append(", ");
            return relationship;
        }

        @Override
        public Object get( String key )
        {
            Object obj = inner.get( key );
            builder.append( key ).append( "=" ).append( TckSerializer.serialize( obj ) ).append(", ");
            return obj;
        }

        @Override
        public String getString( String key )
        {
            String string = inner.getString( key );
            builder.append( key ).append( "=" ).append( TckSerializer.serialize( string ) ).append(", ");
            return string;
        }

        @Override
        public Number getNumber( String key )
        {
            Number number = inner.getNumber( key );
            builder.append( key ).append( "=" ).append( TckSerializer.serialize( number ) ).append(", ");
            return number;
        }

        @Override
        public Boolean getBoolean( String key )
        {
            Boolean aBoolean = inner.getBoolean( key );
            builder.append( key ).append( "=" ).append( TckSerializer.serialize( aBoolean ) ).append(", ");
            return aBoolean;
        }

        @Override
        public Path getPath( String key )
        {
            Path path = inner.getPath( key );
            builder.append( key ).append( "=" ).append( TckSerializer.serialize( path ) ).append(", ");
            return path;
        }
    }
}
