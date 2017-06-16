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
package org.neo4j.values.result;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.values.AnyValue;

import static org.neo4j.helpers.Exceptions.withCause;

public interface QueryResult
{
    String[] fieldNames();

    <E extends Exception> void accept( QueryResultVisitor<E> visitor )
            throws E;

    interface QueryResultVisitor<E extends Exception>
    {
        boolean visit( Record row ) throws E;
    }

    interface Record
    {
        AnyValue[] fields();
    }

    QueryExecutionType getQueryExecutionType();

    QueryStatistics getQueryStatistics();

    ExecutionPlanDescription getExecutionPlanDescription();

    Iterable<Notification> getNotifications();

    Result asPublicResult();

    void close();
}

abstract class IterableQueryResult implements QueryResult, Result
{
    private final Map<String,Integer> indexLookup;

    IterableQueryResult()
    {
        String[] names = fieldNames();
        indexLookup = new HashMap<>( names.length );
        for ( int i = 0; i < names.length; i++ )
        {
            indexLookup.put( names[i], i );
        }
    }

    @Override
    public <VisitationException extends Exception> void accept( ResultVisitor<VisitationException> visitor )
            throws VisitationException
    {
        accept( (QueryResultVisitor<VisitationException>) row -> visitor.visit( new ResultRow()
        {
            @Override
            public Node getNode( String key )
            {
                return get( key, Node.class );
            }

            @Override
            public Relationship getRelationship( String key )
            {
                return get( key, Relationship.class );
            }

            @Override
            public Object get( String key )
            {
                return get( key, Object.class );
            }

            @Override
            public String getString( String key )
            {
                return get( key, String.class );
            }

            @Override
            public Number getNumber( String key )
            {
                return get( key, Number.class );
            }

            @Override
            public Boolean getBoolean( String key )
            {
                return get( key, Boolean.class );
            }

            @Override
            public Path getPath( String key )
            {
                return get( key, Path.class );
            }

            private <T> T get( String key, Class<T> type )
            {
                if ( !indexLookup.containsKey( key ) )
                {
                    throw new NoSuchElementException( "No such entry: " + key );
                }
                Integer index = indexLookup.get( key );

                //TODO do this with a writer, this is wrong
                Object value = row.fields()[index];
                try
                {
                    return type.cast( value );
                }
                catch ( ClassCastException e )
                {
                    throw withCause( new NoSuchElementException(
                            "Element '" + key + "' is not a " + type.getSimpleName() ), e );
                }
            }
        } ) );
    }
}
