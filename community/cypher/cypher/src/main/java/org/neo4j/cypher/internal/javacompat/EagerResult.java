/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.javacompat;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;

import static java.lang.System.lineSeparator;

/**
 * Result produced as result of eager query execution for cases when {@link SnapshotExecutionEngine} is used.
 */
class EagerResult implements Result, QueryResultProvider
{
    private static final String ITEM_SEPARATOR = ", ";
    private final Result originalResult;
    private final VersionContext versionContext;
    private final List<Map<String, Object>> queryResult = new ArrayList<>();
    private int cursor;

    EagerResult( Result result, VersionContext versionContext )
    {
        this.originalResult = result;
        this.versionContext = versionContext;
    }

    public void consume()
    {
        while ( originalResult.hasNext() )
        {
            queryResult.add( originalResult.next() );
        }
    }

    @Override
    public QueryExecutionType getQueryExecutionType()
    {
        return originalResult.getQueryExecutionType();
    }

    @Override
    public List<String> columns()
    {
        return originalResult.columns();
    }

    @Override
    public <T> ResourceIterator<T> columnAs( String name )
    {
        return new EagerResultResourceIterator<>( name );
    }

    @Override
    public boolean hasNext()
    {
        return cursor < queryResult.size();
    }

    @Override
    public Map<String,Object> next()
    {
        return queryResult.get( cursor++ );
    }

    @Override
    public void close()
    {
        // nothing to close. Original result is already closed at this point
    }

    @Override
    public QueryStatistics getQueryStatistics()
    {
        return originalResult.getQueryStatistics();
    }

    @Override
    public ExecutionPlanDescription getExecutionPlanDescription()
    {
        return originalResult.getExecutionPlanDescription();
    }

    @Override
    public QueryResult queryResult()
    {
        return new EagerQueryResult();
    }

    @Override
    public String resultAsString()
    {
        List<String> columns = originalResult.columns();
        StringBuilder builder = new StringBuilder();
        builder.append( String.join( ITEM_SEPARATOR, columns ) );
        if ( !queryResult.isEmpty() )
        {
            builder.append( lineSeparator() );
            int numberOfColumns = columns.size();
            for ( Map<String,Object> row : queryResult )
            {
                writeRow( columns, builder, numberOfColumns, row );
                builder.append( lineSeparator() );
            }
        }
        return builder.toString();
    }

    @Override
    public void writeAsStringTo( PrintWriter writer )
    {
        writer.print( resultAsString() );
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException( "Not supported" );
    }

    @Override
    public Iterable<Notification> getNotifications()
    {
        return originalResult.getNotifications();
    }

    @Override
    public <VisitationException extends Exception> void accept( ResultVisitor<VisitationException> visitor )
            throws VisitationException
    {
        try
        {
            for ( Map<String,Object> map : queryResult )
            {
                visitor.visit( new MapRow( map ) );
            }
            checkIfDirty();
        }
        catch ( NotFoundException e )
        {
            checkIfDirty();
            throw e;
        }
    }

    private void checkIfDirty()
    {
        if ( versionContext.isDirty() )
        {
            throw new QueryExecutionKernelException(
                    new UnstableSnapshotException( "Unable to get clean data snapshot for query serialisation." ) )
                    .asUserException();
        }
    }

    private void writeRow( List<String> columns, StringBuilder builder, int numberOfColumns, Map<String,Object> row )
    {
        for ( int i = 0; i < numberOfColumns; i++ )
        {
            builder.append( row.get( columns.get( i ) ) );
            if ( i != numberOfColumns - 1 )
            {
                builder.append( ITEM_SEPARATOR );
            }
        }
    }

    private class EagerResultResourceIterator<T> implements ResourceIterator<T>
    {
        private final String column;
        int cursor;

        EagerResultResourceIterator( String column )
        {
            this.column = column;
        }

        @Override
        public boolean hasNext()
        {
            return cursor < queryResult.size();
        }

        @Override
        public T next()
        {
            return (T) queryResult.get( cursor++ ).get( column );
        }

        @Override
        public void close()
        {
            // Nothing to close.
        }
    }

    private class EagerQueryResult implements QueryResult
    {

        private final String[] fields;

        EagerQueryResult()
        {
            fields = originalResult.columns().toArray( new String[0] );
        }

        @Override
        public String[] fieldNames()
        {
            return fields;
        }

        @Override
        public <E extends Exception> void accept( QueryResultVisitor<E> visitor ) throws E
        {
            while ( hasNext() )
            {
                Map<String,Object> row = next();
                AnyValue[] anyValues = new AnyValue[fields.length];

                for ( int i = 0; i < fields.length; i++ )
                {
                    anyValues[i] = ValueUtils.of( row.get( fields[i] ) );
                }

                visitor.visit( () -> anyValues );
            }
        }

        @Override
        public QueryExecutionType executionType()
        {
            return originalResult.getQueryExecutionType();
        }

        @Override
        public QueryStatistics queryStatistics()
        {
            return originalResult.getQueryStatistics();
        }

        @Override
        public ExecutionPlanDescription executionPlanDescription()
        {
            return originalResult.getExecutionPlanDescription();
        }

        @Override
        public Iterable<Notification> getNotifications()
        {
            return originalResult.getNotifications();
        }

        @Override
        public void close()
        {
            // nothing to close
        }
    }
}
