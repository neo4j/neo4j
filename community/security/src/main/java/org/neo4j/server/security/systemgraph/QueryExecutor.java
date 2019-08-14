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
package org.neo4j.server.security.systemgraph;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.exceptions.CypherExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

public interface QueryExecutor
{
    void executeQuery( String query, Map<String,Object> params, ErrorPreservingQuerySubscriber subscriber );

    Transaction beginTx();

    default long executeQueryLong( String query )
    {
        return executeQueryLong( query, Collections.emptyMap() );
    }

    default long executeQueryLong( String query, Map<String,Object> params )
    {
        MutableLong count = new MutableLong( -1 );

        ErrorPreservingQuerySubscriber subscriber = new ErrorPreservingQuerySubscriber()
        {
            private int currentOffset = -1;

            @Override
            public void onRecordCompleted()
            {
                currentOffset = -1;
            }

            @Override
            public void onRecord() throws Exception
            {
                super.onRecord();
                currentOffset = 0;
            }

            @Override
            public void onField( AnyValue value )
            {
                if ( currentOffset == 0 )
                {
                    count.setValue( ((NumberValue) value).longValue() );
                }
                currentOffset++;
            }
        };

        executeQuery( query, params, subscriber );
        return count.getValue();
    }

    default void executeQueryWithConstraint( String query, Map<String,Object> params, String failureMessage ) throws InvalidArgumentsException
    {
        ErrorPreservingQuerySubscriber subscriber = new ErrorPreservingQuerySubscriber();
        executeQuery( query, params, subscriber );
        Throwable error = subscriber.errorOrNull();

        if ( error != null )
        {
            if ( error instanceof CypherExecutionException &&
                 ((CypherExecutionException) error).status() == Status.Schema.ConstraintValidationFailed )
            {
                throw new InvalidArgumentsException( failureMessage );
            }
            else if ( error instanceof RuntimeException )
            {
                throw (RuntimeException) error;
            }
            else
            {
                throw new InvalidArgumentsException( error.getMessage(), error );
            }
        }
    }

    default boolean executeQueryWithParamCheck( String query, Map<String,Object> params )
    {
        MutableBoolean paramCheck = new MutableBoolean( false );

        ErrorPreservingQuerySubscriber subscriber = new ErrorPreservingQuerySubscriber()
        {
            @Override
            public void onRecord()
            {
                paramCheck.setTrue(); // If we get a result row, we know that the user and/or role specified in the params exist
            }
        };
        executeQuery( query, params, subscriber );
        return paramCheck.getValue();
    }

    default boolean executeQueryWithParamCheck( String query, Map<String,Object> params, String errorMsg ) throws InvalidArgumentsException
    {
        boolean paramCheck = executeQueryWithParamCheck( query, params );

        if ( !paramCheck )
        {
            throw new InvalidArgumentsException( errorMsg );
        }
        return true;
    }

    default Set<String> executeQueryWithResultSet( String query )
    {
        Set<String> resultSet = new TreeSet<>();

        ErrorPreservingQuerySubscriber subscriber = new ErrorPreservingQuerySubscriber()
        {
            private int currentOffset = -1;

            @Override
            public void onRecord() throws Exception
            {
                currentOffset = 0;
            }

            @Override
            public void onRecordCompleted() throws Exception
            {
                currentOffset = -1;
            }

            @Override
            public void onField( AnyValue value )
            {
                if ( currentOffset == 0 )
                {
                    resultSet.add( ((TextValue) value).stringValue() );
                }
                currentOffset++;
            }
        };

        executeQuery( query, Collections.emptyMap(), subscriber );
        return resultSet;
    }

    default Set<String> executeQueryWithResultSetAndParamCheck( String query, Map<String,Object> params, String errorMsg ) throws InvalidArgumentsException
    {
        MutableBoolean success = new MutableBoolean( false );
        Set<String> resultSet = new TreeSet<>();

        ErrorPreservingQuerySubscriber subscriber = new ErrorPreservingQuerySubscriber()
        {
            private int currentOffset = -1;

            @Override
            public void onRecordCompleted()
            {
                currentOffset = -1;
            }

            @Override
            public void onRecord()
            {
                currentOffset = 0;
                success.setTrue();// If we get a row we know that the parameter existed in the system db
            }

            @Override
            public void onField( AnyValue value )
            {
                try
                {
                    if ( currentOffset == 0 && value != Values.NO_VALUE )
                    {
                        resultSet.add( ((TextValue) value).stringValue() );
                    }
                }
                finally
                {
                    currentOffset++;
                }
            }
        };

        executeQuery( query, params, subscriber );

        if ( success.isFalse() )
        {
            throw new InvalidArgumentsException( errorMsg );
        }
        return resultSet;
    }
}
