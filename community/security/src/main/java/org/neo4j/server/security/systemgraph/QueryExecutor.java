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

import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public interface QueryExecutor
{
    void executeQuery( String query, Map<String,Object> params, QueryResult.QueryResultVisitor resultVisitor );

    Transaction beginTx();

    default long executeQueryLong( String query )
    {
        MutableLong count = new MutableLong();

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            count.setValue( ((NumberValue) row.fields()[0]).longValue() );
            return false;
        };

        executeQuery( query, Collections.emptyMap(), resultVisitor );
        return count.getValue();
    }

    default void executeQueryWithConstraint( String query, Map<String,Object> params, String failureMessage ) throws InvalidArgumentsException
    {
        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row -> true;

        try
        {
            executeQuery( query, params, resultVisitor );
        }
        catch ( Exception e )
        {
            if ( e instanceof QueryExecutionException &&
                    ( (QueryExecutionException) e).getStatusCode().contains( "ConstraintValidationFailed" ) )
            {
                throw new InvalidArgumentsException( failureMessage );
            }
            throw e;
        }
    }

    default boolean executeQueryWithParamCheck( String query, Map<String,Object> params )
    {
        MutableBoolean paramCheck = new MutableBoolean( false );

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            paramCheck.setTrue(); // If we get a result row, we know that the user and/or role specified in the params exist
            return true;
        };

        executeQuery( query, params, resultVisitor );
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

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            resultSet.add( ((TextValue) row.fields()[0]).stringValue() );
            return true;
        };

        executeQuery( query, Collections.emptyMap(), resultVisitor );
        return resultSet;
    }

    default Set<String> executeQueryWithResultSetAndParamCheck( String query, Map<String,Object> params, String errorMsg ) throws InvalidArgumentsException
    {
        MutableBoolean success = new MutableBoolean( false );
        Set<String> resultSet = new TreeSet<>();

        final QueryResult.QueryResultVisitor<RuntimeException> resultVisitor = row ->
        {
            success.setTrue(); // If we get a row we know that the parameter existed in the system db
            Value value = (Value) row.fields()[0];
            if ( value != Values.NO_VALUE )
            {
                resultSet.add( ((TextValue) value).stringValue() );
            }
            return true;
        };

        executeQuery( query, params, resultVisitor );

        if ( success.isFalse() )
        {
            throw new InvalidArgumentsException( errorMsg );
        }
        return resultSet;
    }
}
