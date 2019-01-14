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
package org.neo4j.kernel.impl.query;

import java.util.Map;

import org.neo4j.graphdb.Result;
import org.neo4j.values.virtual.MapValue;

public interface QueryExecutionEngine
{
    Result executeQuery( String query, MapValue parameters, TransactionalContext context )
            throws QueryExecutionKernelException;

    Result executeQuery( String query, Map<String,Object> parameters, TransactionalContext context )
            throws QueryExecutionKernelException;

    Result profileQuery( String query, Map<String,Object> parameters, TransactionalContext context )
            throws QueryExecutionKernelException;

    boolean isPeriodicCommit( String query );

    String prettify( String query );

    long clearQueryCaches();
}

