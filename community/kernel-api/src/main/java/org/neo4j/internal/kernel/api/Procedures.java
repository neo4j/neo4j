/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.internal.kernel.api;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.values.AnyValue;

public interface Procedures
{
    UserFunctionHandle functionGet( QualifiedName name );

    UserFunctionHandle aggregationFunctionGet( QualifiedName name );

    /** Invoke a read-only function by name
     * @param id the id of the function.
     * @param arguments the function arguments.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    AnyValue functionCall( int id, AnyValue[] arguments ) throws ProcedureException;

    /** Invoke a read-only function by name, and set the transaction's access mode to
     * {@link AccessMode.Static#READ READ} for the duration of the function execution.
     * @param id the id of the function.
     * @param arguments the function arguments.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    AnyValue functionCallOverride( int id, AnyValue[] arguments ) throws ProcedureException;

    /**
     * Create a read-only aggregation function by name
     * @param id the id of the function
     * @return the aggregation function
     * @throws ProcedureException
     */
    UserAggregator aggregationFunction( int id ) throws ProcedureException;

    /** Invoke a read-only aggregation function by name, and set the transaction's access mode to
     * {@link AccessMode.Static#READ READ} for the duration of the function execution.
     * @param id the id of the function.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    UserAggregator aggregationFunctionOverride( int id ) throws ProcedureException;

}
