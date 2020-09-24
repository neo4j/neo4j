/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.kernel.api;

import java.util.Set;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.values.AnyValue;

public interface Procedures
{
    /**
     * Get a handle to the given function
     * @param name the name of the function
     * @return A handle to the function or null if no function was found.
     */
    UserFunctionHandle functionGet( QualifiedName name );

    /**
     * Get a handle to the given aggregation function
     * @param name the name of the function
     * @return A handle to the function or null if no function was found.
     */
    UserFunctionHandle aggregationFunctionGet( QualifiedName name );

    /**
     * Fetch a procedure handle
     * @param name the name of the procedure
     * @return a procedure handle
     * @throws ProcedureException if there is no procedure was found for the name.
     */
    ProcedureHandle procedureGet( QualifiedName name ) throws ProcedureException;

    /**
     * Fetch all procedures
     * @return all procedures
     * @throws ProcedureException
     */
    Set<ProcedureSignature> proceduresGetAll( ) throws ProcedureException;

    /**
     * Invoke a read-only procedure by id.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @param context the procedure call context.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<AnyValue[], ProcedureException> procedureCallRead( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException;

    /**
     * Invoke a read/write procedure by id.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @param context the procedure call context.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<AnyValue[], ProcedureException> procedureCallWrite( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException;

    /**
     * Invoke a schema write procedure by id.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @param context the procedure call context.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<AnyValue[], ProcedureException> procedureCallSchema( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException;

    /**
     * Invoke a dbms procedure by id.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @param context the procedure call context.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<AnyValue[], ProcedureException> procedureCallDbms( int id, AnyValue[] arguments, ProcedureCallContext context )
            throws ProcedureException;

    /** Invoke a read-only function by id
     * @param id the id of the function.
     * @param arguments the function arguments.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    AnyValue functionCall( int id, AnyValue[] arguments ) throws ProcedureException;

    /**
     * Create a read-only aggregation function by id
     * @param id the id of the function
     * @return the aggregation function
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    UserAggregator aggregationFunction( int id ) throws ProcedureException;
}
