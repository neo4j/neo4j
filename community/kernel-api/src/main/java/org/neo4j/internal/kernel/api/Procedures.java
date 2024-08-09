/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api;

import java.util.stream.Stream;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.values.AnyValue;

public interface Procedures {
    /**
     * Get a handle to the given function
     *
     * @param name  the name of the function
     * @param scope
     * @return A handle to the function or null if no function was found.
     */
    UserFunctionHandle functionGet(QualifiedName name, CypherScope scope);

    /**
     * Fetch all non-aggregating functions
     * @return all non-aggregating functions
     */
    Stream<UserFunctionSignature> functionGetAll(CypherScope scope);

    /**
     * Get a handle to the given aggregation function
     *
     * @param name  the name of the function
     * @param scope
     * @return A handle to the function or null if no function was found.
     */
    UserFunctionHandle aggregationFunctionGet(QualifiedName name, CypherScope scope);

    /**
     * Fetch all aggregating functions
     * @return all aggregating functions
     */
    Stream<UserFunctionSignature> aggregationFunctionGetAll(CypherScope scope);

    /**
     * Fetch a procedure handle
     *
     * @param name  the name of the procedure
     * @param scope
     * @return a procedure handle
     * @throws ProcedureException if there is no procedure was found for the name.
     */
    ProcedureHandle procedureGet(QualifiedName name, CypherScope scope) throws ProcedureException;

    /**
     * Fetch all procedures
     * @return all procedures
     * @throws ProcedureException
     */
    Stream<ProcedureSignature> proceduresGetAll(CypherScope scope) throws ProcedureException;

    /**
     * Invoke a read-only procedure by id.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @param context the procedure call context.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    ResourceRawIterator<AnyValue[], ProcedureException> procedureCallRead(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException;

    /**
     * Invoke a read/write procedure by id.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @param context the procedure call context.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    ResourceRawIterator<AnyValue[], ProcedureException> procedureCallWrite(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException;

    /**
     * Invoke a schema write procedure by id.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @param context the procedure call context.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    ResourceRawIterator<AnyValue[], ProcedureException> procedureCallSchema(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException;

    /**
     * Invoke a dbms procedure by id.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @param context the procedure call context.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    ResourceRawIterator<AnyValue[], ProcedureException> procedureCallDbms(
            int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException;

    /** Invoke a read-only function by id
     * @param id the id of the function.
     * @param arguments the function arguments.
     * @param context the procedure call context.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    AnyValue functionCall(int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException;

    /** Invoke a read-only built in function by id
     * @param id the id of the function.
     * @param arguments the function arguments.
     * @param context the procedure call context.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    AnyValue builtInFunctionCall(int id, AnyValue[] arguments, ProcedureCallContext context) throws ProcedureException;

    /**
     * Create a read-only aggregation function by id
     * @param id the id of the function
     * @param context the procedure call context.
     * @return the aggregation function
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    UserAggregationReducer aggregationFunction(int id, ProcedureCallContext context) throws ProcedureException;

    /**
     * Create a read-only built-in aggregation function by id
     * @param id the id of the function
     * @param context the procedure call context.
     * @return the aggregation function
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    UserAggregationReducer builtInAggregationFunction(int id, ProcedureCallContext context) throws ProcedureException;

    long signatureVersion();
}
