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
package org.neo4j.internal.kernel.api;

import java.util.Set;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

public interface Procedures
{
    /**
     * Get a handle to the given function
     * @param name the name of the function
     * @return A handle to the function
     */
    UserFunctionHandle functionGet( QualifiedName name );

    /**
     * Get a handle to the given aggregation function
     * @param name the name of the function
     * @return A handle to the function
     */
    UserFunctionHandle aggregationFunctionGet( QualifiedName name );

    /**
     * Fetch a procedure handle
     * @param name the name of the procedure
     * @return a procedure handle
     * @throws ProcedureException
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
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallRead( int id, Object[] arguments )
            throws ProcedureException;

    /**
     * Invoke a read-only procedure by id, and set the transaction's access mode to
     * {@link AccessMode.Static#READ READ} for the duration of the procedure execution.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallReadOverride( int id, Object[] arguments )
            throws ProcedureException;

    /**
     * Invoke a read/write procedure by id.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallWrite( int id, Object[] arguments )
            throws ProcedureException;
    /**
     * Invoke a read/write procedure by id, and set the transaction's access mode to
     * {@link AccessMode.Static#WRITE WRITE} for the duration of the procedure execution.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallWriteOverride( int id, Object[] arguments )
            throws ProcedureException;

    /**
     * Invoke a schema write procedure by id.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallSchema( int id, Object[] arguments )
            throws ProcedureException;
    /**
     * Invoke a schema write procedure by id, and set the transaction's access mode to
     * {@link AccessMode.Static#FULL FULL} for the duration of the procedure execution.
     * @param id the id of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallSchemaOverride( int id, Object[] arguments )
            throws ProcedureException;

    /**
     * Invoke a read-only procedure by name.
     * @param name the name of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallRead( QualifiedName name, Object[] arguments )
            throws ProcedureException;

    /**
     * Invoke a read-only procedure by name, and set the transaction's access mode to
     * {@link AccessMode.Static#READ READ} for the duration of the procedure execution.
     * @param name the name of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallReadOverride( QualifiedName name, Object[] arguments )
            throws ProcedureException;

    /**
     * Invoke a read/write procedure by name.
     * @param name the name of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallWrite( QualifiedName name, Object[] arguments )
            throws ProcedureException;
    /**
     * Invoke a read/write procedure by name, and set the transaction's access mode to
     * {@link AccessMode.Static#WRITE WRITE} for the duration of the procedure execution.
     * @param name the name of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallWriteOverride( QualifiedName name, Object[] arguments )
            throws ProcedureException;

    /**
     * Invoke a schema write procedure by name.
     * @param name the name of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallSchema( QualifiedName name, Object[] arguments )
            throws ProcedureException;
    /**
     * Invoke a schema write procedure by name, and set the transaction's access mode to
     * {@link AccessMode.Static#FULL FULL} for the duration of the procedure execution.
     * @param name the name of the procedure.
     * @param arguments the procedure arguments.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallSchemaOverride( QualifiedName name, Object[] arguments )
            throws ProcedureException;

    /** Invoke a read-only function by id
     * @param id the id of the function.
     * @param arguments the function arguments.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    AnyValue functionCall( int id, AnyValue[] arguments ) throws ProcedureException;

    /** Invoke a read-only function by name
     * @param name the name of the function.
     * @param arguments the function arguments.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    AnyValue functionCall( QualifiedName name, AnyValue[] arguments ) throws ProcedureException;

    /** Invoke a read-only function by id, and set the transaction's access mode to
     * {@link AccessMode.Static#READ READ} for the duration of the function execution.
     * @param id the id of the function.
     * @param arguments the function arguments.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    AnyValue functionCallOverride( int id, AnyValue[] arguments ) throws ProcedureException;

    /** Invoke a read-only function by name, and set the transaction's access mode to
     * {@link AccessMode.Static#READ READ} for the duration of the function execution.
     * @param name the name of the function.
     * @param arguments the function arguments.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    AnyValue functionCallOverride( QualifiedName name, AnyValue[] arguments ) throws ProcedureException;

    /**
     * Create a read-only aggregation function by id
     * @param id the id of the function
     * @return the aggregation function
     * @throws ProcedureException
     */
    UserAggregator aggregationFunction( int id ) throws ProcedureException;

    /**
     * Create a read-only aggregation function by name
     * @param name the name of the function
     * @return the aggregation function
     * @throws ProcedureException
     */
    UserAggregator aggregationFunction( QualifiedName name ) throws ProcedureException;

    /** Invoke a read-only aggregation function by id, and set the transaction's access mode to
     * {@link AccessMode.Static#READ READ} for the duration of the function execution.
     * @param id the id of the function.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    UserAggregator aggregationFunctionOverride( int id ) throws ProcedureException;

    /** Invoke a read-only aggregation function by name, and set the transaction's access mode to
     * {@link AccessMode.Static#READ READ} for the duration of the function execution.
     * @param name the name of the function.
     * @throws ProcedureException if there was an exception thrown during function execution.
     */
    UserAggregator aggregationFunctionOverride( QualifiedName name ) throws ProcedureException;

    /**
     * Retrieve a value mapper for mapping values to regular Java objects.
     * @return a value mapper that maps to Java objects.
     */
    ValueMapper<Object> valueMapper();
}
