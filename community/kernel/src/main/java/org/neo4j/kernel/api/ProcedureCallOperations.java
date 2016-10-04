/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.kernel.api.security.AccessMode;

/**
 * Specifies procedure call operations for the three types of procedure calls that can be made.
 */
public interface ProcedureCallOperations
{
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
     * Invoke a read-only procedure by name, and override the transaction's access mode with
     * the given access mode for the duration of the procedure execution.
     * @param name the name of the procedure.
     * @param arguments the procedure arguments.
     * @param override the access mode to be used for the execution of the procedure.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallRead( QualifiedName name, Object[] arguments, AccessMode override )
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
     * Invoke a read/write procedure by name, and override the transaction's access mode with
     * the given access mode for the duration of the procedure execution.
     * @param name the name of the procedure.
     * @param arguments the procedure arguments.
     * @param override the access mode to be used for the execution of the procedure.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallWrite( QualifiedName name, Object[] arguments, AccessMode override )
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
     * Invoke a schema write procedure by name, and override the transaction's access mode with
     * the given access mode for the duration of the procedure execution.
     * @param name the name of the procedure.
     * @param arguments the procedure arguments.
     * @param override the access mode to be used for the execution of the procedure.
     * @return an iterator containing the procedure results.
     * @throws ProcedureException if there was an exception thrown during procedure execution.
     */
    RawIterator<Object[], ProcedureException> procedureCallSchema( QualifiedName name, Object[] arguments, AccessMode override )
            throws ProcedureException;
}
