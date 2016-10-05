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

import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;

/**
 * A statement which is a smaller coherent unit of work inside a {@link KernelTransaction}.
 * There are accessors for different types of operations. The operations are divided into
 * read and write operations, write operations are divided into {@link #dataWriteOperations()} and
 * {@link #schemaWriteOperations()} since there can be only one type of write operations inside
 * any given transaction. In both cases {@link #tokenWriteOperations()} are allowed though,
 * which is why it can be accessed separately. The transaction, if still "undecided" about its
 * type of write operations, will be decided when calling either {@link #dataWriteOperations()}
 * or {@link #schemaWriteOperations()}, otherwise if already decided, verified so that it's
 * of the same type.
 */
public interface Statement extends Resource
{
    /**
     * @return interface exposing all read operations.
     */
    ReadOperations readOperations();

    /**
     * @return interface exposing all write operations about tokens.
     */
    TokenWriteOperations tokenWriteOperations();

    /**
     * @return interface exposing all write operations about data such as nodes, relationships and properties.
     * @throws InvalidTransactionTypeKernelException if type of this transaction have already been decided
     * and it's of a different type, e.g {@link #schemaWriteOperations()}.
     */
    DataWriteOperations dataWriteOperations() throws InvalidTransactionTypeKernelException;

    /**
     * @return interface exposing all write operations about schema such as indexes and constraints.
     * @throws InvalidTransactionTypeKernelException if type of this transaction have already been decided
     * and it's of a different type, e.g {@link #dataWriteOperations()}.
     */
    SchemaWriteOperations schemaWriteOperations() throws InvalidTransactionTypeKernelException;

    /**
     * @return interface exposing operations for associating metadata with this statement
     */
    QueryRegistryOperations queryRegistration();

    /**
     * @return interface exposing all procedure call operations.
     */
    ProcedureCallOperations procedureCallOperations();
}
