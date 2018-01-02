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

import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;

/**
 * A transaction with the graph database.
 */
public interface Transaction extends AutoCloseable
{
    enum Type
    {
        implicit,
        explicit
    }

    /**
     * The store id of a rolled back transaction.
     */
    long ROLLBACK = -1;

    /**
     * The store id of a read-only transaction.
     */
    long READ_ONLY = 0;

    /**
     * Marks this transaction as successful. When this transaction later gets {@link #close() closed}
     * its changes, if any, will be committed. If this method hasn't been called or if {@link #failure()}
     * has been called then any changes in this transaction will be rolled back as part of {@link #close() closing}.
     */
    void success();

    /**
     * Marks this transaction as failed. No amount of calls to {@link #success()} will clear this flag.
     * When {@link #close() closing} this transaction any changes will be rolled back.
     */
    void failure();

    /**
     * @return The Read operations of the graph.
     */
    Read dataRead();

    /**
     * @return The Write operations of the graph.
     * @throws InvalidTransactionTypeKernelException when transaction cannot be upgraded to a write transaction. This
     * can happen when there have been schema modifications.
     */
    Write dataWrite() throws InvalidTransactionTypeKernelException;

    /**
     * @return The explicit index read operations of the graph.
     */
    ExplicitIndexRead indexRead();

    /**
     * @return The explicit index write operations of the graph.
     */
    ExplicitIndexWrite indexWrite();

    /**
     * @return Token read operations
     */
    TokenRead tokenRead();

    /**
     * @return Token read operations
     */
    TokenWrite tokenWrite();

    /**
     * @return The schema index read operations of the graph, used for finding indexes.
     */
    SchemaRead schemaRead();

    /**
     * @return The schema index write operations of the graph, used for creating and dropping indexes and constraints.
     */
    SchemaWrite schemaWrite();

    /**
     * @return The lock operations of the graph.
     */
    Locks locks();

    /**
     * @return The cursor factory
     */
    CursorFactory cursors();
}
