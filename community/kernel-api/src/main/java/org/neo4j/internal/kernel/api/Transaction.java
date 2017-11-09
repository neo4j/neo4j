/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

/**
 * A transaction with the graph database.
 */
public interface Transaction
{
    /**
     * The store id of a read-only transaction.
     */
    long READ_ONLY_TRANSACTION = 0;

    /**
     * Commit this transaction.
     * @return the id of the commited transaction, or {@link Transaction#READ_ONLY_TRANSACTION} if this was a read only
     * transaction.
     */
    long commit();

    /**
     * Rollback this transaction.
     */
    void rollback();

    /**
     * @return The Read operations of the graph.
     */
    Read dataRead();

    /**
     * @return The Write operations of the graph.
     */
    Write dataWrite();

    /**
     * @return The explicit index read operations of the graph.
     */
    ExplicitIndexRead indexRead();

    /**
     * @return The explicit index write operations of the graph.
     */
    ExplicitIndexWrite indexWrite();

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
}
