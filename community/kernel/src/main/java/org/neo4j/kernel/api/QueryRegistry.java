/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api;

import java.util.Optional;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.values.virtual.MapValue;

/**
 * Tracks currently running stream. This is used for listing currently running stream and to make it possible to
 * terminate a query, not matter which or how many transactions it's working in.
 *
 * If a query uses multiple transactions (think of PERIODIC COMMIT), the query needs to be registered to all
 * transactions it uses.
 */
public interface QueryRegistry {
    /**
     * List of all currently running stream in this transaction. An user can have multiple stream running
     * simultaneously on the same transaction.
     */
    Optional<ExecutingQuery> executingQuery();

    /**
     * Creates a new {@link ExecutingQuery} to be executed and registers it.
     *
     * @return the new ExecutingQuery
     */
    ExecutingQuery startAndBindExecutingQuery(String queryText, MapValue queryParameters);

    /**
     * Registers an already known query to be executed
     */
    void bindExecutingQuery(ExecutingQuery executingQuery);

    /**
     * Unregisters a query that was stopped
     */
    void unbindExecutingQuery(ExecutingQuery executingQuery, long userTransactionId);

    /**
     * Registers an already known query with a statement.
     *
     * This is used solely for snapshot retry. (see SnapshotExecutionEngine)
     */
    void registerExecutingQuery(ExecutingQuery executingQuery);
}
