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
package org.neo4j.kernel.api;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.values.virtual.MapValue;


/**
 * Tracks currently running stream. This is used for listing currently running stream and to make it possible to
 * terminate a query, not matter which or how many transactions it's working in.
 *
 * If a query uses multiple transactions (think of PERIODIC COMMIT), the query needs to be registered to all
 * transactions it uses.
 */
public interface QueryRegistryOperations
{
    /**
     * Sets the user defined meta data to be associated with started queries.
     * @param data the meta data
     */
    void setMetaData( Map<String,Object> data );

    /**
     * Gets associated meta data.
     *
     * @return the meta data
     */
    Map<String,Object> getMetaData();

    /**
     * List of all currently running stream in this transaction. An user can have multiple stream running
     * simultaneously on the same transaction.
     */
    Stream<ExecutingQuery> executingQueries();

    /**
     * Registers a query, and creates the ExecutingQuery object for it.
     */
    ExecutingQuery startQueryExecution(
        ClientConnectionInfo descriptor, String queryText, MapValue queryParameters
    );

    /**
     * Registers an already known query to a this transaction.
     *
     * This is used solely for supporting PERIODIC COMMIT which requires committing and starting new transactions
     * and associating the same ExecutingQuery with those new transactions.
     */
    void registerExecutingQuery( ExecutingQuery executingQuery );

    /**
     * Disassociates a query with this transaction.
     */
    void unregisterExecutingQuery( ExecutingQuery executingQuery );
}
