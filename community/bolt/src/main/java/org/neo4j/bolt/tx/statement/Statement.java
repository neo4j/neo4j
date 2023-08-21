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
package org.neo4j.bolt.tx.statement;

import java.util.List;
import java.util.Optional;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.tx.error.statement.StatementException;
import org.neo4j.graphdb.QueryStatistics;

public interface Statement {

    /**
     * Retrieves the transaction specific identifier via which this statement is referenced.
     *
     * @return a transaction specific identifier.
     */
    long id();

    /**
     * Retrieves a listing of field names returned by this statement.
     *
     * @return a list of field names.
     */
    List<String> fieldNames();

    /**
     * Retrieves the total amount of time spent streaming within this statement.
     *
     * @return time spent (in milliseconds).
     */
    long executionTime();

    /**
     * Retrieves the query statistics for this statement.
     * <p />
     * Statistics are populated once a statement has finished execution (e.g. has no more remaining
     * results).
     *
     * @return a set of query statistics or an empty optional if the statement has yet to complete.
     */
    Optional<QueryStatistics> statistics();

    /**
     * Evaluates whether this statement has remaining results.
     *
     * @return true if results remain, false otherwise.
     */
    boolean hasRemaining();

    /**
     * Consumes {@code n} results from this statement.
     *
     * @param responseHandler a response handler which shall receive generated records and metadata.
     * @param n a number of results or zero if all available results shall be returned at once.
     */
    void consume(ResponseHandler responseHandler, long n) throws StatementException;

    /**
     * Discards {@code n} results from this statement.
     *
     * @param responseHandler a response handler which shall received generated metadata.
     * @param n a number of results or zero if all available results shall be discarded at once.
     */
    void discard(ResponseHandler responseHandler, long n) throws StatementException;

    /**
     * Terminates this statement.
     */
    void terminate();

    /**
     * Subscribes a given listener to lifecycle events within this statement.
     *
     * @param listener a listener.
     */
    void registerListener(Listener listener);

    /**
     * Un-subscribes a given listener from lifecycle events within this statement.
     *
     * @param listener a listener.
     */
    void removeListener(Listener listener);

    void close();

    interface Listener {

        default void onCompleted(Statement statement) {}

        default void onTerminated(Statement statement) {}

        default void onClosed(Statement statement) {}
    }
}
