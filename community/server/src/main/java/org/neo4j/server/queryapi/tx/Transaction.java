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
package org.neo4j.server.queryapi.tx;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.ResultSummary;

public interface Transaction {

    void runQuery(String statement, Map<String, Object> parameters);

    Result retrieveResults();

    Set<Bookmark> commit();

    void rollback();

    boolean isOpen();

    /**
     * Release transaction, session and all other state.
     */
    void close();

    /**
     * Lock this transaction
     */
    boolean tryAcquire();

    boolean tryAcquire(long timeout, TimeUnit timeUnit);

    void release();

    Instant expiresAt();

    String id();

    void extendTimeout();

    ResultSummary resultSummary();

    String databaseName();

    AuthToken authToken();
}
