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
package org.neo4j.kernel.diagnostics;

/**
 * An authenticated connection to a running DBMS, used by {@link DiagnosticsAuthenticatedReportProvider}s to run
 * procedures/queries while gathering a diagnostics report.
 * <p>
 * A single connection is shared by all authenticated providers in a report run and is closed once they have all
 * contributed their sources. The abstraction is intentionally narrow and free of any connection-technology
 * types so that providers (in any module) only depend on {@code neo4j-kernel}.
 */
public interface DiagnosticsLiveConnection extends AutoCloseable {
    /**
     * Runs a read-only query/procedure and returns the fully materialized result.
     *
     * @param database the database to run against, or {@code null} for the connection's default/home database.
     *                 Administrative procedures (e.g. {@code dbms.*}) are typically run against the {@code system} database.
     * @param query the Cypher query or procedure call to execute.
     * @return the materialized result.
     */
    DiagnosticsQueryResult execute(String database, String query);

    @Override
    void close();
}
