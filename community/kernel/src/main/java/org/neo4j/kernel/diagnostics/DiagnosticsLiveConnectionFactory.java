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

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;

/**
 * Establishes an authenticated {@link DiagnosticsLiveConnection} to a running DBMS.
 * <p>
 * Service loaded so that the implementation (which depends on a connection technology such as the Neo4j driver) can
 * live outside of the core modules that only need the abstraction. The diagnostics command loads it via
 * {@link org.neo4j.service.Services}; tests inject their own.
 */
@Service
public interface DiagnosticsLiveConnectionFactory {
    /**
     * Connects to a running DBMS.
     *
     * @param config the instance configuration, used to derive the Bolt URL when {@code boltUrl} is {@code null}.
     * @param boltUrl an explicit Bolt URL including the scheme (e.g. {@code bolt://localhost:7687}), or {@code null}
     * to derive it from {@code config}. The server certificate is not validated.
     * @param username the username to authenticate with.
     * @param password the password to authenticate with.
     * @return an open, verified connection.
     */
    DiagnosticsLiveConnection connect(Config config, String boltUrl, String username, String password);
}
