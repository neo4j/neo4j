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
package org.neo4j.server.configuration;

/**
 * Enumeration of the configurable server modules. There is no 1:1 correlation between a {@literal ConfigurableServerModules} and an implementation of {@link
 * org.neo4j.server.modules.ServerModule ServerModule}. A configurable module may consist of several implementing modules.
 */
public enum ConfigurableServerModules {
    /**
     * Provides the transactional endpoints (Cypher and legacy endpoint).
     */
    TRANSACTIONAL_ENDPOINTS,
    /**
     * Loads unmanaged extensions.
     */
    UNMANAGED_EXTENSIONS,
    /**
     * Provides the Neo4j browser.
     */
    BROWSER,
    /**
     * Provides cluster management endpoints if applicable.
     */
    ENTERPRISE_MANAGEMENT_ENDPOINTS,

    /**
     * Provides the Query API endpoints.
     */
    QUERY_API_ENDPOINTS
}
