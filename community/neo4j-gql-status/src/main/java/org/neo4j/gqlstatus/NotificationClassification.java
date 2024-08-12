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
package org.neo4j.gqlstatus;

import org.neo4j.annotations.api.PublicApi;

/**
 * Classification indicates to a client the classification of a notification.
 */
@PublicApi
public enum NotificationClassification implements GqlClassification {
    /**
     * Deprecated feature/format/functionality
     */
    DEPRECATION,

    /**
     * Unfulfillable hint warnings
     * */
    HINT,

    /**
     * Informational notes which suggests improvements to increase performance by making changes to query/schema
     * */
    PERFORMANCE,

    /**
     * Warnings/info that are not part of a wider class
     * */
    GENERIC,

    /**
     * The query or command mentions entities that are unknown to the system
     * */
    UNRECOGNIZED,

    /**
     * Classification is unknown
     */
    UNKNOWN,

    /**
     * Unsupported feature warnings
     */
    UNSUPPORTED,

    /**
     * Security warnings
     */
    SECURITY,

    /**
     * Topology warnings and information
     */
    TOPOLOGY,

    /**
     * Schema warnings and information
     */
    SCHEMA;
}
