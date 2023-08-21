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
package org.neo4j.memory;

public enum MemoryGroup {
    TRANSACTION("Transaction"),
    BOLT("Bolt"),
    HTTP("HTTP"),
    HTTP_TRANSACTION("HTTP Transaction"),
    PAGE_CACHE("Page Cache"),
    OTHER("Other"),
    RECENT_QUERY_BUFFER("Recent Query Buffer"),
    CENTRAL_BYTE_BUFFER_MANAGER("Central Byte Buffer Manager"),
    NO_TRACKING("No Tracking"),
    CLUSTER("Cluster"),
    BACKUP("Backup");

    private final String name;

    MemoryGroup(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
