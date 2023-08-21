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
package org.neo4j.dbms.systemgraph;

import static org.neo4j.dbms.database.KnownSystemComponentVersion.UNKNOWN_VERSION;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.ComponentVersion;
import org.neo4j.dbms.database.SystemGraphComponent;

public enum CommunityTopologyGraphVersion implements ComponentVersion {
    /**
     * Version scheme of COMMUNITY_TOPOLOGY_GRAPH_COMPONENT with breaking changes to the schema:
     *
     * Version 0 (Neo4j 4.4):
     *  - Introduced the access property on database nodes
     * Version 1 (Neo4j 5.0):
     *  - bug fix: Adding name nodes to fabric databases
     */
    COMMUNITY_TOPOLOGY_44(0, Neo4jVersions.VERSION_44),
    COMMUNITY_TOPOLOGY_50(1, Neo4jVersions.VERSION_50),
    COMMUNITY_TOPOLOGY_58(1, Neo4jVersions.VERSION_58),

    COMMUNITY_TOPOLOGY_UNKNOWN_VERSION(
            UNKNOWN_VERSION, String.format("no '%s' found", COMMUNITY_TOPOLOGY_GRAPH_COMPONENT));

    public static final int FIRST_VALID_COMMUNITY_TOPOLOGY_VERSION = COMMUNITY_TOPOLOGY_44.getVersion();
    public static final int LATEST_COMMUNITY_TOPOLOGY_VERSION = COMMUNITY_TOPOLOGY_58.getVersion();

    CommunityTopologyGraphVersion(int version, String description) {
        this.version = version;
        this.description = description;
    }

    private final String description;
    private final int version;

    @Override
    public String toString() {
        return description + '(' + version + ')';
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public SystemGraphComponent.Name getComponentName() {
        return COMMUNITY_TOPOLOGY_GRAPH_COMPONENT;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isCurrent(Config config) {
        return version == LATEST_COMMUNITY_TOPOLOGY_VERSION;
    }

    @Override
    public boolean migrationSupported() {
        return version >= FIRST_VALID_COMMUNITY_TOPOLOGY_VERSION && version <= LATEST_COMMUNITY_TOPOLOGY_VERSION;
    }

    @Override
    public boolean runtimeSupported() {
        return version >= FIRST_VALID_COMMUNITY_TOPOLOGY_VERSION && version <= LATEST_COMMUNITY_TOPOLOGY_VERSION;
    }
}
