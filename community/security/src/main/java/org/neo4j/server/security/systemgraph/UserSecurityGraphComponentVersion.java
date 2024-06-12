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
package org.neo4j.server.security.systemgraph;

import static org.neo4j.dbms.database.KnownSystemComponentVersion.UNKNOWN_VERSION;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.ComponentVersion;
import org.neo4j.dbms.database.SystemGraphComponent;

public enum UserSecurityGraphComponentVersion implements ComponentVersion {
    /**
     * Version scheme of SECURITY_USER_COMPONENT with breaking changes to the schema:
     * <p>
     * Version 1 (Neo4j 4.0): - A whole new schema was introduced (see {@link UserSecurityGraphComponent}), so all users must be migrated from the previous file
     * format.
     * <p>
     * Version 2 (Neo4j 4.1): - Introduced the version node in the system database
     * <p>
     * Version 3 (Neo4j 4.3-drop04): - Introduced user ids
     * <p>
     * Version 4 (Neo4j 5.0): - Add constraint for user ids
     * <p>
     * Version 5 (Neo4j 5.21): - Introduced auth object for users
     */
    COMMUNITY_SECURITY_43D4(3, SECURITY_USER_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_43D4),
    COMMUNITY_SECURITY_50(4, SECURITY_USER_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_50),
    COMMUNITY_SECURITY_521(5, SECURITY_USER_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_521),

    COMMUNITY_SECURITY_UNKNOWN_VERSION(
            UNKNOWN_VERSION, SECURITY_USER_COMPONENT, String.format("no '%s' graph found", SECURITY_USER_COMPONENT));

    // Static variables for SECURITY_USER_COMPONENT versions
    public static final int FIRST_VALID_COMMUNITY_SECURITY_COMPONENT_VERSION = COMMUNITY_SECURITY_43D4.getVersion();
    public static final int FIRST_RUNTIME_SUPPORTED_COMMUNITY_SECURITY_COMPONENT_VERSION =
            COMMUNITY_SECURITY_43D4.getVersion();
    public static final int LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION = COMMUNITY_SECURITY_521.getVersion();

    private final SystemGraphComponent.Name componentName;
    private final int version;
    private final String description;

    UserSecurityGraphComponentVersion(int version, SystemGraphComponent.Name componentName, String description) {
        this.version = version;
        this.componentName = componentName;
        this.description = description;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public SystemGraphComponent.Name getComponentName() {
        return componentName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isCurrent(Config config) {
        return version == LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;
    }

    @Override
    public boolean migrationSupported() {
        return version >= FIRST_VALID_COMMUNITY_SECURITY_COMPONENT_VERSION
                && version <= LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;
    }

    @Override
    public boolean runtimeSupported() {
        return version >= FIRST_RUNTIME_SUPPORTED_COMMUNITY_SECURITY_COMPONENT_VERSION
                && version <= LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;
    }
}
