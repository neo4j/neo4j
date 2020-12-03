/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.security.systemgraph;

import org.neo4j.dbms.database.ComponentVersion;

import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.UNKNOWN_VERSION;

public enum UserSecurityGraphComponentVersion implements ComponentVersion
{
    /**
     * Version scheme of SECURITY_USER_COMPONENT with breaking changes to the schema:
     * <p>
     * Version 0 (Neo4j 3.5): - Users were stored in a file.
     * <p>
     * Version 1 (Neo4j 4.0): - A whole new schema was introduced (see {@link UserSecurityGraphComponent}), so all users must be migrated from the previous file
     * format.
     * <p>
     * Version 2 (Neo4j 4.1): - Introduced the version node in the system database
     */
    COMMUNITY_SECURITY_35( 0, SECURITY_USER_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_35 ),
    COMMUNITY_SECURITY_40( 1, SECURITY_USER_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_40 ),
    COMMUNITY_SECURITY_41( 2, SECURITY_USER_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_41 ),

    COMMUNITY_SECURITY_UNKNOWN_VERSION( UNKNOWN_VERSION, SECURITY_USER_COMPONENT, String.format( "no '%s' graph found", SECURITY_USER_COMPONENT ) );

    // Static variables for SECURITY_USER_COMPONENT versions
    public static final int FIRST_VALID_COMMUNITY_SECURITY_COMPONENT_VERSION = COMMUNITY_SECURITY_35.getVersion();
    public static final int FIRST_RUNTIME_SUPPORTED_COMMUNITY_SECURITY_COMPONENT_VERSION = COMMUNITY_SECURITY_40.getVersion();
    public static final int LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION = COMMUNITY_SECURITY_41.getVersion();

    private final String componentName;
    private final int version;
    private final String description;

    UserSecurityGraphComponentVersion( int version, String componentName, String description )
    {
        this.version = version;
        this.componentName = componentName;
        this.description = description;
    }

    @Override
    public int getVersion()
    {
        return version;
    }

    @Override
    public String getComponentName()
    {
        return componentName;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public boolean isCurrent()
    {
        return version == LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;
    }

    @Override
    public boolean migrationSupported()
    {
        return version >= FIRST_VALID_COMMUNITY_SECURITY_COMPONENT_VERSION && version <= LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;
    }

    @Override
    public boolean runtimeSupported()
    {
        return version >= FIRST_RUNTIME_SUPPORTED_COMMUNITY_SECURITY_COMPONENT_VERSION && version <= LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;
    }
}
