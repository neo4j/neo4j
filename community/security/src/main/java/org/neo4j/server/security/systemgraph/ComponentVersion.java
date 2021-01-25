/*
 * Copyright (c) "Neo4j"
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

import static org.neo4j.dbms.database.AbstractSystemGraphComponent.SECURITY_PRIVILEGE_COMPONENT;
import static org.neo4j.dbms.database.AbstractSystemGraphComponent.SECURITY_USER_COMPONENT;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.UNKNOWN_VERSION;

/**
 * Describes the version scheme of those system components that needs versioning.
 * Also keeps track of the current versions and for which versions runtime and migration are supported.
 */
public enum ComponentVersion
{
    /**
     * Version scheme of SECURITY_USER_COMPONENT with breaking changes to the schema:
     *
     * Version 0 (Neo4j 3.5):
     *  - Users were stored in a file.
     *
     * Version 1 (Neo4j 4.0):
     *  - A whole new schema was introduced (see {@link UserSecurityGraphComponent}),
     *    so all users must be migrated from the previous file format.
     *
     * Version 2 (Neo4j 4.1):
     *  - Introduced the version node in the system database
     */
    COMMUNITY_SECURITY_35( 0, SECURITY_USER_COMPONENT, Neo4jVersions.VERSION_35 ),
    COMMUNITY_SECURITY_40( 1, SECURITY_USER_COMPONENT, Neo4jVersions.VERSION_40 ),
    COMMUNITY_SECURITY_41( 2, SECURITY_USER_COMPONENT, Neo4jVersions.VERSION_41 ),

    COMMUNITY_SECURITY_UNKNOWN_VERSION( UNKNOWN_VERSION, SECURITY_USER_COMPONENT, String.format( "no '%s' graph found", SECURITY_USER_COMPONENT ) ),

    /**
     * Version scheme of SECURITY_PRIVILEGE_COMPONENT with breaking changes to the schema:
     *
     * Version 0 (Neo4j 3.5):
     *  - Users were stored in a file and the roles for each users in another enterprise-only file.
     *
     * Version 1 (Neo4j 3.6):
     *  - Introduction of the system database as a way to store users and roles.
     *    Users and roles can be migrated from the old file-based format.
     *    Note that the system database was enterprise-only and had a very different schema compare with today.
     *
     * Version 2 (Neo4j 4.0):
     *  - A whole new schema was introduced, so all roles and users must be re-added and the default privileges created.
     *    Each role is represented by a node with label :Role that is connected to zero or more users from the {@link UserSecurityGraphComponent}.
     *    A privilege is represented of a relationship of type :GRANTED or :DENIED from a role node to a node with label (:Privilege),
     *    which in turn is connected as below (where the database node is part of the DefaultSystemGraphComponent).
     *
     *   (:Privilege)-[:SCOPE]->(s:Segment)-[:APPLIES_TO]->(:Resource), (s)-[:FOR]->(database), (s)-[:Qualified]->(qualifier)
     *
     * Version 3 (Neo4j 4.1.0-Drop01):
     *  - The global write privilege became connected to a GraphResource instead of an AllPropertiesResource
     *  - The schema privilege was split into separate index and constraint privileges
     *  - Introduced the PUBLIC role
     *
     * Version 4 (Neo4j 4.1):
     *  - Introduced the version node in the system database
     *
     * Version 5 (Neo4j 4.2.0-Drop04):
     *   - Added support for execute procedure privileges
     *
     * Version 6 (Neo4j 4.2.0-Drop06):
     *   - Added support for execute function privileges
     *
     * Version 7 (Neo4j 4.2.0-Drop07):
     *   - Added support for show index and show constraint privileges
     *
     * Version 8 (Neo4j 4.2.1):
     *   - Fix bug with missing PUBLIC role
     */
    ENTERPRISE_SECURITY_35( 0, SECURITY_PRIVILEGE_COMPONENT, Neo4jVersions.VERSION_35 ),
    ENTERPRISE_SECURITY_36( 1, SECURITY_PRIVILEGE_COMPONENT, Neo4jVersions.VERSION_36 ),
    ENTERPRISE_SECURITY_40( 2, SECURITY_PRIVILEGE_COMPONENT, Neo4jVersions.VERSION_40 ),
    ENTERPRISE_SECURITY_41D1( 3, SECURITY_PRIVILEGE_COMPONENT, Neo4jVersions.VERSION_41D1 ),
    ENTERPRISE_SECURITY_41( 4, SECURITY_PRIVILEGE_COMPONENT, Neo4jVersions.VERSION_41 ),
    ENTERPRISE_SECURITY_42D4( 5, SECURITY_PRIVILEGE_COMPONENT, Neo4jVersions.VERSION_42D4 ),
    ENTERPRISE_SECURITY_42D6( 6, SECURITY_PRIVILEGE_COMPONENT, Neo4jVersions.VERSION_42D6 ),
    ENTERPRISE_SECURITY_42D7( 7, SECURITY_PRIVILEGE_COMPONENT, Neo4jVersions.VERSION_42D7 ),
    ENTERPRISE_SECURITY_42P1( 8, SECURITY_PRIVILEGE_COMPONENT, Neo4jVersions.VERSION_42P1 ),

    ENTERPRISE_SECURITY_UNKNOWN_VERSION( UNKNOWN_VERSION, SECURITY_PRIVILEGE_COMPONENT, String.format( "no '%s' graph found", SECURITY_PRIVILEGE_COMPONENT ) ),
    ENTERPRISE_SECURITY_FUTURE_VERSION( Integer.MIN_VALUE, SECURITY_PRIVILEGE_COMPONENT, "Unrecognized future version" ),

    // Used for testing only:
    ENTERPRISE_SECURITY_FAKE_VERSION( Integer.MAX_VALUE, SECURITY_PRIVILEGE_COMPONENT, "Neo4j 8.8.88" );

    // Static variables for SECURITY_USER_COMPONENT versions
    public static final int FIRST_VALID_COMMUNITY_SECURITY_COMPONENT_VERSION = COMMUNITY_SECURITY_35.getVersion();
    public static final int FIRST_RUNTIME_SUPPORTED_COMMUNITY_SECURITY_COMPONENT_VERSION = COMMUNITY_SECURITY_40.getVersion();
    public static final int LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION = COMMUNITY_SECURITY_41.getVersion();

    // Static variables for SECURITY_PRIVILEGE_COMPONENT versions
    public static final int FIRST_VALID_ENTERPRISE_SECURITY_COMPONENT_VERSION = ENTERPRISE_SECURITY_35.getVersion();
    public static final int FIRST_RUNTIME_SUPPORTED_ENTERPRISE_SECURITY_COMPONENT_VERSION = ENTERPRISE_SECURITY_40.getVersion();
    public static final int LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION = ENTERPRISE_SECURITY_42P1.getVersion();

    private final String componentName;
    private final int version;
    private final String description;

    ComponentVersion( int version, String componentName, String description )
    {

        this.version = version;
        this.componentName = componentName;
        this.description = description;
    }

    public int getVersion()
    {
        return version;
    }

    public String getComponentName()
    {
        return componentName;
    }

    public String getDescription()
    {
        return description;
    }

    boolean isCurrent()
    {
        if ( componentName.equals( SECURITY_USER_COMPONENT ) )
        {
            return version == LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;
        }
        else if ( componentName.equals( SECURITY_PRIVILEGE_COMPONENT ) )
        {
            return version == LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;
        }
        return false;
    }

    public boolean migrationSupported()
    {
        if ( componentName.equals( SECURITY_USER_COMPONENT ) )
        {
            return version >= FIRST_VALID_COMMUNITY_SECURITY_COMPONENT_VERSION && version <= LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;
        }
        else if ( componentName.equals( SECURITY_PRIVILEGE_COMPONENT ) )
        {
            return version >= FIRST_VALID_ENTERPRISE_SECURITY_COMPONENT_VERSION && version <= LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;
        }
        return false;
    }

    public boolean runtimeSupported()
    {
        if ( componentName.equals( SECURITY_USER_COMPONENT ) )
        {
            return version >= FIRST_RUNTIME_SUPPORTED_COMMUNITY_SECURITY_COMPONENT_VERSION && version <= LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;
        }
        else if ( componentName.equals( SECURITY_PRIVILEGE_COMPONENT ) )
        {
            return version >= FIRST_RUNTIME_SUPPORTED_ENTERPRISE_SECURITY_COMPONENT_VERSION && version <= LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;
        }
        return false;
    }

    public static class Neo4jVersions
    {
        public static final String VERSION_35 = "Neo4j 3.5";
        public static final String VERSION_36 = "Neo4j 3.6";
        public static final String VERSION_40 = "Neo4j 4.0";
        public static final String VERSION_41D1 = "Neo4j 4.1.0-Drop01";
        public static final String VERSION_41 = "Neo4j 4.1";
        public static final String VERSION_42D4 = "Neo4j 4.2.0-Drop04";
        public static final String VERSION_42D6 = "Neo4j 4.2.0-Drop06";
        public static final String VERSION_42D7 = "Neo4j 4.2.0-Drop07";
        public static final String VERSION_42P1 = "Neo4j 4.2.1";

        public static final int UNKNOWN_VERSION = -1;
    }
}
