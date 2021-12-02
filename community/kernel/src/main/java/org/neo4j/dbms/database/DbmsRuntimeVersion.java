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
package org.neo4j.dbms.database;

import org.neo4j.kernel.KernelVersion;

public enum DbmsRuntimeVersion implements ComponentVersion
{
    /**
     * Introduced new transaction log version
     */
    V4_2( 2, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_42, KernelVersion.V4_2 ),

    /**
     * Switch to use the Version node
     */
    V4_3( 3, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_43D2, KernelVersion.V4_2 ),

    /**
     * Dense node locking changes, token indexes and relationship property indexes.
     */
    V4_3_D4( 4, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_43D4, KernelVersion.V4_3_D4 ),

    /**
     * Range, Point and Text index types.
     */
    V4_4( 5, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_44, KernelVersion.V4_4 ),

    /**
     * Introduced new transaction log version
     */
    V5_0( 6, DBMS_RUNTIME_COMPONENT, Neo4jVersions.VERSION_50, KernelVersion.V5_0 );

    public static final DbmsRuntimeVersion LATEST_DBMS_RUNTIME_COMPONENT_VERSION = V4_4;

    DbmsRuntimeVersion( int version, String componentName, String description, KernelVersion kernelVersion )
    {
        this.version = version;
        this.componentName = componentName;
        this.description = description;
        this.kernelVersion = kernelVersion;
    }

    private final String componentName;
    private final String description;
    private final KernelVersion kernelVersion;
    private final int version;

    @Override
    public int getVersion()
    {
        return version;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public String getComponentName()
    {
        return componentName;
    }

    @Override
    public boolean isCurrent()
    {
        return version == LATEST_DBMS_RUNTIME_COMPONENT_VERSION.version;
    }

    @Override
    public boolean migrationSupported()
    {
        return true;
    }

    @Override
    public boolean runtimeSupported()
    {
        return true;
    }

    public static DbmsRuntimeVersion fromVersionNumber( int versionNumber )
    {
        for ( DbmsRuntimeVersion componentVersion : DbmsRuntimeVersion.values() )
        {
            if ( componentVersion.version == versionNumber )
            {
                return componentVersion;
            }
        }
        throw new IllegalArgumentException( "Unrecognised DBMS runtime version number: " + versionNumber );
    }

    @Override
    public boolean isGreaterThan( ComponentVersion other )
    {
        if ( !(other instanceof DbmsRuntimeVersion) )
        {
            throw new IllegalArgumentException( "Comparison to different Version type" );
        }
        return this.getVersion() > other.getVersion();
    }

    public KernelVersion kernelVersion()
    {
        return kernelVersion;
    }
}
