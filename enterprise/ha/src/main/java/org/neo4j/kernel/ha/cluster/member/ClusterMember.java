/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel.ha.cluster.member;

import static org.neo4j.helpers.Functions.withDefaults;
import static org.neo4j.helpers.Settings.INTEGER;
import static org.neo4j.helpers.Uris.parameter;

import java.net.URI;
import java.util.Collections;
import java.util.Map;

import org.neo4j.helpers.Functions;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;

public class ClusterMember
{
    private final URI clusterUri;
    private final Map<String, URI> roles;
    private final boolean alive;

    public ClusterMember( URI clusterUri )
    {
        this( clusterUri, Collections.<String, URI>emptyMap(), true );
    }

    ClusterMember( URI clusterUri, Map<String, URI> roles, boolean alive )
    {
        this.clusterUri = clusterUri;
        this.roles = roles;
        this.alive = alive;
    }

    public URI getClusterUri()
    {
        return clusterUri;
    }

    public int getInstanceId()
    {
        URI haURI = getHAUri();

        if (haURI != null)
        {
            // Get serverId parameter, default to -1 if it is missing, and parse to integer
            return INTEGER.apply( withDefaults( Functions.<URI, String>constant( "-1" ), parameter( "serverId" ) ).apply( haURI ));
        } else
            return -1;
    }

    public URI getHAUri()
    {
        URI haURI = roles.get( HighAvailabilityModeSwitcher.MASTER );
        if (haURI == null)
            haURI = roles.get( HighAvailabilityModeSwitcher.SLAVE );

        return haURI;
    }

    public String getHARole()
    {
        if (roles.containsKey( HighAvailabilityModeSwitcher.MASTER ))
            return HighAvailabilityModeSwitcher.MASTER;
        if (roles.containsKey( HighAvailabilityModeSwitcher.SLAVE ))
            return HighAvailabilityModeSwitcher.SLAVE;
        return "UNKNOWN";
    }

    public boolean hasRole(String role)
    {
        return roles.containsKey( role );
    }
    public URI getRoleURI(String role)
    {
        return roles.get( role );
    }

    public Iterable<URI> getRoleURIs()
    {
        return roles.values();
    }

    public boolean isAlive()
    {
        return alive;
    }

    ClusterMember availableAs( String role, URI roleUri )
    {
        return new ClusterMember( this.clusterUri, MapUtil.copyAndPut( roles, role, roleUri ), this.alive );
    }

    ClusterMember unavailableAs( String role )
    {
        return new ClusterMember( this.clusterUri, MapUtil.copyAndRemove( roles, role ), this.alive );
    }

    ClusterMember alive(  )
    {
        return new ClusterMember( this.clusterUri, roles, true );
    }

    ClusterMember failed(  )
    {
        return new ClusterMember( this.clusterUri, roles, false );
    }

    @Override
    public String toString()
    {
        return String.format( "cluster URI=%s, alive=%s, roles=%s",clusterUri.toString(), alive, roles.toString());
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ClusterMember that = (ClusterMember) o;

        if ( !clusterUri.equals( that.clusterUri ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return clusterUri.hashCode();
    }
}
