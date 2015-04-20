/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.store.StoreId;

public class ClusterMember
{
    private final InstanceId instanceId;
    private final Map<String, URI> roles;
    private final StoreId storeId;
    private final boolean alive;
    private final boolean initiallyKnown;

    public ClusterMember( InstanceId instanceId )
    {
        this( instanceId, false );
    }

    ClusterMember( InstanceId instanceId, boolean initiallyKnown )
    {
        this( instanceId, Collections.<String, URI>emptyMap(), StoreId.DEFAULT, true, initiallyKnown );
    }

    ClusterMember( InstanceId instanceId, Map<String, URI> roles, StoreId storeId,
                   boolean alive, boolean initiallyKnown )
    {
        this.instanceId = instanceId;
        this.roles = roles;
        this.storeId = storeId;
        this.alive = alive;
        this.initiallyKnown = initiallyKnown;
    }

    public InstanceId getInstanceId()
    {
        return instanceId;
    }

    public URI getHAUri()
    {
        URI haURI = roles.get( HighAvailabilityModeSwitcher.MASTER );
        if ( haURI == null )
        {
            haURI = roles.get( HighAvailabilityModeSwitcher.SLAVE );
        }
        return haURI;
    }

    public String getHARole()
    {
        if ( roles.containsKey( HighAvailabilityModeSwitcher.MASTER ) )
        {
            return HighAvailabilityModeSwitcher.MASTER;
        }
        if ( roles.containsKey( HighAvailabilityModeSwitcher.SLAVE ) )
        {
            return HighAvailabilityModeSwitcher.SLAVE;
        }
        return HighAvailabilityModeSwitcher.UNKNOWN;
    }

    public boolean hasRole( String role )
    {
        return roles.containsKey( role );
    }

    public Iterable<String> getRoles()
    {
        return roles.keySet();
    }

    public Iterable<URI> getRoleURIs()
    {
        return roles.values();
    }

    public StoreId getStoreId()
    {
        return storeId;
    }

    public boolean isAlive()
    {
        return alive;
    }

    public boolean isInitiallyKnown()
    {
        return initiallyKnown;
    }

    ClusterMember availableAs( String role, URI roleUri, StoreId storeId )
    {
        Map<String, URI> copy = new HashMap<>( roles );
        if ( role.equals( HighAvailabilityModeSwitcher.MASTER ) )
        {
            copy.remove( HighAvailabilityModeSwitcher.SLAVE );
        }
        else if ( role.equals( HighAvailabilityModeSwitcher.SLAVE ) )
        {
            copy.remove( HighAvailabilityModeSwitcher.MASTER );
            copy.remove( OnlineBackupKernelExtension.BACKUP );
        }
        copy.put( role, roleUri );
        return new ClusterMember( this.instanceId, copy, storeId, this.alive, this.initiallyKnown );
    }

    ClusterMember unavailableAs( String role )
    {
        return new ClusterMember( this.instanceId, MapUtil.copyAndRemove( roles, role ),
                this.storeId, this.alive, this.initiallyKnown );
    }

    ClusterMember alive()
    {
        return new ClusterMember( this.instanceId, roles, storeId, true, this.initiallyKnown );
    }

    ClusterMember failed()
    {
        return new ClusterMember( this.instanceId, roles, storeId, false, this.initiallyKnown );
    }

    @Override
    public String toString()
    {
        return String.format( "cluster URI=%s, alive=%s, initial=%s, roles=%s, store=%s",
                instanceId, alive, initiallyKnown, roles, storeId );
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
        return instanceId.equals( that.instanceId );
    }

    @Override
    public int hashCode()
    {
        return instanceId.hashCode();
    }
}
