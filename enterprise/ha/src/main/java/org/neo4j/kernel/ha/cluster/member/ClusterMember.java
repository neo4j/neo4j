/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha.cluster.member;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.backup.OnlineBackupKernelExtension;
import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.store.StoreId;

public class ClusterMember
{
    private final InstanceId instanceId;
    private final Map<String, URI> roles;
    private final StoreId storeId;
    private final boolean alive;

    public ClusterMember( InstanceId instanceId )
    {
        this( instanceId, Collections.emptyMap(), StoreId.DEFAULT, true );
    }

    public ClusterMember( InstanceId instanceId, Map<String,URI> roles, StoreId storeId, boolean alive )
    {
        this.instanceId = instanceId;
        this.roles = roles;
        this.storeId = storeId;
        this.alive = alive;
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
        return new ClusterMember( this.instanceId, copy, storeId, this.alive );
    }

    ClusterMember unavailable()
    {
        return new ClusterMember( this.instanceId, Collections.emptyMap(), storeId, this.alive );
    }

    ClusterMember unavailableAs( String role )
    {
        return new ClusterMember( this.instanceId, MapUtil.copyAndRemove( roles, role ), this.storeId, this.alive );
    }

    ClusterMember alive()
    {
        return new ClusterMember( this.instanceId, roles, storeId, true );
    }

    ClusterMember failed()
    {
        return new ClusterMember( this.instanceId, roles, storeId, false );
    }

    @Override
    public String toString()
    {
        return "ClusterMember{" +
               "instanceId=" + instanceId +
               ", roles=" + roles +
               ", storeId=" + storeId +
               ", alive=" + alive +
               '}';
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
