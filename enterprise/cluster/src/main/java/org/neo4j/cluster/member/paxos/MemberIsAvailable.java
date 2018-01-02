/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster.member.paxos;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.kernel.impl.store.StoreId;

/**
 * This message is broadcast when a member of the cluster declares that
 * it is ready to serve a particular role for the cluster.
 */
public class MemberIsAvailable
        implements Externalizable
{
    private String role;
    private InstanceId instanceId;
    private URI clusterUri;
    private URI roleUri;
    private StoreId storeId;

    public MemberIsAvailable()
    {
    }

    public MemberIsAvailable( String role, InstanceId instanceId, URI clusterUri, URI roleUri, StoreId storeId )
    {
        this.role = role;
        this.instanceId = instanceId;
        this.clusterUri = clusterUri;
        this.roleUri = roleUri;
        this.storeId = storeId;
    }

    public String getRole()
    {
        return role;
    }

    public InstanceId getInstanceId()
    {
        return instanceId;
    }

    public URI getClusterUri()
    {
        return clusterUri;
    }

    public URI getRoleUri()
    {
        return roleUri;
    }

    public StoreId getStoreId()
    {
        return storeId;
    }

    @Override
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeUTF( role );
        out.writeObject( instanceId );
        out.writeUTF( clusterUri.toString() );
        out.writeUTF( roleUri.toString() );
        storeId.writeExternal( out );
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        role = in.readUTF();
        instanceId = (InstanceId) in.readObject();
        clusterUri = URI.create( in.readUTF() );
        roleUri = URI.create( in.readUTF() );
        // if MemberIsAvailable message comes from old instance than we can't read storeId
        try
        {
            storeId = StoreId.from( in );
        }
        catch ( IOException e )
        {
            storeId = StoreId.DEFAULT;
        }
    }

    @Override
    public String toString()
    {
        return String.format( "MemberIsAvailable[ Role: %s, InstanceId: %s, Role URI: %s, Cluster URI: %s]",
                role, instanceId.toString(), roleUri.toString(), clusterUri.toString() );
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

        MemberIsAvailable that = (MemberIsAvailable) o;

        if ( !clusterUri.equals( that.clusterUri ) )
        {
            return false;
        }
        if ( !instanceId.equals( that.instanceId ) )
        {
            return false;
        }
        if ( !role.equals( that.role ) )
        {
            return false;
        }
        if ( !roleUri.equals( that.roleUri ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = role.hashCode();
        result = 31 * result + instanceId.hashCode();
        result = 31 * result + clusterUri.hashCode();
        result = 31 * result + roleUri.hashCode();
        return result;
    }
}
