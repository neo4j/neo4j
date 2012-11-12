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
package org.neo4j.management;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Arrays;

import javax.management.remote.JMXServiceURL;

import org.neo4j.helpers.Pair;

/**
 * This class captures the least amount of information available for a cluster member to any
 * cluster participant.
 */
public class ClusterMemberInfo implements Serializable
{
    private static final long serialVersionUID = 1L;
    private final String instanceId;
    private final boolean available;
    private final String haRole;
    private final String[] clusterRoles;
    private final String[] uris;

    @ConstructorProperties( { "instanceId", "available", "haRole", "clusterRoles", "uris" } )
    public ClusterMemberInfo( String instanceId, boolean available, String haRole, String[] clusterRoles, String[] uris )
    {
        this.instanceId = instanceId;
        this.available = available;
        this.haRole = haRole;
        this.clusterRoles = clusterRoles;
        this.uris = uris;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public boolean isAvailable()
    {
        return available;
    }
    
    public String getHaRole()
    {
        return haRole;
    }

    public String[] getClusterRoles()
    {
        return clusterRoles;
    }

    public String[] getUris()
    {
        return uris;
    }

    @Override
    @SuppressWarnings( "boxing" )
    public String toString()
    {
        return String.format( "Neo4jHaInstance[id=%s,available=%s,haRole=%s,clusterRoles=%s,URI List=%s]",
                instanceId, available, haRole, Arrays.toString( clusterRoles ), Arrays.toString( uris ) );
    }

    public Pair<Neo4jManager, HighAvailability> connect()
    {
        return connect( null, null );
    }

    public Pair<Neo4jManager, HighAvailability> connect( String username, String password )
    {
        URI address = null;
        for (String uri : uris)
        {
            if (uri.startsWith( "jmx" ))
            {
//                address = uri;
            }
        }
        if ( address == null )
        {
            throw new IllegalStateException( "The instance does not have a public JMX server." );
        }
        Neo4jManager manager = Neo4jManager.get( url(address), username, password, instanceId );
        return Pair.of( manager, manager.getHighAvailabilityBean() );
    }

    private JMXServiceURL url( URI address )
    {
        try
        {
            return new JMXServiceURL( address.toASCIIString() );
        }
        catch ( MalformedURLException e )
        {
            throw new IllegalStateException( "The instance does not have a valid JMX server URL." );
        }
    }
}
