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
    private final boolean alive;
    private final String haRole;
    private final String[] uris;
    private final String[] roles;

    @ConstructorProperties( { "instanceId", "available", "alive", "haRole", "uris", "roles" } )
    public ClusterMemberInfo( String instanceId, boolean available, boolean alive, String haRole, String[] uris,
                              String[] roles )
    {
        this.instanceId = instanceId;
        this.available = available;
        this.alive = alive;
        this.haRole = haRole;
        this.uris = uris;
        this.roles = roles;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public boolean isAvailable()
    {
        return available;
    }
    
    public boolean isAlive()
    {
        return alive;
    }
    
    public String getHaRole()
    {
        return haRole;
    }

    public String[] getUris()
    {
        return uris;
    }

    public String[] getRoles()
    {
        return roles;
    }

    @Override
    @SuppressWarnings( "boxing" )
    public String toString()
    {
        return String.format( "Neo4jHaInstance[id=%s,available=%s,haRole=%s,HA URI=%s]", instanceId, available, haRole,
                Arrays.toString(uris) );
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
