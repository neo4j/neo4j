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

import javax.management.remote.JMXServiceURL;

import org.neo4j.helpers.Pair;

public class InstanceInfo implements Serializable
{
    private static final long serialVersionUID = 1L;
    private final String address;
    private final String instanceId;
    private final int machineId;
    private final boolean master;
    private final long lastTxId;

    @ConstructorProperties( { "address", "instanceId", "machineId", "master",
            "lastCommittedTransactionId" } )
    public InstanceInfo( String address, String instanceId, int machineId, boolean master,
            long lastTxId )
    {
        this.address = address;
        this.instanceId = instanceId;
        this.machineId = machineId;
        this.master = master;
        this.lastTxId = lastTxId;
    }

    @Override
    @SuppressWarnings( "boxing" )
    public String toString()
    {
        return String.format( "Neo4jHaInstance[id=%s,address=%s,machineId=%s,lastTxId=%s]",
                instanceId, address, machineId, lastTxId );
    }

    public boolean isMaster()
    {
        return master;
    }

    public String getAddress()
    {
        return address;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public int getMachineId()
    {
        return machineId;
    }

    public long getLastCommittedTransactionId()
    {
        return lastTxId;
    }

    public Pair<Neo4jManager, HighAvailability> connect()
    {
        return connect( null, null );
    }

    public Pair<Neo4jManager, HighAvailability> connect( String username, String password )
    {
        if ( address == null )
        {
            throw new IllegalStateException( "The instance does not have a public JMX server." );
        }
        Neo4jManager manager = Neo4jManager.get( url(), username, password, instanceId );
        return Pair.of( manager, manager.getHighAvailabilityBean() );
    }

    private JMXServiceURL url()
    {
        try
        {
            return new JMXServiceURL( address );
        }
        catch ( MalformedURLException e )
        {
            throw new IllegalStateException( "The instance does not have a valid JMX server URL." );
        }
    }
}
