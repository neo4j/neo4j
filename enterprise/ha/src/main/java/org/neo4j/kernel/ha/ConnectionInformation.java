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
package org.neo4j.kernel.ha;

import java.net.MalformedURLException;

import javax.management.remote.JMXServiceURL;

import org.neo4j.kernel.ha.zookeeper.Machine;

public class ConnectionInformation
{
    private final boolean master;
    private final int machineId;
    private JMXServiceURL jmxURL;
    private String instanceId;
    private final long txId;

    public ConnectionInformation( Machine machine, boolean master )
    {
        this.master = master;
        this.machineId = machine.getMachineId();
        this.txId = machine.getLastCommittedTxId();
    }

    public void setJMXConnectionData( String url, String instanceId )
    {
        this.instanceId = instanceId;
        try
        {
            this.jmxURL = new JMXServiceURL( url );
        }
        catch ( MalformedURLException e )
        {
            this.jmxURL = null;
        }
    }

    public JMXServiceURL getJMXServiceURL()
    {
        return jmxURL;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public int getMachineId()
    {
        return machineId;
    }

    public boolean isMaster()
    {
        return master;
    }

    public long getLastCommitedTransactionId()
    {
        return txId;
    }
}
