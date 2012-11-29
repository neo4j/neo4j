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
package org.neo4j.kernel.ha.cluster.zoo;

import java.net.URI;

import org.neo4j.helpers.Pair;

public class Machine
{
    private final int machineId;
    private final int sequenceId;
    private final long lastCommittedTxId;
    private final Pair<String, Integer> server;
    private final int backupPort;

    private final int masterForCommittedTxId;

    public Machine( int machineId, int sequenceId, long lastCommittedTxId,
                    int masterForCommittedTxId, String server, int backupPort )
    {
        this.machineId = machineId;
        this.sequenceId = sequenceId;
        this.lastCommittedTxId = lastCommittedTxId;
        this.masterForCommittedTxId = masterForCommittedTxId;
        this.server = server != null ? splitIpAndPort( server ) : null;
        this.backupPort = backupPort;
    }

    public int getMachineId()
    {
        return machineId;
    }

    public long getLastCommittedTxId()
    {
        return lastCommittedTxId;
    }

    public boolean wasCommittingMaster()
    {
        return masterForCommittedTxId == machineId;
    }

    public int getMasterForCommittedTxId()
    {
        return masterForCommittedTxId;
    }

    public int getSequenceId()
    {
        return sequenceId;
    }

    public Pair<String, Integer> getServer()
    {
        return server;
    }

    public String getServerAsString()
    {
        return server == null ? null : server.first() + ":" + server.other()+"?serverId="+machineId;
    }

    public int getBackupPort()
    {
        return backupPort;
    }

    @Override
    public String toString()
    {
        return "MachineInfo[ID:" + machineId + ", sequence:" + sequenceId +
                ", last tx:" + lastCommittedTxId + ", server:" + server +
                ", master for last tx:" + masterForCommittedTxId + "]";
    }

    @Override
    public boolean equals( Object obj )
    {
        return (obj instanceof Machine) && ((Machine) obj).machineId == machineId;
    }

    @Override
    public int hashCode()
    {
        return machineId*19;
    }

    public static Pair<String, Integer> splitIpAndPort( String server )
    {
        String[] pos = server.split( ":" );
        if (pos.length == 3)
        {
            URI serverURI = URI.create(server);
            return Pair.of( serverURI.getHost(), serverURI.getPort() );
//            return Pair.of( pos[1].substring( 2 ),
//                    Integer.parseInt( pos[2] ) );
        }
        else
        {
            return Pair.of( pos[0], Integer.parseInt( pos[1].split( "\\?" )[0] ) );
        }

    }
}
