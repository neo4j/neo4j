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
package org.neo4j.kernel.ha.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.neo4j.cluster.protocol.election.ElectionCredentials;

public final class DefaultElectionCredentials implements ElectionCredentials, Externalizable
{
    private int serverId;
    private long latestTxId;
    private boolean currentWinner;

    // For Externalizable
    public DefaultElectionCredentials()
    {}

    public DefaultElectionCredentials( int serverId, long latestTxId, boolean currentWinner )
    {
        this.serverId = serverId;
        this.latestTxId = latestTxId;
        this.currentWinner = currentWinner;
    }

    @Override
    public int compareTo( Object o )
    {
        DefaultElectionCredentials other = (DefaultElectionCredentials) o;
        if ( this.latestTxId == other.latestTxId )
        {
            // Smaller id means higher priority
            if ( this.currentWinner == other.currentWinner )
            {
                return Integer.signum( other.serverId - this.serverId );
            }
            else
            {
                return other.currentWinner ? -1 : 1;
            }
        }
        else
        {
            return Long.signum( this.latestTxId - other.latestTxId );
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj == null )
        {
            return false;
        }
        if ( !(obj instanceof DefaultElectionCredentials ) )
        {
            return false;
        }
        DefaultElectionCredentials other = (DefaultElectionCredentials) obj;
        return other.serverId == this.serverId &&
                other.latestTxId == this.latestTxId &&
                other.currentWinner == this.currentWinner;
    }

    @Override
    public int hashCode()
    {
        return (int) ( 17 * serverId + latestTxId );
    }

    @Override
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeInt( serverId );
        out.writeLong( latestTxId );
        out.writeBoolean( currentWinner );
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        serverId =  in.readInt();
        latestTxId = in.readLong();
        currentWinner = in.readBoolean();
    }

    @Override
    public String toString()
    {
        return "DefaultElectionCredentials[serverId="+serverId +
                ", latestTxId=" + latestTxId +
                ", currentWinner=" + currentWinner+"]";
    }
}
