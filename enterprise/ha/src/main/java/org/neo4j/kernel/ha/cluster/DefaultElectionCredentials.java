/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
    public int compareTo( ElectionCredentials o )
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
    public void readExternal( ObjectInput in ) throws IOException
    {
        serverId =  in.readInt();
        latestTxId = in.readLong();
        currentWinner = in.readBoolean();
    }

    @Override
    public String toString()
    {
        return "DefaultElectionCredentials[serverId=" + serverId +
                ", latestTxId=" + latestTxId +
                ", currentWinner=" + currentWinner + "]";
    }
}
