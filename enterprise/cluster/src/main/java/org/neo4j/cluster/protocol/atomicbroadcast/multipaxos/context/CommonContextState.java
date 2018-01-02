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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.logging.Log;

class CommonContextState
{
    private URI boundAt;
    private long lastKnownLearnedInstanceInCluster = -1;
    private org.neo4j.cluster.InstanceId lastKnownAliveUpToDateInstance;
    private long nextInstanceId = 0;
    private ClusterConfiguration configuration;
    private final int maxAcceptors;

    public CommonContextState( ClusterConfiguration configuration, int maxAcceptors )
    {
        this.configuration = configuration;
        this.maxAcceptors = maxAcceptors;
    }

    private CommonContextState( URI boundAt, long lastKnownLearnedInstanceInCluster, long nextInstanceId,
                        ClusterConfiguration configuration, int maxAcceptors )
    {
        this.boundAt = boundAt;
        this.lastKnownLearnedInstanceInCluster = lastKnownLearnedInstanceInCluster;
        this.nextInstanceId = nextInstanceId;
        this.configuration = configuration;
        this.maxAcceptors = maxAcceptors;
    }

    public URI boundAt()
    {
        return boundAt;
    }

    public void setBoundAt( InstanceId me, URI boundAt )
    {
        this.boundAt = boundAt;
        configuration.getMembers().put( me, boundAt );
    }

    public long lastKnownLearnedInstanceInCluster()
    {
        return lastKnownLearnedInstanceInCluster;
    }

    public void setLastKnownLearnedInstanceInCluster( long lastKnownLearnedInstanceInCluster, InstanceId instanceId )
    {
        if(this.lastKnownLearnedInstanceInCluster <= lastKnownLearnedInstanceInCluster)
        {
            this.lastKnownLearnedInstanceInCluster = lastKnownLearnedInstanceInCluster;
            if ( instanceId != null )
            {
                this.lastKnownAliveUpToDateInstance = instanceId;
            }
        }
        else if(lastKnownLearnedInstanceInCluster == -1)
        {
            // Special case for clearing the state
            this.lastKnownLearnedInstanceInCluster = -1;
        }
    }

    public org.neo4j.cluster.InstanceId getLastKnownAliveUpToDateInstance()
    {
        return this.lastKnownAliveUpToDateInstance;
    }

    public long nextInstanceId()
    {
        return nextInstanceId;
    }

    public void setNextInstanceId( long nextInstanceId )
    {
        this.nextInstanceId = nextInstanceId;
    }

    public long getAndIncrementInstanceId()
    {
        return nextInstanceId++;
    }

    public ClusterConfiguration configuration()
    {
        return configuration;
    }

    public void setConfiguration( ClusterConfiguration configuration )
    {
        this.configuration = configuration;
    }

    public int getMaxAcceptors()
    {
        return maxAcceptors;
    }

    public CommonContextState snapshot( Log log )
    {
        return new CommonContextState( boundAt, lastKnownLearnedInstanceInCluster, nextInstanceId,
                configuration.snapshot(log), maxAcceptors );
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

        CommonContextState that = (CommonContextState) o;

        if ( lastKnownLearnedInstanceInCluster != that.lastKnownLearnedInstanceInCluster )
        {
            return false;
        }
        if ( nextInstanceId != that.nextInstanceId )
        {
            return false;
        }
        if ( boundAt != null ? !boundAt.equals( that.boundAt ) : that.boundAt != null )
        {
            return false;
        }
        if ( configuration != null ? !configuration.equals( that.configuration ) : that.configuration != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = boundAt != null ? boundAt.hashCode() : 0;
        result = 31 * result + (int) (lastKnownLearnedInstanceInCluster ^ (lastKnownLearnedInstanceInCluster >>> 32));
        result = 31 * result + (int) (nextInstanceId ^ (nextInstanceId >>> 32));
        result = 31 * result + (configuration != null ? configuration.hashCode() : 0);
        return result;
    }
}
