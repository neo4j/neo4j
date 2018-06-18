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
package org.neo4j.causalclustering.discovery;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.discovery.data.Transient;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class TransientClusterId implements Transient, Serializable
{

    private static final long serialVersionUID = 24070572631L;

    private final ClusterId clusterId;
    private final Instant lastActive;

    public TransientClusterId( ClusterId clusterId, Instant lastActive )
    {
        this.clusterId = clusterId;
        this.lastActive = lastActive;
    }

    public Instant lastActive()
    {
        return lastActive;
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public UUID uuid()
    {
        return clusterId.uuid();
    }

    public boolean isActiveDuringLast( Duration timeout )
    {
        return isActiveDuringLast( timeout, Instant.now() );
    }

    @Override
    public boolean isActiveDuringLast( Duration timeout, Instant now )
    {
        Duration elapsed = Duration.between( now, lastActive );
        return elapsed.compareTo( timeout ) <= 0;
    }

    public TransientClusterId touchAt( Instant time )
    {
        if ( time.isAfter( lastActive ) )
        {
            return new TransientClusterId( clusterId, time );
        }
        else
        {
            return this;
        }
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
        TransientClusterId that = (TransientClusterId) o;
        return Objects.equals( clusterId, that.clusterId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( clusterId );
    }

    public static class Marshal extends SafeChannelMarshal<TransientClusterId>
    {
        public static Marshal INSTANCE = new Marshal();
        private static ClusterId.Marshal clusterIdMarshal = ClusterId.Marshal.INSTANCE;

        @Override
        protected TransientClusterId unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            ClusterId clusterId = clusterIdMarshal.unmarshal( channel );
            long lastActiveEpochSecond = channel.getLong();
            Instant lastActive = Instant.ofEpochSecond( lastActiveEpochSecond );
            return new TransientClusterId( clusterId, lastActive );
        }

        @Override
        public void marshal( TransientClusterId transientClusterId, WritableChannel channel ) throws IOException
        {
            clusterIdMarshal.marshal( transientClusterId.clusterId, channel );
            channel.putLong( transientClusterId.lastActive.getEpochSecond() );
        }
    }
}
