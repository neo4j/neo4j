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
package org.neo4j.causalclustering.identity;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ClusterId
{
    private final UUID uuid;

    public ClusterId( UUID uuid )
    {
        this.uuid = uuid;
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
        ClusterId clusterId = (ClusterId) o;
        return Objects.equals( uuid, clusterId.uuid );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( uuid );
    }

    public UUID uuid()
    {
        return uuid;
    }

    @Override
    public String toString()
    {
        return "ClusterId{" +
               "uuid=" + uuid +
               '}';
    }

    public static class Marshal extends SafeChannelMarshal<ClusterId>
    {
        public static final Marshal INSTANCE = new Marshal();

        @Override
        public void marshal( ClusterId clusterId, WritableChannel channel ) throws IOException
        {
            channel.putLong( clusterId.uuid.getMostSignificantBits() );
            channel.putLong( clusterId.uuid.getLeastSignificantBits() );
        }

        @Override
        public ClusterId unmarshal0( ReadableChannel channel ) throws IOException
        {
            long mostSigBits = channel.getLong();
            long leastSigBits = channel.getLong();
            return new ClusterId( new UUID( mostSigBits, leastSigBits ) );
        }
    }
}
