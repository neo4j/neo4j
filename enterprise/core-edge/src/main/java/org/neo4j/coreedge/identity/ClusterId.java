/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.identity;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.neo4j.coreedge.core.state.storage.SafeChannelMarshal;
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
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
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
