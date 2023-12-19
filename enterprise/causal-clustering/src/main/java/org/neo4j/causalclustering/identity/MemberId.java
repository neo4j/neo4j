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
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.String.format;

public class MemberId implements Serializable
{
    private static final long serialVersionUID = -984540169345015775L;
    private final UUID uuid;
    private final String shortName;

    public MemberId( UUID uuid )
    {
        Objects.requireNonNull( uuid );
        this.uuid = uuid;
        shortName = uuid.toString().substring( 0, 8 );
    }

    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public String toString()
    {
        return format( "MemberId{%s}", shortName );
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

        MemberId that = (MemberId) o;
        return Objects.equals( uuid, that.uuid );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( uuid );
    }

    /**
     * Format:
     * ┌──────────────────────────────┐
     * │mostSignificantBits    8 bytes│
     * │leastSignificantBits   8 bytes│
     * └──────────────────────────────┘
     */
    public static class Marshal extends SafeStateMarshal<MemberId>
    {
        @Override
        public void marshal( MemberId memberId, WritableChannel channel ) throws IOException
        {
            if ( memberId == null )
            {
                channel.put( (byte) 0 );
            }
            else
            {
                channel.put( (byte) 1 );
                channel.putLong( memberId.uuid.getMostSignificantBits() );
                channel.putLong( memberId.uuid.getLeastSignificantBits() );
            }
        }

        @Override
        public MemberId unmarshal0( ReadableChannel channel ) throws IOException
        {
            byte nullMarker = channel.get();
            if ( nullMarker == 0 )
            {
                return null;
            }
            else
            {
                long mostSigBits = channel.getLong();
                long leastSigBits = channel.getLong();
                return new MemberId( new UUID( mostSigBits, leastSigBits ) );
            }
        }

        @Override
        public MemberId startState()
        {
            return null;
        }

        @Override
        public long ordinal( MemberId memberId )
        {
            return memberId == null ? 0 : 1;
        }
    }
}
