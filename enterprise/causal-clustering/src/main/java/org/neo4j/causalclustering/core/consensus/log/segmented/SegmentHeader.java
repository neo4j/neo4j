/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * The header written at the beginning of each segment.
 */
class SegmentHeader
{
    static final int SIZE = 4 * Long.BYTES;

    private final long prevFileLastIndex;
    private final long version;
    private final long prevIndex;
    private final long prevTerm;

    SegmentHeader( long prevFileLastIndex, long version, long prevIndex, long prevTerm )
    {
        this.prevFileLastIndex = prevFileLastIndex;
        this.version = version;
        this.prevIndex = prevIndex;
        this.prevTerm = prevTerm;
    }

    long prevFileLastIndex()
    {
        return prevFileLastIndex;
    }

    public long version()
    {
        return version;
    }

    public long prevIndex()
    {
        return prevIndex;
    }

    public long prevTerm()
    {
        return prevTerm;
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
        SegmentHeader that = (SegmentHeader) o;
        return prevFileLastIndex == that.prevFileLastIndex &&
               version == that.version &&
               prevIndex == that.prevIndex &&
               prevTerm == that.prevTerm;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( prevFileLastIndex, version, prevIndex, prevTerm );
    }

    @Override
    public String toString()
    {
        return "SegmentHeader{" +
               "prevFileLastIndex=" + prevFileLastIndex +
               ", version=" + version +
               ", prevIndex=" + prevIndex +
               ", prevTerm=" + prevTerm +
               '}';
    }

    static class Marshal extends SafeChannelMarshal<SegmentHeader>
    {
        //@Override
        public void marshal( SegmentHeader header, WritableChannel channel ) throws IOException
        {
            channel.putLong( header.prevFileLastIndex );
            channel.putLong( header.version );
            channel.putLong( header.prevIndex );
            channel.putLong( header.prevTerm );
        }

        //@Override
        public SegmentHeader unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            long prevFileLastIndex = channel.getLong();
            long version = channel.getLong();
            long prevIndex = channel.getLong();
            long prevTerm = channel.getLong();
            return new SegmentHeader( prevFileLastIndex, version, prevIndex, prevTerm );
        }
    }
}
