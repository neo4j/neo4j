/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt;

import java.util.Objects;

import org.neo4j.memory.HeapEstimator;

public class BoltProtocolVersion
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( BoltProtocolVersion.class );

    private int majorVersion;
    private int minorVersion;

    public BoltProtocolVersion( int majorVersion, int minorVersion )
    {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    public static BoltProtocolVersion fromRawBytes( int rawVersion )
    {
        return new BoltProtocolVersion( getMajorFromRawBytes(rawVersion), getMinorFromRawBytes(rawVersion) );
    }

    public static int getMinorFromRawBytes( int rawVersion )
    {
        return ( rawVersion >> 8 ) & 0x000000FF;
    }

    public static int getMajorFromRawBytes( int rawVersion )
    {
        return rawVersion & 0x000000FF;
    }

    public static int getRangeFromRawBytes( int rawVersion )
    {
        return ( rawVersion >> 16 ) & 0x000000FF;
    }

    public long getMinorVersion()
    {
        return minorVersion;
    }

    public long getMajorVersion()
    {
        return majorVersion;
    }

    public int toInt()
    {
        int shiftedMinor = minorVersion << 8;
        return shiftedMinor | majorVersion;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( minorVersion, majorVersion );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o == this )
        {
            return true;
        }
        else if ( !(o instanceof BoltProtocolVersion other) )
        {
            return false;
        }
        else
        {
            return this.getMajorVersion() == other.getMajorVersion() && this.getMinorVersion() == other.getMinorVersion();
        }
    }
}
