/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

public class BoltProtocolVersion
{
    private int majorVersion;
    private int minorVersion;

    public BoltProtocolVersion( int majorVersion, int minorVersion )
    {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    public static BoltProtocolVersion fromRawBytes( int rawVersion )
    {
        int major = rawVersion & 0x000000FF;
        int minor = ( rawVersion >> 8 ) & 0x000000FF;

        return new BoltProtocolVersion( major, minor );
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
        else if ( !(o instanceof BoltProtocolVersion) )
        {
            return false;
        }
        else
        {
            BoltProtocolVersion other = (BoltProtocolVersion) o;
            return this.getMajorVersion() == other.getMajorVersion() && this.getMinorVersion() == other.getMinorVersion();
        }
    }
}
