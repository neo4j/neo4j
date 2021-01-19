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
package org.neo4j.kernel;

import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.util.Preconditions;

/**
 * One version scheme to unify various internal versions into one with the intent of conceptual simplification and simplification of version bumping.
 * The existing version byte codes originally comes from legacy versioning of the log entry versions. This kernel version now controls that said version
 * as well as the explicitly set version which a database is set to run with.
 *
 * On a high level there's a DBMS runtime version which granularity is finer and is therefore a super set of the version set in here, which only
 * contains versions that has some sort of format change. This kernel version codes doesn't follow the same codes as the DBMS runtime version codes
 * and kernel will have a translation between the two.
 */
public enum KernelVersion
{
    UNKNOWN( (byte) -1 ),
    V2_3( (byte) -10 ), // 2.3 to 3.5.
    V4_0( (byte) 1 ), // 4.0 to 4.1. Added checksums to the log files.
    V4_2( (byte) 2 ); // 4.2+. Removed checkpoint entries.

    public static KernelVersion LATEST = V4_2;
    private static final KernelVersion[] sortedVersions = Arrays.stream( values() )
            .filter( KernelVersion::isKnown )
            .sorted( Comparator.comparingInt( KernelVersion::version ).reversed() )
            .toArray( KernelVersion[]::new );

    private final byte version;

    KernelVersion( byte version )
    {
        this.version = version;
    }

    public byte version()
    {
        return this.version;
    }

    public boolean isKnown()
    {
        return this != UNKNOWN;
    }

    public boolean isLatest()
    {
        return this == LATEST;
    }

    public boolean isGreaterThan( KernelVersion other )
    {
        Preconditions.checkState( this != UNKNOWN, "Cannot compare " + UNKNOWN );
        Preconditions.checkArgument( other != UNKNOWN, "Cannot compare " + UNKNOWN );
        return version > other.version;
    }

    @Override
    public String toString()
    {
        return "KernelVersion{" + name() + ",version=" + version + '}';
    }

    public static KernelVersion getForVersion( byte version )
    {
        if ( version <= LATEST.version() )
        {
            for ( KernelVersion kernelVersion : sortedVersions )
            {
                if ( version >= kernelVersion.version() )
                {
                    return kernelVersion;
                }
            }
        }
        throw new IllegalArgumentException( "No matching " + KernelVersion.class.getSimpleName() + " for version " + version );
    }
}
