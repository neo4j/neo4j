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

import org.eclipse.collections.api.map.primitive.ImmutableByteObjectMap;
import org.eclipse.collections.impl.factory.primitive.ByteObjectMaps;

import java.util.List;

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
    V4_2( (byte) 2 ), // 4.2+. Removed checkpoint entries.
    // 4.3(some drop)+. Not a change to log entry format, but record storage engine log format change. Since record storage commands
    // has no command version of their own it relies on a bump of the parser set version to distinguish between versions unfortunately.
    // Also introduces token index and relationship property index features.
    V4_3_D4( (byte) 3 ),
    V4_4( (byte) 4 ), // 4.4. Introduces RANGE, POINT and TEXT index types.
    V5_0( (byte) 5 ); // 5.0.

    public static final KernelVersion EARLIEST = V4_2;
    public static final KernelVersion LATEST = V5_0;
    public static final KernelVersion VERSION_IN_WHICH_TOKEN_INDEXES_ARE_INTRODUCED = V4_3_D4;
    public static final KernelVersion VERSION_RANGE_POINT_TEXT_INDEX_TYPES_ARE_INTRODUCED = V4_4;
    private static final ImmutableByteObjectMap<KernelVersion> versionMap =
            ByteObjectMaps.immutable.from( List.of( values() ), KernelVersion::version, v -> v );

    private final byte version;

    KernelVersion( byte version )
    {
        this.version = version;
    }

    public byte version()
    {
        return this.version;
    }

    public boolean isLatest()
    {
        return this == LATEST;
    }

    public boolean isGreaterThan( KernelVersion other )
    {
        return version > other.version;
    }

    public boolean isLessThan( KernelVersion other )
    {
        return version < other.version;
    }

    public boolean isAtLeast( KernelVersion other )
    {
        return version >= other.version;
    }

    @Override
    public String toString()
    {
        return "KernelVersion{" + name() + ",version=" + version + '}';
    }

    public static KernelVersion getForVersion( byte version )
    {
        KernelVersion kernelVersion = versionMap.get( version );
        if ( kernelVersion == null )
        {
            throw new IllegalArgumentException( "No matching " + KernelVersion.class.getSimpleName() + " for version " + version );
        }
        return kernelVersion;
    }
}
