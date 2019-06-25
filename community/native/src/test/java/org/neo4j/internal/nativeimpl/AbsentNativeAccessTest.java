/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.nativeimpl;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class AbsentNativeAccessTest
{
    private final AbsentNativeAccess absentNativeAccess = new AbsentNativeAccess();

    @Test
    void absentNativeAccessIsNotAvailable()
    {
        assertFalse( absentNativeAccess.isAvailable() );
    }

    @Test
    void absentNativeAccessSkipCacheAlwaysFinishSuccessfully()
    {
        assertEquals( 0, absentNativeAccess.tryEvictFromCache( 1 ) );
        assertEquals( 0, absentNativeAccess.tryEvictFromCache( 2 ) );
        assertEquals( 0, absentNativeAccess.tryEvictFromCache( -1 ) );
    }

    @Test
    void absentNativeAccessPreallocationsAlwaysFinishSuccessfully()
    {
        assertEquals( 0, absentNativeAccess.tryPreallocateSpace( 0, 1L ) );
        assertEquals( 0, absentNativeAccess.tryPreallocateSpace( 1, 2L ) );
        assertEquals( 0, absentNativeAccess.tryPreallocateSpace( 3, 4L ) );
    }
}
