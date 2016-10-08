/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.security;

import org.junit.Test;

import org.neo4j.kernel.api.security.AccessMode;

import static org.junit.Assert.assertEquals;

public class AccessModeSnapshotTest
{
    @Test
    public void shouldCorrectlyReflectAccessMode()
    {
        testAccessMode( AccessMode.Static.READ );
        testAccessMode( AccessMode.Static.WRITE );
        testAccessMode( AccessMode.Static.WRITE_ONLY );
        testAccessMode( AccessMode.Static.FULL );
        testAccessMode( AccessMode.Static.OVERRIDE_READ );
        testAccessMode( AccessMode.Static.OVERRIDE_WRITE );
        testAccessMode( AccessMode.Static.OVERRIDE_SCHEMA );
        testAccessMode( AccessMode.Static.CREDENTIALS_EXPIRED );
    }

    private void testAccessMode( AccessMode originalAccessMode )
    {
        AccessMode accessModeSnapshot = AccessModeSnapshot.createAccessModeSnapshot( originalAccessMode );
        assertEquals( accessModeSnapshot.allowsReads(), originalAccessMode.allowsReads() );
        assertEquals( accessModeSnapshot.allowsWrites(), originalAccessMode.allowsWrites() );
        assertEquals( accessModeSnapshot.allowsSchemaWrites(), originalAccessMode.allowsSchemaWrites() );
        assertEquals( accessModeSnapshot.overrideOriginalMode(), originalAccessMode.overrideOriginalMode() );
        assertEquals( accessModeSnapshot.name(), originalAccessMode.name() );
    }
}
