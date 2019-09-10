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
package org.neo4j.kernel.database;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GlobalAvailabilityGuardControllerTest
{
    private final CompositeDatabaseAvailabilityGuard guard = mock( CompositeDatabaseAvailabilityGuard.class );
    private final GlobalAvailabilityGuardController guardController = new GlobalAvailabilityGuardController( guard );

    @Test
    void doNotAbortOnRunning()
    {
        when( guard.isShutdown() ).thenReturn( false );
        assertFalse( guardController.shouldAbort( new NamedDatabaseId( "any", randomUUID() ) ) );
    }

    @Test
    void abortOnShutdown()
    {
        when( guard.isShutdown() ).thenReturn( true );
        assertTrue( guardController.shouldAbort( new NamedDatabaseId( "any", randomUUID() ) ) );
    }
}
