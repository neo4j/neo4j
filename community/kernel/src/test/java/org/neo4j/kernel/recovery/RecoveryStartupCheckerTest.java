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
package org.neo4j.kernel.recovery;

import org.junit.jupiter.api.Test;

import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class RecoveryStartupCheckerTest
{
    private final DatabaseStartupController startupController = mock( DatabaseStartupController.class );
    private final NamedDatabaseId namedDatabaseId = randomNamedDatabaseId();

    @Test
    void throwAboutExceptionOnShouldAbort()
    {
        var recoveryStartupChecker = new RecoveryStartupChecker( startupController, namedDatabaseId );

        assertDoesNotThrow( recoveryStartupChecker::checkIfCanceled );

        when( startupController.shouldAbort( namedDatabaseId ) ).thenReturn( true );
        assertThrows( DatabaseStartAbortedException.class, recoveryStartupChecker::checkIfCanceled );
    }
}
