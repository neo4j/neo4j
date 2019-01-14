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
package org.neo4j.bolt.runtime;

import org.junit.Test;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.Status;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class Neo4jErrorTest
{
    @Test
    public void shouldAssignUnknownStatusToUnpredictedException()
    {
        // Given
        Throwable cause = new Throwable( "This is not an error we know how to handle." );
        Neo4jError error = Neo4jError.from( cause );

        // Then
        assertThat( error.status(), equalTo( Status.General.UnknownError ) );
    }

    @Test
    public void shouldConvertDeadlockException()
    {
        // When
        Neo4jError error = Neo4jError.from( new DeadlockDetectedException( null ) );

        // Then
        assertEquals( Status.Transaction.DeadlockDetected, error.status() );
    }

    @Test
    public void shouldSetStatusToDatabaseUnavailableOnDatabaseShutdownException()
    {
        // Given
        DatabaseShutdownException ex = new DatabaseShutdownException();

        // When
        Neo4jError error = Neo4jError.from( ex );

        // Then
        assertThat( error.status(), equalTo( Status.General.DatabaseUnavailable ) );
        assertThat( error.cause(), equalTo( ex ) );
    }
}
