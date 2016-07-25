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
package org.neo4j.bolt.v1.runtime.internal;

import org.junit.Test;

import org.neo4j.cypher.LoadExternalResourceException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.Status;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class Neo4jErrorTest
{
    @Test
    public void shouldAssignUnknownStatusToUnpredictedException()
    {
        // Given
        Throwable cause = new Throwable( "This is not an error we know how to handle." );
        Neo4jError error = Neo4jError.from( cause );

        // Then
        assertThat( error.status(), equalTo( (Status) Status.General.UnknownError ) );
    }

    @Test
    public void shouldConvertDeadlockException() throws Throwable
    {
        // When
        Neo4jError error = Neo4jError.from( new DeadlockDetectedException( null ) );

        // Then
        assertEquals( error.status(), Status.Transaction.DeadlockDetected );
    }

    @Test
    public void loadExternalResourceShouldNotReferToLog()
    {
        // Given
        Neo4jError error = Neo4jError.from( new LoadExternalResourceException( "foo", null ) );

        // Then
        assertThat( error.status().code().classification().shouldLog(), is( false ) );
    }
}
