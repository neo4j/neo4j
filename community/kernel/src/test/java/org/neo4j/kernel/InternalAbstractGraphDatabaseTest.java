/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.InternalAbstractGraphDatabase.Dependencies;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class InternalAbstractGraphDatabaseTest
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldThrowAppropriateExceptionIfStartFails()
    {
        // Given
        RuntimeException startupError = new RuntimeException();

        InternalAbstractGraphDatabase db = newFaultyInternalAbstractGraphDatabase( startupError, null );

        try
        {
            // When
            db.run();
            fail( "Should have thrown " + RuntimeException.class );
        }
        catch ( RuntimeException exception )
        {
            // Then
            assertEquals( startupError, Exceptions.rootCause( exception ) );
        }
    }

    @Test
    public void shouldThrowAppropriateExceptionIfBothStartAndShutdownFail()
    {
        // Given
        RuntimeException startupError = new RuntimeException();
        RuntimeException shutdownError = new RuntimeException();

        InternalAbstractGraphDatabase db = newFaultyInternalAbstractGraphDatabase( startupError, shutdownError );

        try
        {
            // When
            db.run();
            fail( "Should have thrown " + RuntimeException.class );
        }
        catch ( RuntimeException exception )
        {
            // Then
            assertEquals( shutdownError, exception );
            assertEquals( startupError, Exceptions.rootCause( exception.getSuppressed()[0] ) );
        }
    }

    private InternalAbstractGraphDatabase newFaultyInternalAbstractGraphDatabase( final RuntimeException startupError,
            final RuntimeException shutdownError )
    {
        Dependencies mockedDependencies = mock( Dependencies.class, RETURNS_MOCKS );

        return new InternalAbstractGraphDatabase( dir.absolutePath(), stringMap(), mockedDependencies )
        {
            @Override
            protected void create()
            {
                availabilityGuard = mock( AvailabilityGuard.class );
            }

            @Override
            protected void registerRecovery()
            {
                if ( startupError != null )
                {
                    throw startupError;
                }
            }

            @Override
            public void shutdown()
            {
                if ( shutdownError != null )
                {
                    throw shutdownError;
                }
            }
        };
    }
}
