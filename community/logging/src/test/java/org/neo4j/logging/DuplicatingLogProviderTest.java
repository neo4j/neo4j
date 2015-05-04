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
package org.neo4j.logging;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class DuplicatingLogProviderTest
{
    @Test
    public void shouldReturnSameLoggerForSameClass()
    {
        // Given
        DuplicatingLogProvider logProvider = new DuplicatingLogProvider();

        // Then
        DuplicatingLog log = logProvider.getLog( getClass() );
        assertThat( logProvider.getLog( DuplicatingLogProviderTest.class ), sameInstance( log ) );
    }

    @Test
    public void shouldReturnSameLoggerForSameContext()
    {
        // Given
        DuplicatingLogProvider logProvider = new DuplicatingLogProvider();

        // Then
        DuplicatingLog log = logProvider.getLog( "test context" );
        assertThat( logProvider.getLog( "test context" ), sameInstance( log ) );
    }
}
