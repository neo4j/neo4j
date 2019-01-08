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
package org.neo4j.kernel.impl.factory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.logging.Log;

import static org.mockito.Mockito.mock;

public class EditionModuleTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldFailWhenAuthEnabledAndNoSecurityModuleFound()
    {
        // Expect
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Failed to load security module with key 'non-existent-security-module'" );

        // When
        EditionModule.setupSecurityModule( null, mock( Log.class ), null, "non-existent-security-module" );
    }
}
