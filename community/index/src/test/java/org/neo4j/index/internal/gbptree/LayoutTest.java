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
package org.neo4j.index.internal.gbptree;

import org.junit.Test;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class LayoutTest
{
    @Test
    public void shouldCreateDifferentIdentifierWithDifferentName()
    {
        // GIVEN
        String firstName = "one";
        String secondName = "two";
        int checksum = 123;

        // WHEN
        long firstIdentifier = Layout.namedIdentifier( firstName, checksum );
        long secondIdentifier = Layout.namedIdentifier( secondName, checksum );

        // THEN
        assertNotEquals( firstIdentifier, secondIdentifier );
    }

    @Test
    public void shouldCreateDifferentIdentifierWithDifferentChecksums()
    {
        // GIVEN
        String name = "name";
        int firstChecksum = 123;
        int secondChecksum = 456;

        // WHEN
        long firstIdentifier = Layout.namedIdentifier( name, firstChecksum );
        long secondIdentifier = Layout.namedIdentifier( name, secondChecksum );

        // THEN
        assertNotEquals( firstIdentifier, secondIdentifier );
    }

    @Test
    public void shouldFailOnTooLongName()
    {
        // WHEN
        try
        {
            Layout.namedIdentifier( "too-long", 12 );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN good
        }
    }
}
