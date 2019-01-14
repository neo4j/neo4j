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
package org.neo4j.concurrent;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DecayingFlagsTest
{
    @Test
    public void shouldTrackToggle()
    {
        // Given
        DecayingFlags.Key myFeature = new DecayingFlags.Key( 1 );
        DecayingFlags set = new DecayingFlags( 1 );

        // When
        set.flag( myFeature );

        // Then
        assertEquals( "4000", set.asHex() );
    }

    @Test
    public void shouldTrackMultipleFlags()
    {
        // Given
        DecayingFlags.Key featureA = new DecayingFlags.Key( 1 );
        DecayingFlags.Key featureB = new DecayingFlags.Key( 3 );
        DecayingFlags set = new DecayingFlags( 2 );

        // When
        set.flag( featureA );
        set.flag( featureA );
        set.flag( featureB );

        // Then
        assertEquals( "5000", set.asHex() );
    }

    @Test
    public void toggleShouldDecay()
    {
        // Given
        DecayingFlags.Key featureA = new DecayingFlags.Key( 1 );
        DecayingFlags.Key featureB = new DecayingFlags.Key( 3 );
        DecayingFlags set = new DecayingFlags( 2 );

        // And given Feature A has been used quite a bit, while
        // feature B is not quite as popular..
        set.flag( featureA );
        set.flag( featureA );
        set.flag( featureB );

        // When
        set.sweep();

        // Then
        assertEquals( "4000", set.asHex() );

        // When
        set.sweep();

        // Then
        assertEquals( "0000", set.asHex() );
    }

    @Test
    public void resetFlagShouldRecoverIfToggledAgain()
    {
        // Given
        DecayingFlags.Key featureA = new DecayingFlags.Key( 9 );
        DecayingFlags set = new DecayingFlags( 2 );

        set.flag( featureA );

        // When
        set.sweep();

        // Then
        assertEquals( "0000", set.asHex() );

        // When
        set.flag( featureA );

        // Then
        assertEquals( "0040", set.asHex() );
    }
}
