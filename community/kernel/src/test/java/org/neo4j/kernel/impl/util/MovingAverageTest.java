/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import org.junit.Test;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MovingAverageTest
{
    @Test
    public void shouldHaveAverageMovingWithChanges() throws Exception
    {
        // GIVEN
        MovingAverage average = new MovingAverage( 5 );

        // WHEN moving to 10 as average
        long avg = average.average();
        for ( int i = 0; i < 5; i++ )
        {
            average.add( 10 );
            assertEquals( 10L, average.average() );
            avg = average.average();
        }
        assertEquals( 10L, average.average() );

        // WHEN moving to 100 as average
        for ( int i = 0; i < 5; i++ )
        {
            average.add( 100 );
            assertThat( average.average(), greaterThan( avg ) );
            avg = average.average();
        }
        assertEquals( 100L, average.average() );
    }
}
