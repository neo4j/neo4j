/**
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
package org.neo4j.kernel.impl.util.statistics;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;

public class RollingAverageTest
{
    @Test
    public void shouldCalcAverage() throws Exception
    {
        // Given
        RollingAverage avg = new RollingAverage( new RollingAverage.Parameters( 100, RollingAverage.Parameters.DEFAULT_EQUALITY_TOLERANCE ) );

        avg.record( 1 );
        avg.record( 2 );
        avg.record( 2 );
        avg.record( 3 );

        // When
        double result = avg.average();

        // Then
        assertThat(result, closeTo(2.0, 0.01));
    }

    @Test
    public void shouldCalcAverageWhenWindowsShift() throws Exception
    {
        // Given
        RollingAverage avg = new RollingAverage( new RollingAverage.Parameters( 2, RollingAverage.Parameters.DEFAULT_EQUALITY_TOLERANCE ) );

        avg.record( 10 );
        avg.record( 2 );
        avg.record( 2 );
        avg.record( 3 );
        avg.record( 2 );
        avg.record( 2 );

        // When
        double result = avg.average();

        // Then
        assertThat(result, closeTo(2.5, 0.1));
    }

}
