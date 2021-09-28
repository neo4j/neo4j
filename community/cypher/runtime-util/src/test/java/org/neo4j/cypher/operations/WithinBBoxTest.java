/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.operations;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.cypher.operations.CypherFunctions.withinBBox;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;

class WithinBBoxTest
{
    private final Random random = ThreadLocalRandom.current();
    private static final int ITERATIONS = 1000;

    @Test
    void testInclusivePoints()
    {
        var lowerLeft = pointValue( Cartesian, 0.0, 0.0 );
        var upperRight = pointValue( Cartesian, 1.0, 1.0 );

        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertThat(
                    withinBBox(  pointValue( Cartesian, random.nextDouble(), random.nextDouble() ), lowerLeft, upperRight ) )
                    .isEqualTo( TRUE );

        }
    }

    @Test
    void testBoundaryPoints()
    {
        var lowerLeft = pointValue( Cartesian, 0.0, 0.0 );
        var upperRight = pointValue( Cartesian, 1.0, 1.0 );

        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertThat(
                    withinBBox(  pointValue( Cartesian, 0.0, random.nextDouble() ), lowerLeft, upperRight ) )
                    .isEqualTo( TRUE );
            assertThat(
                    withinBBox(  pointValue( Cartesian, 1.0, random.nextDouble() ), lowerLeft, upperRight ) )
                    .isEqualTo( TRUE );
            assertThat(
                    withinBBox(  pointValue( Cartesian, random.nextDouble(), 0.0 ), lowerLeft, upperRight ) )
                    .isEqualTo( TRUE );
            assertThat(
                    withinBBox(  pointValue( Cartesian, random.nextDouble(), 1.0 ), lowerLeft, upperRight ) )
                    .isEqualTo( TRUE );

        }
    }

    @Test
    void testPointsOutsideBBox()
    {
        var lowerLeft = pointValue( Cartesian, 2.0, 2.0 );
        var upperRight = pointValue( Cartesian, 3.0, 3.0 );

        for ( int i = 0; i < ITERATIONS; i++ )
        {
            assertThat(
                    withinBBox(  pointValue( Cartesian, random.nextDouble(), random.nextDouble() ), lowerLeft, upperRight ) )
                    .isEqualTo( FALSE );
        }
    }

    @Test
    void testNullInNullOut()
    {
        var lowerLeft = pointValue( Cartesian, 0.0, 0.0 );
        var upperRight = pointValue( Cartesian, 1.0, 1.0 );

        assertThat(
                withinBBox( NO_VALUE, lowerLeft, upperRight ) )
                .isEqualTo( NO_VALUE );
        assertThat(
                withinBBox( pointValue( Cartesian, random.nextDouble(), random.nextDouble() ), NO_VALUE, upperRight ) )
                .isEqualTo( NO_VALUE );
        assertThat(
                withinBBox( pointValue( Cartesian, random.nextDouble(), random.nextDouble() ), lowerLeft, NO_VALUE ) )
                .isEqualTo( NO_VALUE );
    }

    @Test
    void testInvalidTypes()
    {
        var lowerLeft = pointValue( Cartesian, 0.0, 0.0 );
        var upperRight = pointValue( Cartesian, 1.0, 1.0 );

        assertThat(
                withinBBox( longValue(15), lowerLeft, upperRight ) )
                .isEqualTo( NO_VALUE );
        assertThat(
                withinBBox( pointValue( Cartesian, random.nextDouble(), random.nextDouble() ), stringValue("I'm a point"), upperRight ) )
                .isEqualTo( NO_VALUE );
        assertThat(
                withinBBox( pointValue( Cartesian, random.nextDouble(), random.nextDouble() ), lowerLeft, TRUE ) )
                .isEqualTo( NO_VALUE );
    }
}
