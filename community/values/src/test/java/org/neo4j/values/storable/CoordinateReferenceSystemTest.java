/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.values.storable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;

import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.InvalidArgumentException;

class CoordinateReferenceSystemTest {
    @Test
    void shouldGetCrsByCode() {
        assertEquals(CARTESIAN, CoordinateReferenceSystem.get(CARTESIAN.getCode()));
        assertEquals(WGS_84, CoordinateReferenceSystem.get(WGS_84.getCode()));
    }

    @Test
    void shouldFailToGetWithIncorrectCode() {
        InvalidArgumentException exception =
                assertThrows(InvalidArgumentException.class, () -> CoordinateReferenceSystem.get(42));
        assertEquals("Unknown coordinate reference system: code=42", exception.getMessage());
    }

    @Test
    void shouldFindByTableAndCode() {
        assertThat(CoordinateReferenceSystem.get(1, 4326)).isEqualTo(CoordinateReferenceSystem.WGS_84);
        assertThat(CoordinateReferenceSystem.get(1, 4979)).isEqualTo(CoordinateReferenceSystem.WGS_84_3D);
        assertThat(CoordinateReferenceSystem.get(2, 7203)).isEqualTo(CoordinateReferenceSystem.CARTESIAN);
        assertThat(CoordinateReferenceSystem.get(2, 9157)).isEqualTo(CoordinateReferenceSystem.CARTESIAN_3D);
    }

    @Test
    void shouldCalculateCartesianDistance() {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.CARTESIAN;
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0), cart(0.0, 1.0))).isEqualTo(1.0);
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0), cart(1.0, 0.0))).isEqualTo(1.0);
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0), cart(1.0, 1.0))).isCloseTo(1.4, offset(0.02));
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0), cart(0.0, -1.0)))
                .isEqualTo(1.0);
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0), cart(-1.0, 0.0)))
                .isEqualTo(1.0);
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0), cart(-1.0, -1.0)))
                .isCloseTo(1.4, offset(0.02));
        assertThat(crs.getCalculator().distance(cart(1.0, 0.0), cart(0.0, -1.0)))
                .isCloseTo(1.4, offset(0.02));
        assertThat(crs.getCalculator().distance(cart(1.0, 0.0), cart(-1.0, 0.0)))
                .isEqualTo(2.0);
        assertThat(crs.getCalculator().distance(cart(1.0, 0.0), cart(-1.0, -1.0)))
                .isCloseTo(2.24, offset(0.01));
        assertThat(crs.getCalculator().distance(cart(-1000000.0, -1000000.0), cart(1000000.0, 1000000.0)))
                .isCloseTo(2828427.0, offset(1.0));
    }

    @Test
    void shouldCalculateCartesianDistance3D() {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.CARTESIAN_3D;
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0, 0.0), cart(1.0, 0.0, 0.0)))
                .isEqualTo(1.0);
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0, 0.0), cart(0.0, 1.0, 0.0)))
                .isEqualTo(1.0);
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0, 0.0), cart(0.0, 0.0, 1.0)))
                .isEqualTo(1.0);
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0, 0.0), cart(0.0, 1.0, 1.0)))
                .isCloseTo(1.41, offset(0.01));
        assertThat(crs.getCalculator().distance(cart(0.0, 0.0, 0.0), cart(1.0, 1.0, 1.0)))
                .isCloseTo(1.73, offset(0.01));
        assertThat(crs.getCalculator()
                        .distance(cart(-1000000.0, -1000000.0, -1000000.0), cart(1000000.0, 1000000.0, 1000000.0)))
                .isCloseTo(3464102.0, offset(1.0));
    }

    @Test
    void shouldCalculateGeographicDistance() {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS_84;
        assertThat(crs.getCalculator().distance(geo(0.0, 0.0), geo(0.0, 90.0)))
                .as("2D distance should match")
                .isCloseTo(10000000.0, offset(20000.0));
        assertThat(crs.getCalculator().distance(geo(0.0, 0.0), geo(0.0, -90.0)))
                .as("2D distance should match")
                .isCloseTo(10000000.0, offset(20000.0));
        assertThat(crs.getCalculator().distance(geo(0.0, -45.0), geo(0.0, 45.0)))
                .as("2D distance should match")
                .isCloseTo(10000000.0, offset(20000.0));
        assertThat(crs.getCalculator().distance(geo(-45.0, 0.0), geo(45.0, 0.0)))
                .as("2D distance should match")
                .isCloseTo(10000000.0, offset(20000.0));
        assertThat(crs.getCalculator().distance(geo(-45.0, 0.0), geo(45.0, 0.0)))
                .as("2D distance should match")
                .isCloseTo(10000000.0, offset(20000.0));
        // "distance function should measure distance from Copenhagen train station to Neo4j in Malmö"
        PointValue cph = geo(12.564590, 55.672874);
        PointValue malmo = geo(12.994341, 55.611784);
        double expected = 27842.0;
        assertThat(crs.getCalculator().distance(cph, malmo))
                .as("2D distance should match")
                .isCloseTo(expected, offset(0.1));
    }

    @Test
    void shouldCalculateGeographicDistance3D() {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS_84_3D;
        // "distance function should measure distance from Copenhagen train station to Neo4j in Malmö"
        PointValue cph = geo(12.564590, 55.672874, 0.0);
        PointValue cphHigh = geo(12.564590, 55.672874, 1000.0);
        PointValue malmo = geo(12.994341, 55.611784, 0.0);
        PointValue malmoHigh = geo(12.994341, 55.611784, 1000.0);
        double expected = 27842.0;
        double expectedHigh = 27862.0;
        assertThat(crs.getCalculator().distance(cph, malmo))
                .as("3D distance should match")
                .isCloseTo(expected, offset(0.1));
        assertThat(crs.getCalculator().distance(cph, malmoHigh))
                .as("3D distance should match")
                .isCloseTo(expectedHigh, offset(0.2));
        assertThat(crs.getCalculator().distance(cphHigh, malmo))
                .as("3D distance should match")
                .isCloseTo(expectedHigh, offset(0.2));
    }

    private static PointValue cart(double... coords) {
        CoordinateReferenceSystem crs =
                coords.length == 3 ? CoordinateReferenceSystem.CARTESIAN_3D : CoordinateReferenceSystem.CARTESIAN;
        return Values.pointValue(crs, coords);
    }

    private static PointValue geo(double... coords) {
        CoordinateReferenceSystem crs =
                coords.length == 3 ? CoordinateReferenceSystem.CARTESIAN_3D : CoordinateReferenceSystem.CARTESIAN;
        return Values.pointValue(crs, coords);
    }
}
