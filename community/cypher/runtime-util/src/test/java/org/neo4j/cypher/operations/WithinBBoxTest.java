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
package org.neo4j.cypher.operations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.cypher.operations.CypherFunctions.withinBBox;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

class WithinBBoxTest {
    private final Random random = ThreadLocalRandom.current();
    private static final int ITERATIONS = 1000;

    @Test
    void testInclusivePoints() {
        var lowerLeft = pointValue(CARTESIAN, 0.0, 0.0);
        var upperRight = pointValue(CARTESIAN, 1.0, 1.0);

        for (int i = 0; i < ITERATIONS; i++) {
            assertThat(withinBBox(
                            pointValue(CARTESIAN, random.nextDouble(), random.nextDouble()), lowerLeft, upperRight))
                    .isEqualTo(TRUE);
        }
    }

    @Test
    void testBoundaryPoints() {
        var lowerLeft = pointValue(CARTESIAN, 0.0, 0.0);
        var upperRight = pointValue(CARTESIAN, 1.0, 1.0);

        for (int i = 0; i < ITERATIONS; i++) {
            assertThat(withinBBox(pointValue(CARTESIAN, 0.0, random.nextDouble()), lowerLeft, upperRight))
                    .isEqualTo(TRUE);
            assertThat(withinBBox(pointValue(CARTESIAN, 1.0, random.nextDouble()), lowerLeft, upperRight))
                    .isEqualTo(TRUE);
            assertThat(withinBBox(pointValue(CARTESIAN, random.nextDouble(), 0.0), lowerLeft, upperRight))
                    .isEqualTo(TRUE);
            assertThat(withinBBox(pointValue(CARTESIAN, random.nextDouble(), 1.0), lowerLeft, upperRight))
                    .isEqualTo(TRUE);
        }
    }

    @Test
    void testPointsOutsideBBox() {
        var lowerLeft = pointValue(CARTESIAN, 2.0, 2.0);
        var upperRight = pointValue(CARTESIAN, 3.0, 3.0);

        for (int i = 0; i < ITERATIONS; i++) {
            assertThat(withinBBox(
                            pointValue(CARTESIAN, random.nextDouble(), random.nextDouble()), lowerLeft, upperRight))
                    .isEqualTo(FALSE);
        }
    }

    @Test
    void testNullInNullOut() {
        var lowerLeft = pointValue(CARTESIAN, 0.0, 0.0);
        var upperRight = pointValue(CARTESIAN, 1.0, 1.0);

        assertThat(withinBBox(NO_VALUE, lowerLeft, upperRight)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(pointValue(CARTESIAN, random.nextDouble(), random.nextDouble()), NO_VALUE, upperRight))
                .isEqualTo(NO_VALUE);
        assertThat(withinBBox(pointValue(CARTESIAN, random.nextDouble(), random.nextDouble()), lowerLeft, NO_VALUE))
                .isEqualTo(NO_VALUE);
    }

    @Test
    void testInvalidTypes() {
        var lowerLeft = pointValue(CARTESIAN, 0.0, 0.0);
        var upperRight = pointValue(CARTESIAN, 1.0, 1.0);

        assertThat(withinBBox(longValue(15), lowerLeft, upperRight)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(
                        pointValue(CARTESIAN, random.nextDouble(), random.nextDouble()),
                        stringValue("I'm a point"),
                        upperRight))
                .isEqualTo(NO_VALUE);
        assertThat(withinBBox(pointValue(CARTESIAN, random.nextDouble(), random.nextDouble()), lowerLeft, TRUE))
                .isEqualTo(NO_VALUE);
    }

    @Test
    void testDifferentCRS() {
        var a = pointValue(CARTESIAN, 0.0, 0.0);
        var b = pointValue(WGS_84, 1.0, 1.0);
        var c = pointValue(CARTESIAN_3D, 1.0, 1.0, 1.0);

        assertThat(withinBBox(a, a, a)).isEqualTo(TRUE);
        assertThat(withinBBox(a, a, b)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(a, a, c)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(a, b, a)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(a, b, b)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(a, b, c)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(a, c, a)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(a, c, b)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(a, c, c)).isEqualTo(NO_VALUE);

        assertThat(withinBBox(b, a, a)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(b, a, b)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(b, a, c)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(b, b, a)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(b, b, b)).isEqualTo(TRUE);
        assertThat(withinBBox(b, b, c)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(b, c, a)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(b, c, b)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(b, c, c)).isEqualTo(NO_VALUE);

        assertThat(withinBBox(c, a, a)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(c, a, b)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(c, a, c)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(c, b, a)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(c, b, b)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(c, b, c)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(c, c, a)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(c, c, b)).isEqualTo(NO_VALUE);
        assertThat(withinBBox(c, c, c)).isEqualTo(TRUE);
    }

    @Test
    void handleCrossing0thMeridian() {
        var lowerLeft = pointValue(WGS_84, -1, 60);
        var upperRight = pointValue(WGS_84, 1, 66);

        assertThat(withinBBox(pointValue(WGS_84, -1.5, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, -1, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 0.5, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 0, 63), lowerLeft, upperRight)).isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 0.5, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 1, 63), lowerLeft, upperRight)).isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 1.5, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleCrossing180thMeridian() {
        var lowerLeft = pointValue(WGS_84, 179, 60);
        var upperRight = pointValue(WGS_84, -179, 66);

        assertThat(withinBBox(pointValue(WGS_84, 178.5, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 179, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 179.5, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 180, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -179.5, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -179, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -178.5, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleCrossingTheEquator() {
        var lowerLeft = pointValue(WGS_84, 10, -1);
        var upperRight = pointValue(WGS_84, 20, 1);

        assertThat(withinBBox(pointValue(WGS_84, 15, -1.5), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 15, -1), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 15, -0.5), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 15, 0), lowerLeft, upperRight)).isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 15, 0.5), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 15, 1.0), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 15, 1.5), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleLowerLeftWestOfUpperRightWesternHemisphere() {
        var lowerLeft = pointValue(WGS_84, 0, 60);
        var upperRight = pointValue(WGS_84, 10, 66);

        assertThat(withinBBox(pointValue(WGS_84, 11, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 10, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 9, 63), lowerLeft, upperRight)).isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 5, 63), lowerLeft, upperRight)).isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 1, 63), lowerLeft, upperRight)).isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 0, 63), lowerLeft, upperRight)).isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -1, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleLowerLeftEastOfUpperRightWesternHemisphere() {
        var lowerLeft = pointValue(WGS_84, 10, 60);
        var upperRight = pointValue(WGS_84, 0, 66);

        assertThat(withinBBox(pointValue(WGS_84, 11, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 10, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 9, 63), lowerLeft, upperRight)).isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 5, 63), lowerLeft, upperRight)).isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 1, 63), lowerLeft, upperRight)).isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 0, 63), lowerLeft, upperRight)).isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -1, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
    }

    @Test
    void handleLowerWestOfUpperRightEasternHemisphere() {
        var lowerLeft = pointValue(WGS_84, -160, 60);
        var upperRight = pointValue(WGS_84, -150, 66);

        assertThat(withinBBox(pointValue(WGS_84, -140, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, -150, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -151, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -155, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -159, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -160, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -170, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleLowerLeftEastOfUpperRightEasternHemisphere() {
        var lowerLeft = pointValue(WGS_84, -150, 60);
        var upperRight = pointValue(WGS_84, -160, 66);

        assertThat(withinBBox(pointValue(WGS_84, -140, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -150, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -151, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, -155, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, -159, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, -160, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -170, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
    }

    @Test
    void handleLowerWestOfUpperRightCrossingHemispheres() {
        var lowerLeft = pointValue(WGS_84, 175, 60);
        var upperRight = pointValue(WGS_84, -175, 66);

        assertThat(withinBBox(pointValue(WGS_84, 170, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 175, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 180, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -175, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -170, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleLowerLeftEastOfUpperRightCrossingHemispheres() {
        var lowerLeft = pointValue(WGS_84, -175, 60);
        var upperRight = pointValue(WGS_84, 175, 66);

        assertThat(withinBBox(pointValue(WGS_84, 170, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 175, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 180, 63), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, -175, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, -170, 63), lowerLeft, upperRight))
                .isEqualTo(TRUE);
    }

    @Test
    void handleLowerLeftSouthOfUpperRightNorthernHemisphere() {
        var lowerLeft = pointValue(WGS_84, 30, 60);
        var upperRight = pointValue(WGS_84, 40, 70);

        assertThat(withinBBox(pointValue(WGS_84, 35, 55), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 60), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 65), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 70), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 75), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleLowerLeftNorthOfUpperNorthernHemisphere() {
        var lowerLeft = pointValue(WGS_84, 30, 70);
        var upperRight = pointValue(WGS_84, 40, 60);

        assertThat(withinBBox(pointValue(WGS_84, 35, 55), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 60), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 65), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 70), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 75), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleLowerLeftSouthOfUpperRightSouthernHemisphere() {
        var lowerLeft = pointValue(WGS_84, 30, -70);
        var upperRight = pointValue(WGS_84, 40, -60);

        assertThat(withinBBox(pointValue(WGS_84, 35, -55), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -60), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -65), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -70), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -75), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleLowerLeftNorthOfUpperRightSouthernHemisphere() {
        var lowerLeft = pointValue(WGS_84, 30, -60);
        var upperRight = pointValue(WGS_84, 40, -70);

        assertThat(withinBBox(pointValue(WGS_84, 35, 55), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -55), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -60), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -65), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -70), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -75), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleLowerLeftSouthOfUpperRightCrossingEquator() {
        var lowerLeft = pointValue(WGS_84, 30, -10);
        var upperRight = pointValue(WGS_84, 40, 10);

        assertThat(withinBBox(pointValue(WGS_84, 35, -15), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -10), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 0), lowerLeft, upperRight)).isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 10), lowerLeft, upperRight))
                .isEqualTo(TRUE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 15), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleLowerLeftNorthOfUpperRightCrossingEquator() {
        var lowerLeft = pointValue(WGS_84, 30, 10);
        var upperRight = pointValue(WGS_84, 40, -10);

        assertThat(withinBBox(pointValue(WGS_84, 35, -15), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, -10), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 0), lowerLeft, upperRight)).isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 10), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 15), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }

    @Test
    void handleBothDirectionsShifted() {
        var lowerLeft = pointValue(WGS_84, 40, 40);
        var upperRight = pointValue(WGS_84, 30, 30);

        assertThat(withinBBox(pointValue(WGS_84, 45, 30), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 40, 30), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 35, 30), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 30, 30), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 25, 30), lowerLeft, upperRight))
                .isEqualTo(FALSE);

        assertThat(withinBBox(pointValue(WGS_84, 30, 45), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 30, 40), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 30, 35), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 30, 30), lowerLeft, upperRight))
                .isEqualTo(FALSE);
        assertThat(withinBBox(pointValue(WGS_84, 30, 25), lowerLeft, upperRight))
                .isEqualTo(FALSE);
    }
}
