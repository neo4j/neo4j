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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84_3D;
import static org.neo4j.values.storable.Values.pointValue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.values.Comparison;

class PointValueTest {
    long seed;
    Random random;

    @BeforeEach
    final void setup() {
        seed = System.nanoTime();
        random = new Random(seed);
    }

    @ParameterizedTest
    @EnumSource
    final void constructionShouldThrowNonFinite(CoordinateReferenceSystem crs) {
        final var nonFinite = new double[] {
            Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY,
            Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY
        };

        final var dimension = crs.getDimension();
        final var coordinates =
                IntStream.range(0, dimension).mapToDouble(i -> i).toArray();
        for (final var n : nonFinite) {
            for (int d = 0; d < dimension; d++) {
                final var coords = Arrays.copyOf(coordinates, coordinates.length);
                coords[d] = n;
                assertNonFiniteCoordinateThrows(crs, coords);
            }
        }
    }

    @ParameterizedTest
    @EnumSource
    final void pointValueBetweenMinMax(CoordinateReferenceSystem crs) {
        // the intent here is to rely on the test becoming flakey (albeit rare) if the min/max is ever insufficient
        final var iterations = 1_000_000;
        final var min = PointValue.minPointValueOf(crs);
        final var max = PointValue.maxPointValueOf(crs);

        try {
            Stream.concat(
                            extremePointValues(crs),
                            Stream.generate(() -> randomPointValue(crs)).limit(iterations))
                    .forEach(point -> assertThat(point)
                            .as("compare to minimum PointValue")
                            .isGreaterThanOrEqualTo(PointValue.MIN_VALUE)
                            .as("compare to minimum %s value", crs)
                            .isGreaterThanOrEqualTo(min)
                            .as("compare to maximum %s value", crs)
                            .isLessThanOrEqualTo(max)
                            .as("compare to maximum PointValue")
                            .isLessThanOrEqualTo(PointValue.MAX_VALUE));
        } catch (AssertionError e) {
            throw new AssertionError(String.format("%s [ random seed used: %dL ]", e.getMessage(), seed), e);
        }
    }

    @Test
    final void cartesianShouldEqualItself() {
        assertThat(pointValue(CARTESIAN, 1.0, 2.0)).isEqualTo(pointValue(CARTESIAN, 1.0, 2.0));
        assertThat(pointValue(CARTESIAN, -1.0, 2.0)).isEqualTo(pointValue(CARTESIAN, -1.0, 2.0));
        assertThat(pointValue(CARTESIAN, -1.0, -2.0)).isEqualTo(pointValue(CARTESIAN, -1.0, -2.0));
        assertThat(pointValue(CARTESIAN, 0.0, 0.0)).isEqualTo(pointValue(CARTESIAN, 0.0, 0.0));
    }

    @Test
    final void cartesianShouldNotEqualOtherPoint() {
        assertThat(pointValue(CARTESIAN, 1.0, 2.0)).isNotEqualTo(pointValue(CARTESIAN, 3.0, 4.0));
        assertThat(pointValue(CARTESIAN, 1.0, 2.0)).isNotEqualTo(pointValue(CARTESIAN, -1.0, 2.0));
    }

    @Test
    final void geographicShouldEqualItself() {
        assertThat(pointValue(WGS_84, 1.0, 2.0)).isEqualTo(pointValue(WGS_84, 1.0, 2.0));
        assertThat(pointValue(WGS_84, -1.0, 2.0)).isEqualTo(pointValue(WGS_84, -1.0, 2.0));
        assertThat(pointValue(WGS_84, -1.0, -2.0)).isEqualTo(pointValue(WGS_84, -1.0, -2.0));
        assertThat(pointValue(WGS_84, 0.0, 0.0)).isEqualTo(pointValue(WGS_84, 0.0, 0.0));
    }

    @Test
    final void geographicShouldNotEqualOtherPoint() {
        assertThat(pointValue(WGS_84, 1.0, 2.0)).isNotEqualTo(pointValue(WGS_84, 3.0, 4.0));
        assertThat(pointValue(WGS_84, 1.0, 2.0)).isNotEqualTo(pointValue(WGS_84, -1.0, 2.0));
    }

    @Test
    final void geographicShouldNotEqualCartesian() {
        assertThat(pointValue(WGS_84, 1.0, 2.0)).isNotEqualTo(pointValue(CARTESIAN, 1.0, 2.0));
    }

    @Test
    final void geometricInvalid2DPointsShouldBehave() {
        // we wrap around for x [-180,180]
        // we fail on going over or under [-90,90] for y

        // valid ones for x
        assertThat(pointValue(WGS_84, 0, 0).coordinate()).containsExactly(0, 0);
        assertThat(pointValue(WGS_84, 180, 0).coordinate()).containsExactly(180, 0);
        assertThat(pointValue(WGS_84, -180, 0).coordinate()).containsExactly(-180, 0);

        // valid ones for x that should wrap around
        assertThat(pointValue(WGS_84, 190, 0).coordinate()).containsExactly(-170, 0);
        assertThat(pointValue(WGS_84, -190, 0).coordinate()).containsExactly(170, 0);
        assertThat(pointValue(WGS_84, 360, 0).coordinate()).containsExactly(0, 0);
        assertThat(pointValue(WGS_84, -360, 0).coordinate()).containsExactly(0, 0);
        assertThat(pointValue(WGS_84, 350, 0).coordinate()).containsExactly(-10, 0);
        assertThat(pointValue(WGS_84, -350, 0).coordinate()).containsExactly(10, 0);
        assertThat(pointValue(WGS_84, 370, 0).coordinate()).containsExactly(10, 0);
        assertThat(pointValue(WGS_84, -370, 0).coordinate()).containsExactly(-10, 0);
        assertThat(pointValue(WGS_84, 540, 0).coordinate()).containsExactly(180, 0);
        assertThat(pointValue(WGS_84, -540, 0).coordinate()).containsExactly(-180, 0);

        // valid ones for y
        assertThat(pointValue(WGS_84, 0, 90).coordinate()).containsExactly(0, 90);
        assertThat(pointValue(WGS_84, 0, -90).coordinate()).containsExactly(0, -90);

        // invalid ones for y

        assertThatThrownBy(() -> pointValue(WGS_84, 0, 91))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage(
                        "Cannot create WGS84 point with invalid coordinate: [0.0, 91.0]. Valid range for Y coordinate is [-90, 90].");

        assertThatThrownBy(() -> pointValue(WGS_84, 0, -91))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage(
                        "Cannot create WGS84 point with invalid coordinate: [0.0, -91.0]. Valid range for Y coordinate is [-90, 90].");
    }

    @Test
    final void geometricInvalid3DPointsShouldBehave() {
        // we wrap around for x [-180,180]
        // we fail on going over or under [-90,90] for y
        // we accept all values for z

        // valid ones for x
        assertThat(pointValue(WGS_84_3D, 0, 0, 0).coordinate()).containsExactly(0, 0, 0);
        assertThat(pointValue(WGS_84_3D, 180, 0, 0).coordinate()).containsExactly(180, 0, 0);
        assertThat(pointValue(WGS_84_3D, -180, 0, 0).coordinate()).containsExactly(-180, 0, 0);

        // valid ones for x that should wrap around
        assertThat(pointValue(WGS_84_3D, 190, 0, 0).coordinate()).containsExactly(-170, 0, 0);
        assertThat(pointValue(WGS_84_3D, -190, 0, 0).coordinate()).containsExactly(170, 0, 0);
        assertThat(pointValue(WGS_84_3D, 360, 0, 0).coordinate()).containsExactly(0, 0, 0);
        assertThat(pointValue(WGS_84_3D, -360, 0, 0).coordinate()).containsExactly(0, 0, 0);
        assertThat(pointValue(WGS_84_3D, 350, 0, 0).coordinate()).containsExactly(-10, 0, 0);
        assertThat(pointValue(WGS_84_3D, -350, 0, 0).coordinate()).containsExactly(10, 0, 0);
        assertThat(pointValue(WGS_84_3D, 370, 0, 0).coordinate()).containsExactly(10, 0, 0);
        assertThat(pointValue(WGS_84_3D, -370, 0, 0).coordinate()).containsExactly(-10, 0, 0);
        assertThat(pointValue(WGS_84_3D, 540, 0, 0).coordinate()).containsExactly(180, 0, 0);
        assertThat(pointValue(WGS_84_3D, -540, 0, 0).coordinate()).containsExactly(-180, 0, 0);

        // valid ones for y
        assertThat(pointValue(WGS_84_3D, 0, 90, 0).coordinate()).containsExactly(0, 90, 0);
        assertThat(pointValue(WGS_84_3D, 0, -90, 0).coordinate()).containsExactly(0, -90, 0);

        // invalid ones for y
        assertThatThrownBy(() -> pointValue(WGS_84_3D, 0, 91, 0))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage(
                        "Cannot create WGS84 point with invalid coordinate: [0.0, 91.0, 0.0]. Valid range for Y coordinate is [-90, 90].");

        assertThatThrownBy(() -> pointValue(WGS_84_3D, 0, -91, 0))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage(
                        "Cannot create WGS84 point with invalid coordinate: [0.0, -91.0, 0.0]. Valid range for Y coordinate is [-90, 90].");
    }

    @Test
    final void shouldHaveValueGroup() {
        assertThat(pointValue(CARTESIAN, 1, 2).valueGroup()).isNotNull();
        assertThat(pointValue(WGS_84, 1, 2).valueGroup()).isNotNull();
    }

    // -------------------------------------------------------------
    // Comparison tests

    @Test
    public final void shouldCompareTwoPoints() {
        assertThat(pointValue(CARTESIAN, 1, 2))
                .as("Two identical points should be equal")
                .isEqualByComparingTo(pointValue(CARTESIAN, 1, 2));
        assertThat(pointValue(CARTESIAN, 1, 2))
                .as("Different CRS should compare CRS codes")
                .isGreaterThan(pointValue(WGS_84, 1, 2));
        assertThat(pointValue(CARTESIAN, 2, 3))
                .as("Point greater on both dimensions is greater")
                .isGreaterThan(pointValue(CARTESIAN, 1, 2));
        assertThat(pointValue(CARTESIAN, 2, 2))
                .as("Point greater on first dimensions is greater")
                .isGreaterThan(pointValue(CARTESIAN, 1, 2));
        assertThat(pointValue(CARTESIAN, 1, 3))
                .as("Point greater on second dimensions is greater")
                .isGreaterThan(pointValue(CARTESIAN, 1, 2));
        assertThat(pointValue(CARTESIAN, 0, 1))
                .as("Point smaller on both dimensions is smaller")
                .isLessThan(pointValue(CARTESIAN, 1, 2));
        assertThat(pointValue(CARTESIAN, 0, 2))
                .as("Point smaller on first dimensions is smaller")
                .isLessThan(pointValue(CARTESIAN, 1, 2));
        assertThat(pointValue(CARTESIAN, 1, 1))
                .as("Point smaller on second dimensions is smaller")
                .isLessThan(pointValue(CARTESIAN, 1, 2));
        assertThat(pointValue(CARTESIAN, 2, 1))
                .as("Point greater on first and smaller on second dimensions is greater")
                .isGreaterThan(pointValue(CARTESIAN, 1, 2));
        assertThat(pointValue(CARTESIAN, 0, 3))
                .as("Point smaller on first and greater on second dimensions is smaller")
                .isLessThan(pointValue(CARTESIAN, 1, 2));
    }

    @Test
    public final void shouldTernaryCompareTwoPoints() {
        assertThat(pointValue(CARTESIAN, 1, 2).unsafeTernaryCompareTo(pointValue(CARTESIAN, 1, 2)))
                .as("Two identical points should be equal")
                .isEqualTo(Comparison.EQUAL);
        assertThat(pointValue(CARTESIAN, 1, 2).unsafeTernaryCompareTo(pointValue(WGS_84, 1, 2)))
                .as("Different CRS should be incomparable")
                .isEqualTo(Comparison.UNDEFINED);
        assertThat(pointValue(CARTESIAN, 2, 3).unsafeTernaryCompareTo(pointValue(CARTESIAN, 1, 2)))
                .as("Point greater on both dimensions is UNDEFINED")
                .isEqualTo(Comparison.UNDEFINED);
        assertThat(pointValue(CARTESIAN, 2, 2).unsafeTernaryCompareTo(pointValue(CARTESIAN, 1, 2)))
                .as("Point greater on first dimensions is UNDEFINED")
                .isEqualTo(Comparison.UNDEFINED);
        assertThat(pointValue(CARTESIAN, 1, 3).unsafeTernaryCompareTo(pointValue(CARTESIAN, 1, 2)))
                .as("Point greater on second dimensions is UNDEFINED")
                .isEqualTo(Comparison.UNDEFINED);
        assertThat(pointValue(CARTESIAN, 0, 1).unsafeTernaryCompareTo(pointValue(CARTESIAN, 1, 2)))
                .as("Point smaller on both dimensions is UNDEFINED")
                .isEqualTo(Comparison.UNDEFINED);
        assertThat(pointValue(CARTESIAN, 0, 2).unsafeTernaryCompareTo(pointValue(CARTESIAN, 1, 2)))
                .as("Point smaller on first dimensions is UNDEFINED")
                .isEqualTo(Comparison.UNDEFINED);
        assertThat(pointValue(CARTESIAN, 1, 1).unsafeTernaryCompareTo(pointValue(CARTESIAN, 1, 2)))
                .as("Point smaller on second dimensions is UNDEFINED")
                .isEqualTo(Comparison.UNDEFINED);
        assertThat(pointValue(CARTESIAN, 2, 1).unsafeTernaryCompareTo(pointValue(CARTESIAN, 1, 2)))
                .as("Point greater on first and smaller on second dimensions is UNDEFINED")
                .isEqualTo(Comparison.UNDEFINED);
        assertThat(pointValue(CARTESIAN, 0, 3).unsafeTernaryCompareTo(pointValue(CARTESIAN, 1, 2)))
                .as("Point smaller on first and greater on second dimensions is UNDEFINED")
                .isEqualTo(Comparison.UNDEFINED);
    }

    // -------------------------------------------------------------
    // Parser tests

    @Test
    final void shouldBeAbleToParsePoints() {
        assertThat(PointValue.parse("{latitude: 56.7, longitude: 13.2}"))
                .as("default %s", WGS_84)
                .isEqualTo(pointValue(WGS_84, 13.2, 56.7));

        assertThat(PointValue.parse("{latitude: 40.7128, longitude: -74.0060, crs: 'wgs-84'}"))
                .as("explicitly %s", WGS_84)
                .isEqualTo(pointValue(WGS_84, -74.0060, 40.7128));

        assertThat(PointValue.parse("{x: -21, y: -45.3}"))
                .as("default %s", CARTESIAN)
                .isEqualTo(pointValue(CARTESIAN, -21, -45.3));

        assertThat(PointValue.parse("{x: -21, y: -45.3, srid: 4326}"))
                .as("explicitly %s, via SRID", WGS_84)
                .isEqualTo(pointValue(WGS_84, -21, -45.3));

        assertThat(PointValue.parse("{x: 17, y: -52.8, crs: 'cartesian'}"))
                .as("explicitly %s", CARTESIAN)
                .isEqualTo(pointValue(CARTESIAN, 17, -52.8));

        assertThat(PointValue.parse("{latitude: 56.7, longitude: 13.2, height: 123.4}"))
                .as("default %s", WGS_84_3D)
                .isEqualTo(pointValue(WGS_84_3D, 13.2, 56.7, 123.4));

        assertThat(PointValue.parse("{latitude: 56.7, longitude: 13.2, z: 123.4}"))
                .as("default %s", WGS_84_3D)
                .isEqualTo(pointValue(WGS_84_3D, 13.2, 56.7, 123.4));

        assertThat(PointValue.parse("{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs: 'wgs-84-3D'}"))
                .as("explicitly %s", WGS_84_3D)
                .isEqualTo(pointValue(WGS_84_3D, -74.0060, 40.7128, 567.8));

        assertThat(PointValue.parse("{x: -21, y: -45.3, z: 7.2}"))
                .as("default %s", CARTESIAN_3D)
                .isEqualTo(pointValue(CARTESIAN_3D, -21, -45.3, 7.2));

        assertThat(PointValue.parse("{x: 17, y: -52.8, z: -83.1, crs: 'cartesian-3D'}"))
                .as("explicitly %s", CARTESIAN_3D)
                .isEqualTo(pointValue(CARTESIAN_3D, 17, -52.8, -83.1));
    }

    @Test
    final void shouldBeAbleToParsePointWithUnquotedCrs() {
        assertThat(PointValue.parse("{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs:wgs-84-3D}"))
                .as("explicitly %s, without quotes", WGS_84_3D)
                .isEqualTo(pointValue(WGS_84_3D, -74.0060, 40.7128, 567.8));
    }

    @Test
    final void shouldBeAbleToParsePointThatOverridesHeaderInformation() {
        final var headerInformation = "{crs:wgs-84}";
        final var data = "{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs:wgs-84-3D}";

        final var expected = PointValue.parse(data);
        final var actual = PointValue.parse(data, PointValue.parseHeaderInformation(headerInformation));

        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getCoordinateReferenceSystem()).isEqualTo(WGS_84_3D);
    }

    @Test
    final void shouldBeAbleToParseIncompletePointWithHeaderInformation() {
        final var headerInformation = "{latitude: 40.7128}";
        final var data = "{longitude: -74.0060, height: 567.8, crs:wgs-84-3D}";

        assertCannotParse(data);

        // this should work
        PointValue.parse(data, PointValue.parseHeaderInformation(headerInformation));
    }

    @Test
    final void shouldBeAbleToParseWeirdlyFormattedPoints() {
        // TODO: Should some/all of these fail?
        assertThat(PointValue.parse(" \t\n { latitude : 2.0  ,longitude :1.0  } \t"))
                .isEqualTo(pointValue(WGS_84, 1.0, 2.0));
        assertThat(PointValue.parse(" \t\n { latitude : 2.0  ,longitude :1.0 , } \t"))
                .isEqualTo(pointValue(WGS_84, 1.0, 2.0));
        assertThat(PointValue.parse(" \t\n { x :+.2e-7,y: -1.0E07 , } \t"))
                .isEqualTo(pointValue(CARTESIAN, 2.0E-8, -1.0E7));
        assertThat(PointValue.parse(" \t\n { x :+.2e-7,y: -1.0E07 , garbage} \t"))
                .isEqualTo(pointValue(CARTESIAN, 2.0E-8, -1.0E7));
    }

    @Test
    final void shouldNotBeAbleToParsePointsWithConflictingDuplicateFields() {
        assertCannotParse("{latitude: 2.0, longitude: 1.0, latitude: 3.0}").hasMessageContaining("Duplicate field");
        assertCannotParse("{latitude: 2.0, longitude: 1.0, latitude: 3.0}").hasMessageContaining("Duplicate field");
        assertCannotParse("{crs: 'cartesian', x: 2.0, x: 1.0, y: 3}").hasMessageContaining("Duplicate field");
        assertCannotParse("{crs: 'invalid crs', x: 1.0, y: 3, crs: 'cartesian'}")
                .hasMessageContaining("Duplicate field");
    }

    @Test
    final void shouldNotBeAbleToParseIncompletePoints() {
        assertCannotParse("{latitude: 56.7, longitude:}");
        assertCannotParse("{latitude: 56.7}");
        assertCannotParse("{}");
        assertCannotParse("{only_a_key}");
        assertCannotParse("{crs:'WGS-84'}");
        assertCannotParse("{a:a}");
        assertCannotParse("{ : 2.0, x : 1.0 }");
        assertCannotParse("x:1,y:2");
        assertCannotParse("{x:1,y:2,srid:-9}");
        assertCannotParse("{crs:WGS-84 , lat:1, y:2}");
    }

    final double randomFiniteDouble() {
        final var bytes = new byte[Double.BYTES];
        double value;
        do {
            random.nextBytes(bytes);
            value = ByteBuffer.wrap(bytes).getDouble();
        } while (!Double.isFinite(value));
        return value;
    }

    final double randomLongitude() {
        return randomBoundedDouble(-180, 180);
    }

    final double randomLatitude() {
        return randomBoundedDouble(-90, 90);
    }

    final double randomBoundedDouble(double min, double max) {
        final var value = (double) random.nextLong() - Long.MIN_VALUE;
        final var scale = (max - min) / ((double) Long.MAX_VALUE - Long.MIN_VALUE);
        return scale * value + min;
    }

    final PointValue randomPointValue(CoordinateReferenceSystem crs) {
        return switch (crs) {
            case CARTESIAN -> pointValue(crs, randomFiniteDouble(), randomFiniteDouble());
            case CARTESIAN_3D -> pointValue(crs, randomFiniteDouble(), randomFiniteDouble(), randomFiniteDouble());
            case WGS_84 -> pointValue(crs, randomLongitude(), randomLatitude());
            case WGS_84_3D -> pointValue(crs, randomLongitude(), randomLatitude(), randomFiniteDouble());
        };
    }

    private static Stream<PointValue> extremePointValues(CoordinateReferenceSystem crs) {
        return switch (crs) {
            case CARTESIAN -> Stream.of(
                    pointValue(crs, -Double.MAX_VALUE, -Double.MAX_VALUE),
                    pointValue(crs, -Double.MAX_VALUE, Double.MAX_VALUE),
                    pointValue(crs, Double.MAX_VALUE, -Double.MAX_VALUE),
                    pointValue(crs, Double.MAX_VALUE, Double.MAX_VALUE));

            case CARTESIAN_3D -> Stream.of(
                    pointValue(crs, -Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE),
                    pointValue(crs, -Double.MAX_VALUE, -Double.MAX_VALUE, Double.MAX_VALUE),
                    pointValue(crs, -Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE),
                    pointValue(crs, -Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE),
                    pointValue(crs, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE),
                    pointValue(crs, Double.MAX_VALUE, -Double.MAX_VALUE, Double.MAX_VALUE),
                    pointValue(crs, Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE),
                    pointValue(crs, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE));

            case WGS_84 -> Stream.of(
                    pointValue(crs, -180, -90),
                    pointValue(crs, -180, 90),
                    pointValue(crs, 180, -90),
                    pointValue(crs, 180, 90));

            case WGS_84_3D -> Stream.of(
                    pointValue(crs, -180, -90, -Double.MAX_VALUE),
                    pointValue(crs, -180, -90, Double.MAX_VALUE),
                    pointValue(crs, -180, 90, -Double.MAX_VALUE),
                    pointValue(crs, -180, 90, Double.MAX_VALUE),
                    pointValue(crs, 180, -90, -Double.MAX_VALUE),
                    pointValue(crs, 180, -90, Double.MAX_VALUE),
                    pointValue(crs, 180, 90, -Double.MAX_VALUE),
                    pointValue(crs, 180, 90, Double.MAX_VALUE));
        };
    }

    private static AbstractThrowableAssert<?, ? extends Throwable> assertNonFiniteCoordinateThrows(
            CoordinateReferenceSystem crs, double... coordinates) {
        return assertThatThrownBy(() -> pointValue(crs, coordinates))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContaining("Cannot create a point with non-finite coordinate values");
    }

    private static AbstractThrowableAssert<?, ? extends Throwable> assertCannotParse(String text) {
        return assertThatThrownBy(() -> PointValue.parse(text)).isInstanceOf(InvalidArgumentException.class);
    }
}
