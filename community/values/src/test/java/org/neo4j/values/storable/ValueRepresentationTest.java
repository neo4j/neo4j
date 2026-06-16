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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS_84;
import static org.neo4j.values.storable.ValueRepresentation.ANYTHING;
import static org.neo4j.values.storable.ValueRepresentation.FLOAT32;
import static org.neo4j.values.storable.ValueRepresentation.FLOAT64;
import static org.neo4j.values.storable.ValueRepresentation.GEOMETRY;
import static org.neo4j.values.storable.ValueRepresentation.INT16;
import static org.neo4j.values.storable.ValueRepresentation.INT32;
import static org.neo4j.values.storable.ValueRepresentation.INT64;
import static org.neo4j.values.storable.ValueRepresentation.INT8;
import static org.neo4j.values.storable.ValueRepresentation.LOCAL_TIME;
import static org.neo4j.values.storable.ValueRepresentation.UTF16_TEXT;
import static org.neo4j.values.storable.ValueRepresentation.UTF8_TEXT;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.list;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

class ValueRepresentationTest {

    @Test
    void shouldCoerceNumbers() {
        // 8 bit numbers
        assertEquals(INT8, INT8.coerce(INT8));
        assertEquals(INT16, INT8.coerce(INT16));
        assertEquals(INT32, INT8.coerce(INT32));
        assertEquals(INT64, INT8.coerce(INT64));
        assertEquals(FLOAT32, INT8.coerce(FLOAT32));
        assertEquals(FLOAT64, INT8.coerce(FLOAT64));
        assertEquals(ValueRepresentation.UNKNOWN, INT8.coerce(LOCAL_TIME));

        // 16 bit integers
        assertEquals(INT16, INT16.coerce(INT8));
        assertEquals(INT16, INT16.coerce(INT16));
        assertEquals(INT32, INT16.coerce(INT32));
        assertEquals(INT64, INT16.coerce(INT64));
        assertEquals(FLOAT32, INT16.coerce(FLOAT32));
        assertEquals(FLOAT64, INT16.coerce(FLOAT64));
        assertEquals(ValueRepresentation.UNKNOWN, INT16.coerce(LOCAL_TIME));

        // 32 bit integers
        assertEquals(INT32, INT32.coerce(INT8));
        assertEquals(INT32, INT32.coerce(INT16));
        assertEquals(INT32, INT32.coerce(INT32));
        assertEquals(INT64, INT32.coerce(INT64));
        assertEquals(FLOAT64, INT32.coerce(FLOAT32));
        assertEquals(FLOAT64, INT32.coerce(FLOAT64));
        assertEquals(ValueRepresentation.UNKNOWN, INT32.coerce(LOCAL_TIME));

        // 64 bit integers
        assertEquals(INT64, INT64.coerce(INT8));
        assertEquals(INT64, INT64.coerce(INT16));
        assertEquals(INT64, INT64.coerce(INT32));
        assertEquals(INT64, INT64.coerce(INT64));
        assertEquals(FLOAT64, INT64.coerce(FLOAT32));
        assertEquals(FLOAT64, INT64.coerce(FLOAT64));
        assertEquals(ValueRepresentation.UNKNOWN, INT64.coerce(LOCAL_TIME));

        // 32 bit floats
        assertEquals(FLOAT32, FLOAT32.coerce(INT8));
        assertEquals(FLOAT32, FLOAT32.coerce(INT16));
        assertEquals(FLOAT64, FLOAT32.coerce(INT32));
        assertEquals(FLOAT64, FLOAT32.coerce(INT64));
        assertEquals(FLOAT32, FLOAT32.coerce(FLOAT32));
        assertEquals(FLOAT64, FLOAT32.coerce(FLOAT64));
        assertEquals(ValueRepresentation.UNKNOWN, FLOAT32.coerce(LOCAL_TIME));

        // 64 bit floats
        assertEquals(FLOAT64, FLOAT64.coerce(INT8));
        assertEquals(FLOAT64, FLOAT64.coerce(INT16));
        assertEquals(FLOAT64, FLOAT64.coerce(INT32));
        assertEquals(FLOAT64, FLOAT64.coerce(INT64));
        assertEquals(FLOAT64, FLOAT64.coerce(FLOAT32));
        assertEquals(FLOAT64, FLOAT64.coerce(FLOAT64));
        assertEquals(ValueRepresentation.UNKNOWN, FLOAT64.coerce(LOCAL_TIME));
    }

    @Test
    void shouldCoerceText() {
        assertEquals(UTF8_TEXT, UTF8_TEXT.coerce(UTF8_TEXT));
        assertEquals(UTF16_TEXT, UTF8_TEXT.coerce(UTF16_TEXT));
        assertEquals(UTF16_TEXT, UTF16_TEXT.coerce(UTF8_TEXT));
        assertEquals(UTF16_TEXT, UTF16_TEXT.coerce(UTF16_TEXT));
    }

    @Test
    void shouldCoerceWithAnything() {
        Arrays.stream(ValueRepresentation.values()).forEach(representation -> {
            assertEquals(representation, ANYTHING.coerce(representation));
            assertEquals(representation, representation.coerce(ANYTHING));
        });
    }

    @Test
    void shouldFailToCreateArrayOfPointsWithDifferentCRS() {
        // given
        ListValue points = list(pointValue(CARTESIAN, 1.0, 1.0), pointValue(WGS_84, 1.0, 1.0));

        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> GEOMETRY.arrayOf(points))
                .isInstanceOf(CypherTypeException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G03)
                .hasStatusDescription("error: data exception - invalid value type")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N39)
                .hasStatusDescription(
                        "error: data exception - unsupported property value type. Value point({x: 1.0, y: 1.0, crs: 'wgs-84'}) cannot be stored in properties.");
    }

    @Test
    void shouldFailToCreateArrayOfPointsWithDifferentDimension() {
        // given
        ListValue points = list(pointValue(CARTESIAN, 1.0, 1.0), pointValue(CARTESIAN_3D, 1.0, 1.0, 1.0));

        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> GEOMETRY.arrayOf(points))
                .isInstanceOf(CypherTypeException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G03)
                .hasStatusDescription("error: data exception - invalid value type")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N39)
                .hasStatusDescription(
                        "error: data exception - unsupported property value type. Value point({x: 1.0, y: 1.0, z: 1.0, crs: 'cartesian-3d'}) cannot be stored in properties.");
    }

    @Test
    void shouldFailToCreateArrayOfNoValue() {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> ANYTHING.arrayOf(list(Values.NO_VALUE)))
                .isInstanceOf(CypherTypeException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G03)
                .hasStatusDescription("error: data exception - invalid value type")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N39)
                .hasStatusDescription(
                        "error: data exception - unsupported property value type. Value [NULL] cannot be stored in properties.");
    }

    @Test
    void shouldFailToCreateArrayOfMixedTypes() {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> ANYTHING.arrayOf(list(intValue(42), stringValue("a"))))
                .isInstanceOf(CypherTypeException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G03)
                .hasStatusDescription("error: data exception - invalid value type")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N39)
                .hasStatusDescription(
                        "error: data exception - unsupported property value type. Value String(\"a\") cannot be stored in properties.");
    }

    @Test
    void shouldFailToCreateArrayOfList() {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> ANYTHING.arrayOf(list(list(intValue(42)))))
                .isInstanceOf(CypherTypeException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G03)
                .hasStatusDescription("error: data exception - invalid value type")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N39)
                .hasStatusDescription(
                        "error: data exception - unsupported property value type. Value [[42]] cannot be stored in properties.");
    }

    // Feel free to update or remove this test if it is no longer true for vectors.
    // Its aim is to exercise the !value.valueRepresentation().canCreateArrayOfValueGroup() case
    @ParameterizedTest
    @MethodSource
    void shouldFailToCreateArrayOfVector(ValueRepresentation vectorValueRepresentaiton) {
        assertThat(vectorValueRepresentaiton.canCreateArrayOfValueGroup())
                .as("remove when VectorArray is storable, IND-468")
                .isFalse();
    }

    private static Stream<ValueRepresentation> shouldFailToCreateArrayOfVector() {
        return Arrays.stream(ValueRepresentation.values())
                .filter(rep -> rep.valueGroup().category() == ValueCategory.VECTOR);
    }

    @Test
    void shouldFailToCreateArrayOfMap() {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(() -> ANYTHING.arrayOf(list(MapValue.EMPTY)))
                .isInstanceOf(CypherTypeException.class)
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22G03)
                .hasStatusDescription("error: data exception - invalid value type")
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N01)
                .hasStatusDescription(
                        "error: data exception - invalid type. Expected the value Map{} to be of type BOOLEAN, STRING, INTEGER, FLOAT, DATE, LOCAL TIME, ZONED TIME, LOCAL DATETIME, ZONED DATETIME, DURATION or POINT, but was of type MAP.");
    }
}
