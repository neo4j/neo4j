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

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.charArray;
import static org.neo4j.values.storable.Values.charValue;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatArray;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortArray;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.string.UTF8;

class ValuesTest {
    @Test
    void shouldBeEqualToItself() {
        assertEqual(booleanValue(false), booleanValue(false));
        assertEqual(byteValue((byte) 0), byteValue((byte) 0));
        assertEqual(shortValue((short) 0), shortValue((short) 0));
        assertEqual(intValue(0), intValue(0));
        assertEqual(longValue(0), longValue(0));
        assertEqual(floatValue(0.0f), floatValue(0.0f));
        assertEqual(doubleValue(0.0), doubleValue(0.0));
        assertEqual(stringValue(""), stringValue(""));

        assertEqual(booleanValue(true), booleanValue(true));
        assertEqual(byteValue((byte) 1), byteValue((byte) 1));
        assertEqual(shortValue((short) 1), shortValue((short) 1));
        assertEqual(intValue(1), intValue(1));
        assertEqual(longValue(1), longValue(1));
        assertEqual(floatValue(1.0f), floatValue(1.0f));
        assertEqual(doubleValue(1.0), doubleValue(1.0));
        assertEqual(charValue('x'), charValue('x'));
        assertEqual(stringValue("hi"), stringValue("hi"));

        assertEqual(booleanArray(new boolean[] {}), booleanArray(new boolean[] {}));
        assertEqual(byteArray(new byte[] {}), byteArray(new byte[] {}));
        assertEqual(shortArray(new short[] {}), shortArray(new short[] {}));
        assertEqual(intArray(new int[] {}), intArray(new int[] {}));
        assertEqual(longArray(new long[] {}), longArray(new long[] {}));
        assertEqual(floatArray(new float[] {}), floatArray(new float[] {}));
        assertEqual(doubleArray(new double[] {}), doubleArray(new double[] {}));
        assertEqual(charArray(new char[] {}), charArray(new char[] {}));
        assertEqual(stringArray(), stringArray());

        assertEqual(booleanArray(new boolean[] {true}), booleanArray(new boolean[] {true}));
        assertEqual(byteArray(new byte[] {1}), byteArray(new byte[] {1}));
        assertEqual(shortArray(new short[] {1}), shortArray(new short[] {1}));
        assertEqual(intArray(new int[] {1}), intArray(new int[] {1}));
        assertEqual(longArray(new long[] {1}), longArray(new long[] {1}));
        assertEqual(floatArray(new float[] {1.0f}), floatArray(new float[] {1.0f}));
        assertEqual(doubleArray(new double[] {1.0}), doubleArray(new double[] {1.0}));
        assertEqual(charArray(new char[] {'x'}), charArray(new char[] {'x'}));
        assertEqual(stringArray("hi"), stringArray("hi"));
    }

    @Test
    void pointValueShouldRequireConsistentInput() {
        assertThrows(
                InvalidArgumentException.class, () -> Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 2, 3));
        assertThrows(
                InvalidArgumentException.class, () -> Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 1, 2));
        assertThrows(
                InvalidArgumentException.class, () -> Values.pointValue(CoordinateReferenceSystem.WGS_84, 1, 2, 3));
        assertThrows(
                InvalidArgumentException.class, () -> Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 1, 2));
    }

    @Test
    void differentStringSubClassesShouldBeSeenAsOfSameValueType() {
        // given
        TextValue stringValue = stringValue("abc");
        TextValue utf8Value = utf8Value(UTF8.encode("def"));

        // when
        boolean areOfSameClasses = stringValue.getClass().equals(utf8Value);
        boolean areOfSameValueType = stringValue.isSameValueTypeAs(utf8Value);

        // then
        assertFalse(areOfSameClasses);
        assertTrue(areOfSameValueType);
    }

    @Test
    void differentTypesShouldNotBeEqual() {
        // This includes NaN
        List<Value> items = Arrays.asList(
                stringValue("foo"),
                intValue(42),
                booleanValue(false),
                doubleValue(Double.NaN),
                time(14, 0, 0, 0, UTC),
                localTime(14, 0, 0, 0),
                duration(0, 0, 0, 1_000_000_000),
                byteArray(new byte[] {}),
                charArray(new char[] {'x'}));
        for (int i = 0; i < items.size(); i++) {
            for (int j = 0; j < items.size(); j++) {
                if (i != j) {
                    assertNotEqual(items.get(i), items.get(j));
                }
            }
        }
    }

    @Test
    void shouldParseStringMap() {
        assertThat(Value.parseStringMap("{singleKey:10}")).isEqualTo(Map.of("singleKey", "10"));
        assertThat(Value.parseStringMap("{singleKey:some string}")).isEqualTo(Map.of("singleKey", "some string"));
        assertThat(Value.parseStringMap("{singleKey:'some string'}")).isEqualTo(Map.of("singleKey", "some string"));
        assertThat(Value.parseStringMap("{singleKey:\"some string\"}")).isEqualTo(Map.of("singleKey", "some string"));
        assertThat(Value.parseStringMap("{singleKey:value:with:colons}"))
                .isEqualTo(Map.of("singleKey", "value:with:colons"));
        assertThat(Value.parseStringMap("{key1:value:with:colons,key2:'another value'}"))
                .isEqualTo(Map.of("key1", "value:with:colons", "key2", "another value"));
    }
}
