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
package org.neo4j.values.virtual;

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.values.storable.NoValue.NO_VALUE;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.charArray;
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
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqualValues;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqualWithNoValues;
import static org.neo4j.values.utils.AnyValueTestUtil.assertIncomparable;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.list;
import static org.neo4j.values.virtual.VirtualValues.range;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.Values;

class ListTest {

    private ListValue[] equivalentLists = {
        VirtualValues.list(Values.longValue(1L), Values.longValue(4L), Values.longValue(7L)),
        range(1L, 8L, 3L),
        VirtualValues.fromArray(longArray(new long[] {1L, 4L, 7L})),
        list(-2L, 1L, 4L, 7L, 10L).slice(1, 4),
        list(-2L, 1L, 4L, 7L).drop(1),
        list(1L, 4L, 7L, 10L, 13L).take(3),
        list(7L, 4L, 1L).reverse(),
        VirtualValues.concat(list(1L, 4L), list(7L)),
        list(1L, 4L, 1L, 7L, 4L, 1L).distinct()
    };

    private ListValue[] nonEquivalentLists = {
        VirtualValues.list(Values.longValue(1L), Values.longValue(4L), Values.longValue(7L)),
        range(2L, 9L, 3L),
        VirtualValues.fromArray(longArray(new long[] {3L, 6L, 9L})),
        list(-2L, 1L, 5L, 8L, 11L).slice(1, 4),
        list(-2L, 6L, 9L, 12L).drop(1),
        list(7L, 10L, 13L, 10L, 13L).take(3),
        list(15L, 12L, 9L).reverse(),
        VirtualValues.concat(list(10L, 13L), list(16L))
    };

    @Test
    void shouldContainsOnList() {
        // Given
        // When
        ListValue list = VirtualValues.list(longValue(1L), longValue(2L));

        // Then
        assertEquals(list.ternaryContains(longValue(1L)), BooleanValue.TRUE);
        assertEquals(list.ternaryContains(longValue(3L)), BooleanValue.FALSE);
    }

    @Test
    void shouldContainsOnAppendListWithNulls() {
        // Given
        // When
        ListValue list = VirtualValues.list(longValue(1L), NO_VALUE, longValue(3L));

        // Then
        assertEquals(list.ternaryContains(longValue(1L)), BooleanValue.TRUE);
        assertEquals(list.ternaryContains(longValue(4L)), Values.NO_VALUE);
    }

    @Test
    void shouldBeEqualToItself() {
        assertEqual(list(new String[] {"hi"}, 3.0), list(new String[] {"hi"}, 3.0));

        assertEqual(list(), list());
    }

    @Test
    void shouldBeEqualToArrayIfValuesAreEqual() {
        // the empty list equals any array that is empty
        assertEqualValues(list(), booleanArray(new boolean[] {}));
        assertEqualValues(list(), byteArray(new byte[] {}));
        assertEqualValues(list(), charArray(new char[] {}));
        assertEqualValues(list(), doubleArray(new double[] {}));
        assertEqualValues(list(), floatArray(new float[] {}));
        assertEqualValues(list(), intArray(new int[] {}));
        assertEqualValues(list(), longArray(new long[] {}));
        assertEqualValues(list(), shortArray(new short[] {}));
        assertEqualValues(list(), stringArray());

        // actual values to test the equality
        assertEqualValues(list(true), booleanArray(new boolean[] {true}));
        assertEqualValues(list(true, false), booleanArray(new boolean[] {true, false}));
        assertEqualValues(list(84), byteArray("T".getBytes()));
        assertEqualValues(
                list(84, 104, 105, 115, 32, 105, 115, 32, 106, 117, 115, 116, 32, 97, 32, 116, 101, 115, 116),
                byteArray("This is just a test".getBytes()));
        assertEqualValues(list('h'), charArray(new char[] {'h'}));
        assertEqualValues(list('h', 'i'), charArray(new char[] {'h', 'i'}));
        assertEqualValues(list('h', 'i', '!'), charArray(new char[] {'h', 'i', '!'}));
        assertEqualValues(list(1.0), doubleArray(new double[] {1.0}));
        assertEqualValues(list(1.0, 2.0), doubleArray(new double[] {1.0, 2.0}));
        assertEqualValues(list(1.5f), floatArray(new float[] {1.5f}));
        assertEqualValues(list(1.5f, -5f), floatArray(new float[] {1.5f, -5f}));
        assertEqualValues(list(1), intArray(new int[] {1}));
        assertEqualValues(list(1, -3), intArray(new int[] {1, -3}));
        assertEqualValues(list(2L), longArray(new long[] {2L}));
        assertEqualValues(list(2L, -3L), longArray(new long[] {2L, -3L}));
        assertEqualValues(list((short) 2), shortArray(new short[] {(short) 2}));
        assertEqualValues(list((short) 2, (short) -3), shortArray(new short[] {(short) 2, (short) -3}));
        assertEqualValues(list("hi"), stringArray("hi"));
        assertEqualValues(list("hi", "ho"), stringArray("hi", "ho"));
        assertEqualValues(list("hi", "ho", "hu", "hm"), stringArray("hi", "ho", "hu", "hm"));
    }

    @Test
    void shouldNotEqual() {
        assertNotEqual(list(), list(2));
        assertNotEqual(list(), list(1, 2));
        assertNotEqual(list(1), list(2));
        assertNotEqual(list(1), list(1, 2));
        assertNotEqual(list(1, 1), list(1, 2));
        assertNotEqual(list(1, "d"), list(1, "f"));
        assertNotEqual(list(1, "d"), list("d", 1));
        assertNotEqual(list("d"), list(false));
        assertNotEqual(list(Values.stringArray("d")), list("d"));

        assertNotEqual(list(longArray(new long[] {3, 4, 5})), list(intArray(new int[] {3, 4, 50})));

        // different value types
        assertNotEqual(list(true, true), intArray(new int[] {0, 0}));
        assertNotEqual(list(true, true), longArray(new long[] {0L, 0L}));
        assertNotEqual(list(true, true), shortArray(new short[] {(short) 0, (short) 0}));
        assertNotEqual(list(true, true), floatArray(new float[] {0.0f, 0.0f}));
        assertNotEqual(list(true, true), doubleArray(new double[] {0.0, 0.0}));
        assertNotEqual(list(true, true), charArray(new char[] {'T', 'T'}));
        assertNotEqual(list(true, true), stringArray("True", "True"));
        assertNotEqual(list(true, true), byteArray(new byte[] {(byte) 0, (byte) 0}));

        // wrong or missing items
        assertNotEqual(list(true), booleanArray(new boolean[] {true, false}));
        assertNotEqual(list(true, true), booleanArray(new boolean[] {true, false}));
        assertNotEqual(
                list(84, 104, 32, 105, 115, 32, 106, 117, 115, 116, 32, 97, 32, 116, 101, 115, 116),
                byteArray("This is just a test".getBytes()));
        assertNotEqual(list('h'), charArray(new char[] {'h', 'i'}));
        assertNotEqual(list('h', 'o'), charArray(new char[] {'h', 'i'}));
        assertNotEqual(list(9.0, 2.0), doubleArray(new double[] {1.0, 2.0}));
        assertNotEqual(list(1.0), doubleArray(new double[] {1.0, 2.0}));
        assertNotEqual(list(1.5f), floatArray(new float[] {1.5f, -5f}));
        assertNotEqual(list(1.5f, 5f), floatArray(new float[] {1.5f, -5f}));
        assertNotEqual(list(1, 3), intArray(new int[] {1, -3}));
        assertNotEqual(list(-3), intArray(new int[] {1, -3}));
        assertNotEqual(list(2L, 3L), longArray(new long[] {2L, -3L}));
        assertNotEqual(list(2L), longArray(new long[] {2L, -3L}));
        assertNotEqual(list((short) 2, (short) 3), shortArray(new short[] {(short) 2, (short) -3}));
        assertNotEqual(list((short) 2), shortArray(new short[] {(short) 2, (short) -3}));
        assertNotEqual(list("hi", "hello"), stringArray("hi"));
        assertNotEqual(list("hi", "hello"), stringArray("hello", "hi"));
        assertNotEqual(list("hello"), stringArray("hi"));

        assertNotEqual(list(1, 'b'), charArray(new char[] {'a', 'b'}));
    }

    @Test
    void shouldHandleNullInList() {
        assertIncomparable(list(1, null), list(1, 2));
        assertEqualWithNoValues(list(NO_VALUE), list(NO_VALUE));
        assertNotEqual(list(1, null), list(2, 3));

        assertEqualWithNoValues(list(NO_VALUE), stringArray(new String[] {null}));
        assertEqualWithNoValues(list(null, null), stringArray(null, null));
        assertEqualWithNoValues(list("hi", null), stringArray("hi", null));
    }

    @Test
    void shouldCoerce() {
        assertEqual(list(new String[] {"h"}, 3.0), list(new char[] {'h'}, 3));

        assertEqualValues(list("a", 'b'), charArray(new char[] {'a', 'b'}));
    }

    @Test
    void shouldRecurse() {
        assertEqual(list('a', list('b', list('c'))), list('a', list('b', list('c'))));
    }

    @Test
    void shouldNestCorrectly() {
        assertEqual(
                list(
                        booleanArray(new boolean[] {true, false}),
                        intArray(new int[] {1, 2}),
                        stringArray("Hello", "World")),
                list(
                        booleanArray(new boolean[] {true, false}),
                        intArray(new int[] {1, 2}),
                        stringArray("Hello", "World")));

        assertNotEqual(
                list(
                        booleanArray(new boolean[] {true, false}),
                        intArray(new int[] {5, 2}),
                        stringArray("Hello", "World")),
                list(
                        booleanArray(new boolean[] {true, false}),
                        intArray(new int[] {1, 2}),
                        stringArray("Hello", "World")));

        assertNotEqual(
                list(
                        intArray(new int[] {1, 2}),
                        booleanArray(new boolean[] {true, false}),
                        stringArray("Hello", "World")),
                list(
                        booleanArray(new boolean[] {true, false}),
                        intArray(new int[] {1, 2}),
                        stringArray("Hello", "World")));
    }

    @Test
    void shouldRecurseAndCoerce() {
        assertEqual(list("a", list('b', list("c"))), list('a', list("b", list('c'))));
    }

    @Test
    void shouldTreatDifferentListImplementationSimilar() {
        for (ListValue list1 : equivalentLists) {
            for (ListValue list2 : equivalentLists) {
                assertEqual(list1, list2);
                assertArrayEquals(
                        list1.asArray(),
                        list2.asArray(),
                        format(
                                "%s.asArray != %s.toArray",
                                list1.getClass().getSimpleName(),
                                list2.getClass().getSimpleName()));
            }
        }
    }

    @Test
    void shouldNotTreatDifferentListImplementationSimilarOfNonEquivalentListsSimilar() {
        for (ListValue list1 : nonEquivalentLists) {
            for (ListValue list2 : nonEquivalentLists) {
                if (list1 == list2) {
                    continue;
                }
                assertNotEqual(list1, list2);
                assertFalse(
                        Arrays.equals(list1.asArray(), list2.asArray()),
                        format(
                                "%s.asArray != %s.toArray",
                                list1.getClass().getSimpleName(),
                                list2.getClass().getSimpleName()));
            }
        }
    }

    @Test
    void shouldReportIfEmpty() {
        // Given
        ListValue empty = VirtualValues.EMPTY_LIST;
        ListValue appended = empty.append(stringValue("test"));
        ListValue prepended = empty.prepend(stringValue("test"));
        ListValue concat = VirtualValues.concat(appended, prepended);
        ListValue emptyConcat = VirtualValues.concat(empty, empty);
        ListValue sliced = concat.slice(0, 1);
        ListValue emptySliced = concat.slice(0, 0);
        ListValue javaList = VirtualValues.fromList(List.of(stringValue("a"), stringValue("b"), stringValue("c")));
        ListValue emptyJavaList = VirtualValues.fromList(Collections.emptyList());
        ListValue rangeList = VirtualValues.range(0, 3, 1);
        ListValue emptyRangeList = VirtualValues.range(0, -1, 1);
        ListValue reversedList = concat.reverse();
        ListValue emptyReversedList = emptyConcat.reverse();
        ListValue arrayList = VirtualValues.fromArray(Values.intArray(new int[] {1, 2, 3}));
        ListValue emptyArrayList = VirtualValues.fromArray(Values.intArray(EMPTY_INT_ARRAY));

        // Then
        assertThat(empty.isEmpty()).isTrue();
        assertThat(appended.isEmpty()).isFalse();
        assertThat(prepended.isEmpty()).isFalse();
        assertThat(concat.isEmpty()).isFalse();
        assertThat(emptyConcat.isEmpty()).isTrue();
        assertThat(sliced.isEmpty()).isFalse();
        assertThat(emptySliced.isEmpty()).isTrue();
        assertThat(javaList.isEmpty()).isFalse();
        assertThat(emptyJavaList.isEmpty()).isTrue();
        assertThat(rangeList.isEmpty()).isFalse();
        assertThat(emptyRangeList.isEmpty()).isTrue();
        assertThat(reversedList.isEmpty()).isFalse();
        assertThat(emptyReversedList.isEmpty()).isTrue();
        assertThat(arrayList.isEmpty()).isFalse();
        assertThat(emptyArrayList.isEmpty()).isTrue();
    }

    @Test
    void storableListsShouldBeStorable() {
        ListValue list = VirtualValues.list(longValue(1), intValue(2), shortValue((short) 3));

        assertThat(list.toStorableArray()).isInstanceOf(LongArray.class).isEqualTo(longArray(new long[] {1, 2, 3}));
    }

    @Test
    void notStorableListShouldNotBeStorable() {
        ListValue list = VirtualValues.list(longArray(new long[] {1, 2, 3}), longArray(new long[] {4, 5, 6}));

        assertThatThrownBy(list::toStorableArray).isInstanceOf(CypherTypeException.class);
    }

    @Test
    void createAppropriateFloatingPointArray() {
        assertThat(VirtualValues.list(floatValue(Float.MAX_VALUE), shortValue((short) 1))
                        .toStorableArray())
                .isInstanceOf(FloatArray.class)
                .isEqualTo(floatArray(new float[] {Float.MAX_VALUE, 1}));
        assertThat(VirtualValues.list(floatValue(Float.MAX_VALUE), intValue(Integer.MAX_VALUE))
                        .toStorableArray())
                .isInstanceOf(DoubleArray.class)
                .isEqualTo(doubleArray(new double[] {Float.MAX_VALUE, Integer.MAX_VALUE}));
        // NOTE: this is really a lossy conversion but it is assumed that the user doing `SET n.prop=[1, 1.0] will
        // be aware of this conversion happening
        assertThat(VirtualValues.list(longValue(9007199254740993L), doubleValue(1.0))
                        .toStorableArray())
                .isInstanceOf(DoubleArray.class)
                .isEqualTo(doubleArray(new double[] {9007199254740992D, 1.0}));
    }
}
