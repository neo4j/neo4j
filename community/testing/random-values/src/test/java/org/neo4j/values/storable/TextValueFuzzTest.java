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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class TextValueFuzzTest {
    @Inject
    private RandomSupport random;

    private static final int ITERATIONS = 1000;

    @Test
    void shouldCompareToForAllStringsInBasicMultilingualPlane() {
        for (int i = 0; i < ITERATIONS; i++) {
            assertConsistent(
                    random.nextBasicMultilingualPlaneString(),
                    random.nextBasicMultilingualPlaneString(),
                    (t1, t2) -> Math.signum(t1.compareTo(t2)));
        }
    }

    @Test
    void shouldAdd() {
        for (int i = 0; i < ITERATIONS; i++) {
            assertConsistent(random.nextString(), random.nextString(), TextValue::plus);
        }
    }

    @Test
    void shouldComputeLength() {
        for (int i = 0; i < ITERATIONS; i++) {
            assertConsistent(random.nextString(), TextValue::length);
        }
    }

    @Test
    void shouldComputeIsEmpty() {
        for (int i = 0; i < ITERATIONS; i++) {
            assertConsistent(random.nextString(), TextValue::isEmpty);
        }
    }

    @Test
    void shouldReverse() {
        for (int i = 0; i < ITERATIONS; i++) {
            assertConsistent(random.nextString(), TextValue::reverse);
        }
    }

    @Test
    void shouldTrim() {
        for (int i = 0; i < ITERATIONS; i++) {
            assertConsistent(random.nextString(), TextValue::trim);
        }
    }

    @Test
    void shouldSubstring() {
        for (int i = 0; i < ITERATIONS; i++) {
            final var randomString = random.nextString();
            final int stringLength = randomString.length();
            final int start = random.nextInt(stringLength);
            final int length = random.nextInt(stringLength - start);
            assertConsistent(randomString, value -> value.substring(start, length));
        }
    }

    @Test
    void shouldHandleStringPredicates() {
        for (int i = 0; i < ITERATIONS; i++) {
            String value = random.nextString();
            String other;
            if (random.nextBoolean()) {
                other = value;
            } else {
                other = random.nextString();
            }

            assertConsistent(value, other, TextValue::startsWith);
            assertConsistent(value, other, TextValue::endsWith);
            assertConsistent(value, other, TextValue::contains);
        }
    }

    private <T> void assertConsistent(String string, Function<TextValue, T> test) {
        final var utf8Bytes = string.getBytes(UTF_8);
        TextValue textValue = stringValue(string);
        TextValue utf8Value = utf8Value(utf8Bytes);
        TextValue utf8Substring = randomUTF8SubstringEqualTo(string);

        T a = test.apply(textValue);
        T b = test.apply(utf8Value);
        T c = test.apply(utf8Substring);

        String errorMsg = format("operation not consistent for %s", string);
        assertThat(a).as(errorMsg).isEqualTo(b);
        assertThat(a).as(errorMsg).isEqualTo(c);
        assertThat(b).as(errorMsg).isEqualTo(a);
        assertThat(b).as(errorMsg).isEqualTo(c);
        assertThat(c).as(errorMsg).isEqualTo(a);
        assertThat(c).as(errorMsg).isEqualTo(b);
    }

    private <T> void assertConsistent(String string1, String string2, BiFunction<TextValue, TextValue, T> test) {
        TextValue textValue1 = stringValue(string1);
        TextValue textValue2 = stringValue(string2);
        TextValue utf8Value1 = utf8Value(string1.getBytes(UTF_8));
        TextValue utf8Value2 = utf8Value(string2.getBytes(UTF_8));
        TextValue utf8Substring1 = randomUTF8SubstringEqualTo(string1);
        TextValue utf8Substring2 = randomUTF8SubstringEqualTo(string2);

        T a = test.apply(textValue1, textValue2);
        T b = test.apply(textValue1, utf8Value2);
        T c = test.apply(utf8Value1, textValue2);
        T d = test.apply(utf8Value1, utf8Value2);
        T e = test.apply(utf8Substring1, utf8Substring2);
        T f = test.apply(utf8Substring1, utf8Value2);

        String errorMsg = format("operation not consistent for `%s` and `%s`", string1, string2);
        assertThat(a).as(errorMsg).isEqualTo(b);
        assertThat(b).as(errorMsg).isEqualTo(a);
        assertThat(a).as(errorMsg).isEqualTo(c);
        assertThat(c).as(errorMsg).isEqualTo(a);
        assertThat(a).as(errorMsg).isEqualTo(d);
        assertThat(d).as(errorMsg).isEqualTo(a);
        assertThat(e).as(errorMsg).isEqualTo(a);
        assertThat(a).as(errorMsg).isEqualTo(e);
        assertThat(f).as(errorMsg).isEqualTo(a);
        assertThat(a).as(errorMsg).isEqualTo(f);
    }

    private TextValue randomUTF8SubstringEqualTo(String string) {
        final var utf8Bytes = string.getBytes(UTF_8);
        final var prefix = random.nextString().getBytes(UTF_8);
        final var suffix = random.nextString().getBytes(UTF_8);
        final var substringBytes = Arrays.copyOf(prefix, prefix.length + utf8Bytes.length + suffix.length);
        System.arraycopy(utf8Bytes, 0, substringBytes, prefix.length, utf8Bytes.length);
        System.arraycopy(suffix, 0, substringBytes, prefix.length + utf8Bytes.length, suffix.length);
        return utf8Value(substringBytes, prefix.length, utf8Bytes.length);
    }
}
