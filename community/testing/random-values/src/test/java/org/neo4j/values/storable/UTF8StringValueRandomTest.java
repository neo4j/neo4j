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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class UTF8StringValueRandomTest {
    @Inject
    RandomSupport random;

    @Test
    void shouldCompareToRandomAlphanumericString() {
        for (int i = 0; i < 100; i++) {
            String string1 = random.nextAlphaNumericString();
            String string2 = random.nextAlphaNumericString();
            UTF8StringValueTest.assertCompareTo(string1, string2);
        }
    }

    @Test
    void shouldCompareToAsciiString() {
        for (int i = 0; i < 100; i++) {
            String string1 = random.nextAsciiString();
            String string2 = random.nextAsciiString();
            UTF8StringValueTest.assertCompareTo(string1, string2);
        }
    }

    @Test
    void shouldCompareBasicMultilingualPlaneString() {
        for (int i = 0; i < 100; i++) {
            String string1 = random.nextBasicMultilingualPlaneString();
            String string2 = random.nextBasicMultilingualPlaneString();
            UTF8StringValueTest.assertCompareTo(string1, string2);
        }
    }

    @Test
    void trimTest() {
        for (int i = 0; i < 10000; ++i) {
            final var string = random.nextString();
            final var originalStringCodepoints = string.codePoints().toArray();
            if (string.length() > 1) {
                final var utf8StringValue = Values.utf8Value(string);
                final var stringWrappingValue = Values.stringValue(string);
                final var from = random.nextInt(string.length() - 1);
                final var length = random.nextInt(string.length() - from);
                final var trim = new String(new int[] {random.among(originalStringCodepoints)}, 0, 1);
                final var trimA = Values.utf8Value(trim);
                final var trimB = Values.stringValue(trim);
                final var utfString = utf8StringValue.substring(from, length);
                final var wrappedString = stringWrappingValue.substring(from, length);

                final var resultA = utfString.trim(trimA);
                final var resultB = utfString.trim(trimB);
                final var resultC = wrappedString.trim(trimA);
                final var resultD = wrappedString.trim(trimB);

                assertThat(resultA)
                        .describedAs(
                                "trimA=%s, trimB=%s, results: %s, %s, %s, %s",
                                trimA, trimB, resultA, resultB, resultC, resultD)
                        .isEqualTo(resultB)
                        .isEqualTo(resultC)
                        .isEqualTo(resultD);
            }
        }
    }
}
