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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.values.storable.Values.EMPTY_STRING;
import static org.neo4j.values.storable.Values.charValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;
import static org.neo4j.values.virtual.VirtualValues.list;

import org.junit.jupiter.api.Test;

class CharValueTest {
    private static final char[] CHARS = {' ', '楡', 'a', '7', 'Ö'};

    @Test
    void shouldHandleDifferentTypesOfChars() {
        for (char c : CHARS) {
            TextValue charValue = charValue(c);
            TextValue stringValue = stringValue(Character.toString(c));

            assertThat(charValue).isEqualTo(stringValue);
            assertThat(charValue.length()).isEqualTo(stringValue.length());
            assertThat(charValue.hashCode()).isEqualTo(stringValue.hashCode());
            assertThat(charValue.split(Character.toString(c))).isEqualTo(stringValue.split(Character.toString(c)));
            assertThat(charValue.toUpper()).isEqualTo(stringValue.toUpper());
            assertThat(charValue.toLower()).isEqualTo(stringValue.toLower());
        }
    }

    @Test
    void shouldSplit() {
        CharValue charValue = charValue('a');
        assertThat(charValue.split("a")).isEqualTo(list(EMPTY_STRING, EMPTY_STRING));
        assertThat(charValue.split("A")).isEqualTo(list(charValue));
    }

    @Test
    void shouldTrim() {
        assertThat(charValue('a').trim()).isEqualTo(charValue('a'));
        assertThat(charValue('a').trim(charValue('a'))).isEqualTo(EMPTY_STRING);
        assertThat(charValue(' ').trim()).isEqualTo(EMPTY_STRING);
        assertThat(charValue(' ').trim(charValue('a'))).isEqualTo(charValue(' '));
        assertThat(charValue('x').trim(stringValue("abcx"))).isEqualTo(EMPTY_STRING);
        String string = "abcx";
        byte[] bytes = string.getBytes(UTF_8);
        assertThat(charValue('x').trim(utf8Value(bytes, 0, 4))).isEqualTo(EMPTY_STRING);
    }

    @Test
    void shouldLTrim() {
        assertThat(charValue('a').ltrim()).isEqualTo(charValue('a'));
        assertThat(charValue('a').ltrim(charValue('a'))).isEqualTo(EMPTY_STRING);
        assertThat(charValue(' ').ltrim()).isEqualTo(EMPTY_STRING);
        assertThat(charValue(' ').ltrim(charValue('a'))).isEqualTo(charValue(' '));
        assertThat(charValue('x').ltrim(stringValue("abcx"))).isEqualTo(EMPTY_STRING);
        String string = "abcx";
        byte[] bytes = string.getBytes(UTF_8);
        assertThat(charValue('x').ltrim(utf8Value(bytes, 0, 4))).isEqualTo(EMPTY_STRING);
    }

    @Test
    void shouldRTrim() {
        assertThat(charValue('a').rtrim()).isEqualTo(charValue('a'));
        assertThat(charValue('a').rtrim(charValue('a'))).isEqualTo(EMPTY_STRING);
        assertThat(charValue(' ').rtrim()).isEqualTo(EMPTY_STRING);
        assertThat(charValue(' ').rtrim(charValue('a'))).isEqualTo(charValue(' '));
        assertThat(charValue('x').rtrim(stringValue("abcx"))).isEqualTo(EMPTY_STRING);
        String string = "abcx";
        byte[] bytes = string.getBytes(UTF_8);
        assertThat(charValue('x').rtrim(utf8Value(bytes, 0, 4))).isEqualTo(EMPTY_STRING);
    }

    @Test
    void shouldReverse() {
        for (char c : CHARS) {
            CharValue charValue = charValue(c);
            assertThat(charValue.reverse()).isEqualTo(charValue);
        }
    }

    @Test
    void shouldReplace() {
        assertThat(charValue('a').replace("a", "a long string")).isEqualTo(stringValue("a long string"));
        assertThat(charValue('a').replace("b", "a long string")).isEqualTo(charValue('a'));
    }

    @Test
    void shouldSubstring() {
        assertThat(charValue('a').substring(0, 1)).isEqualTo(charValue('a'));
        assertThat(charValue('a').substring(1, 3)).isEqualTo(EMPTY_STRING);
    }
}
