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
package org.neo4j.internal.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.NameUtil.escapeDoubleQuotes;
import static org.neo4j.internal.helpers.NameUtil.escapeGlob;
import static org.neo4j.internal.helpers.NameUtil.escapeName;
import static org.neo4j.internal.helpers.NameUtil.escapeSingleQuotes;
import static org.neo4j.internal.helpers.NameUtil.forceEscapeName;
import static org.neo4j.internal.helpers.NameUtil.unescapeName;

import org.junit.jupiter.api.Test;

public class NameUtilTest {
    @Test
    void escapeNonAlphanumericStrings() {
        assertThat(escapeName("abc12_A")).isEqualTo("abc12_A");
        assertThat(escapeName("Åbc12_A")).isEqualTo("Åbc12_A");
        assertThat(escapeName("\0")).isEqualTo("`\0`");
        assertThat(escapeName("\n")).isEqualTo("`\n`");
        assertThat(escapeName("comma, separated")).isEqualTo("`comma, separated`");
        assertThat(escapeName("escaped content `back ticks #")).isEqualTo("`escaped content ``back ticks #`");
        assertThat(escapeName("escaped content two `back `ticks")).isEqualTo("`escaped content two ``back ``ticks`");
    }

    @Test
    void forceEscapeNonAlphanumericStrings() {
        assertThat(forceEscapeName("abc12_A")).isEqualTo("`abc12_A`");
        assertThat(escapeName("\0")).isEqualTo("`\0`");
    }

    @Test
    void escapeAndReplaceUnicodeBackticks() {
        assertThat(forceEscapeName("\\u0060")).isEqualTo("````");
        assertThat(forceEscapeName("\\uuuuu0060")).isEqualTo("````");
        assertThat(forceEscapeName("T\\u0060est")).isEqualTo("`T``est`");
    }

    @Test
    void escapeSingleQuotesAlphanumericStrings() {
        assertThat(escapeSingleQuotes("\\'test'")).isEqualTo("\\\\\\'test\\'");
    }

    @Test
    void escapeDoubleQuotesAlphanumericStrings() {
        assertThat(escapeDoubleQuotes("\\\"test\"")).isEqualTo("\\\\\\\"test\\\"");
    }

    @Test
    void reEscapeNonAlphanumericStrings() {
        assertThat(unescapeName("abc12_A")).isEqualTo("abc12_A");
        assertThat(unescapeName("Åbc12_A")).isEqualTo("Åbc12_A");
        assertThat(unescapeName("`\0`")).isEqualTo("\0");
        assertThat(unescapeName("`\n`")).isEqualTo("\n");
        assertThat(unescapeName("`comma, separated`")).isEqualTo("comma, separated");
        assertThat(unescapeName("`escaped content ``back ticks #`")).isEqualTo("escaped content `back ticks #");
        assertThat(unescapeName("`escaped content two ``back ``ticks`")).isEqualTo("escaped content two `back `ticks");
    }

    @Test
    void escapeNonGlobStrings() {
        assertThat(escapeGlob("abc.1?2_A")).isEqualTo("abc.1?2_A");
        assertThat(escapeGlob("Åbc*12_A")).isEqualTo("Åbc*12_A");
        assertThat(escapeGlob("*")).isEqualTo("*");
        assertThat(escapeGlob("?")).isEqualTo("?");
        assertThat(escapeGlob("abc.glob")).isEqualTo("abc.glob");
        assertThat(escapeGlob(".glob")).isEqualTo("`.glob`");
        assertThat(escapeGlob("\0")).isEqualTo("`\0`");
        assertThat(escapeGlob("\n")).isEqualTo("`\n`");
        assertThat(escapeGlob("comma, separated")).isEqualTo("`comma, separated`");
        assertThat(escapeGlob("escaped content `back ticks #")).isEqualTo("`escaped content ``back ticks #`");
        assertThat(escapeGlob("escaped content two `back `ticks")).isEqualTo("`escaped content two ``back ``ticks`");
    }
}
