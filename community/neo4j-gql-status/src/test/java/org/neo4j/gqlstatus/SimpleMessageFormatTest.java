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
package org.neo4j.gqlstatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gqlstatus.SimpleMessageFormat.compile;

import java.util.List;
import java.util.function.Function;
import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.api.Test;

// More test coverage in GqlStatusInfoCodeMessageFormatPropertyTest (lives in other module)
public class SimpleMessageFormatTest {

    @Test
    void basicSubstitution() {
        assertFormat("").isEqualTo("");
        assertFormat("%s", "a").isEqualTo("a");
        assertFormat(" %s ", "a").isEqualTo(" a ");
        assertFormat("%s%s", "a", "b").isEqualTo("ab");
        assertFormat(" %s %s ", "a", "b").isEqualTo(" a b ");
        assertFormat("%%s%%%s%%%%s%%%%%s", "a", "b", "c", "d").isEqualTo("%a%%b%%%c%%%%d");
    }

    @Test
    void substitutionWithSpecialChars() {
        for (final var substitution : List.of("%s", "%", "$", "^", "\n", "$a", "$1", ".", "{}", "{10}")) {
            for (final var around : List.of("", "%", "$", "^", "\n", "$a", "$1", ".", "{}", "{10}", " ")) {
                assertFormat(around + "%s" + around, substitution).isEqualTo(around + substitution + around);
                assertFormat(around + around, substitution).isEqualTo(around + around);
            }
        }
    }

    @Test
    void missingParameters() {
        assertFormat("%s").isEqualTo("null");
        assertFormat(" %s ").isEqualTo(" null ");
        assertFormat("%s%s").isEqualTo("nullnull");
        assertFormat("%s%s", "a").isEqualTo("anull");
        assertFormat(" %s %s ").isEqualTo(" null null ");
        assertFormat(" %s %s ", "a").isEqualTo(" a null ");
        assertFormat("%%s%%%s%%%%s%%%%%s").isEqualTo("%null%%null%%%null%%%%null");
        assertFormat("%%s%%%s%%%%s%%%%%s", "a", "b").isEqualTo("%a%%b%%%null%%%%null");
    }

    @Test
    void tooManyParameters() {
        assertFormat("", "x").isEqualTo("");
        assertFormat("%s", "a", "x").isEqualTo("a");
        assertFormat(" %s ", "a", "x").isEqualTo(" a ");
        assertFormat("%s%s", "a", "b", "x").isEqualTo("ab");
        assertFormat(" %s %s ", "a", "b", "x").isEqualTo(" a b ");
        assertFormat("%%s%%%s%%%%s%%%%%s", "a", "b", "c", "d", "x").isEqualTo("%a%%b%%%c%%%%d");
    }

    @Test
    void noParameters() {
        assertFormat("hej och hå").isEqualTo("hej och hå");
        assertFormat("hej och hå", "x", "x", "x").isEqualTo("hej och hå");
        assertFormat("%%h%e%j% %o%c%h% %h%å%%").isEqualTo("%%h%e%j% %o%c%h% %h%å%%");
    }

    @Test
    void nullSafety() {
        assertThat(compile("a: %s").format(null)).isEqualTo("a: null");
        assertThat(compile("a").format(null)).isEqualTo("a");
        assertThat(compile("a: %s").format(new Object[] {null})).isEqualTo("a: null");
    }

    @Test
    void formatter() {
        final var formatter = new Function<Object, String>() {
            @Override
            public String apply(Object o) {
                return "👉" + o + "👈";
            }
        };
        assertFormat("Yo %s!", formatter, "Joe").isEqualTo("Yo 👉Joe👈!");
        assertFormat("Yo %s!", formatter, (Object) null).isEqualTo("Yo 👉null👈!");
    }

    private AbstractStringAssert<?> assertFormat(String template, Object... params) {
        final var formattedWithStart = compile(template).format(new StringBuilder("start"), params);
        assertThat(formattedWithStart).startsWith("start");
        return assertThat(compile(template).format(params)).isEqualTo(formattedWithStart.substring("start".length()));
    }

    private AbstractStringAssert<?> assertFormat(
            String template, Function<Object, String> formatter, Object... params) {
        final var formattedWithStart = compile(template, formatter).format(new StringBuilder("start"), params);
        assertThat(formattedWithStart).startsWith("start");
        return assertThat(compile(template, formatter).format(params))
                .isEqualTo(formattedWithStart.substring("start".length()));
    }
}
