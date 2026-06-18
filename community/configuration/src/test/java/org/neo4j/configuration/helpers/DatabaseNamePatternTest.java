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
package org.neo4j.configuration.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DatabaseNamePatternTest {

    @Test
    void shouldNotGetAnErrorForAValidDatabaseName() {
        assertValid("my.Vaild-D*b123?");
        assertValid("my.Vaild-Db123");
    }

    @Test
    void shouldMatchWithProvidedDatabaseNames() {
        assertThat(new DatabaseNamePattern("???").matches("abc")).isTrue();
        assertThat(new DatabaseNamePattern("****").matches("Customer01")).isTrue();
        assertThat(new DatabaseNamePattern("?*??").matches("Customer01")).isTrue();
        assertThat(new DatabaseNamePattern("cust*").matches("Customer01")).isTrue();
        assertThat(new DatabaseNamePattern("*01").matches("Customer01")).isTrue();
        assertThat(new DatabaseNamePattern("Widgets-customer-*-db1").matches("Widgets-customer-001-db1"))
                .isTrue();
        assertThat(new DatabaseNamePattern("Widgets-customer-*-db?").matches("Widgets-customer-222-db5"))
                .isTrue();
        assertThat(new DatabaseNamePattern("Widgets-****-*-db?").matches("Widgets-customer-222-db5"))
                .isTrue();
        assertThat(new DatabaseNamePattern("c*01").matches("Customer01")).isTrue();
        assertThat(new DatabaseNamePattern("c?st*tp").matches("Customer01tp")).isTrue();
        assertThat(new DatabaseNamePattern("cust*tp?").matches("Customer01tp")).isTrue();
        assertThat(new DatabaseNamePattern("database1").matches("database1")).isTrue();
        assertThat(new DatabaseNamePattern("my.Vaild-D*b1?3").matches("my.Vaild-Daweeb123"))
                .isTrue();
    }

    @Test
    void shouldNotMatchWithProvidedDatabaseNames() {
        assertThat(new DatabaseNamePattern("C?").matches("Customer01")).isFalse();
        assertThat(new DatabaseNamePattern("C?tomer01").matches("Customer01")).isFalse();
        assertThat(new DatabaseNamePattern("temp").matches("temp2")).isFalse();
        assertThat(new DatabaseNamePattern("r*r").matches("tur")).isFalse();
    }

    @Test
    void shouldGetAnErrorForAnEmptyDatabaseName() {
        assertThatThrownBy(() -> assertValid(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The provided database name is empty.");

        assertThatThrownBy(() -> assertValid(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("The provided database name is empty.");
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidCharacters() {
        assertThatThrownBy(() -> assertValid("database%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Database name 'database%' contains illegal characters. Use simple ascii characters, numbers,"
                                + " dots, question marks, asterisk and dashes.");

        assertThatThrownBy(() -> assertValid("data{base}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Database name 'data{base}' contains illegal characters. Use simple ascii characters, numbers,"
                                + " dots, question marks, asterisk and dashes.");

        assertThatThrownBy(() -> assertValid("data/base"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Database name 'data/base' contains illegal characters. Use simple ascii characters, numbers,"
                                + " dots, question marks, asterisk and dashes.");

        assertThatThrownBy(() -> assertValid("dataåäö"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Database name 'dataåäö' contains illegal characters. Use simple ascii characters, numbers, "
                                + "dots, question marks, asterisk and dashes.");
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidLength() {
        // Too short
        assertThatThrownBy(() -> assertValid(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The provided database name is empty.");

        assertThatThrownBy(() -> assertValid(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The provided database name is empty.");

        assertThatThrownBy(() -> assertValid("a".repeat(64)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The provided database name must have a length between 1 and 63 characters.");
    }

    @ParameterizedTest
    @MethodSource("values")
    void shouldGetExactDatabaseNames(List<String> names, Optional<Set<String>> expected) {
        assertThat(DatabaseNamePattern.exactNames(
                        names.stream().map(DatabaseNamePattern::new).toList()))
                .isEqualTo(expected);
    }

    static Stream<Arguments> values() {
        return Stream.of(
                Arguments.of(List.of("foo"), Optional.of(Set.of("foo"))),
                Arguments.of(List.of("foo", "bar"), Optional.of(Set.of("foo", "bar"))),
                Arguments.of(List.of("foo", "foo"), Optional.of(Set.of("foo"))),
                Arguments.of(List.of("foo?", "bar"), Optional.empty()));
    }

    @Test
    void shouldMatchAnyPattern() {
        var patterns = List.of("foo*", "foo", "foo?", "bar");
        var match = DatabaseNamePattern.matchAny(
                patterns.stream().map(DatabaseNamePattern::new).toList());

        assertThat(match.test("foo")).isTrue();
        assertThat(match.test("bar")).isTrue();
        assertThat(match.test("foobar")).isTrue();
        assertThat(match.test("raboof")).isFalse();
    }

    private static void assertValid(String name) {
        new DatabaseNamePattern(name);
    }
}
