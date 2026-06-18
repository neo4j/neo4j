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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.database.NormalizedDatabaseName;

class DatabaseNameValidatorTest {
    @Test
    void shouldNotGetAnErrorForAValidDatabaseName() {
        assertValid("my.Vaild-Db123");
    }

    @Test
    void shouldNotGetAnErrorForAValidDatabaseNameStartingWithDigit() {
        assertValid("1database");
    }

    @Test
    void shouldGetAnErrorForAnEmptyDatabaseName() {
        assertThatThrownBy(() -> assertValid(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The provided database name is empty.");

        assertThatThrownBy(() -> DatabaseNameValidator.validateExternalDatabaseName(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("The provided database name is empty.");
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidCharacters() {
        assertThatThrownBy(() -> assertValid("database%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Database name 'database%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.");

        assertThatThrownBy(() -> assertValid("data_base"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Database name 'data_base' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.");

        assertThatThrownBy(() -> assertValid("dataåäö"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Database name 'dataåäö' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.");
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidFirstCharacter() {
        assertThatThrownBy(() -> assertValid("ädatabase"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Database name 'ädatabase' is not starting with an ASCII alphabetic character or number.");

        assertThatThrownBy(() -> assertValid("_database"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Database name '_database' is not starting with an ASCII alphabetic character or number.");
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithSystemPrefix() {
        assertThatThrownBy(() -> assertValid("systemdatabase"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Database name 'systemdatabase' is invalid, due to the prefix 'system'.");
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidLength() {
        // Too short
        assertThatThrownBy(() -> assertValid("me"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The provided database name must have a length between 3 and 63 characters.");

        // Too long
        assertThatThrownBy(() -> assertValid(
                        "ihaveallooootoflettersclearlymorethanishould-ihaveallooootoflettersclearlymorethanishould"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The provided database name must have a length between 3 and 63 characters.");
    }

    private static void assertValid(String name) {
        DatabaseNameValidator.validateExternalDatabaseName(new NormalizedDatabaseName(name));
    }
}
