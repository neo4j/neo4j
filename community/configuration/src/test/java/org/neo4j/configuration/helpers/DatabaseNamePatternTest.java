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

import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DatabaseNamePatternTest {

    @Test
    void shouldNotGetAnErrorForAValidDatabaseName() {
        assertValid("my.Vaild-D*b123?");
        assertValid("my.Vaild-Db123");
    }

    @Test
    void shouldMatchWithProvidedDatabaseNames() {
        assertTrue(new DatabaseNamePattern("???").matches("abc"));
        assertTrue(new DatabaseNamePattern("****").matches("Customer01"));
        assertTrue(new DatabaseNamePattern("?*??").matches("Customer01"));
        assertTrue(new DatabaseNamePattern("cust*").matches("Customer01"));
        assertTrue(new DatabaseNamePattern("*01").matches("Customer01"));
        assertTrue(new DatabaseNamePattern("Widgets-customer-*-db1").matches("Widgets-customer-001-db1"));
        assertTrue(new DatabaseNamePattern("Widgets-customer-*-db?").matches("Widgets-customer-222-db5"));
        assertTrue(new DatabaseNamePattern("Widgets-****-*-db?").matches("Widgets-customer-222-db5"));
        assertTrue(new DatabaseNamePattern("c*01").matches("Customer01"));
        assertTrue(new DatabaseNamePattern("c?st*tp").matches("Customer01tp"));
        assertTrue(new DatabaseNamePattern("cust*tp?").matches("Customer01tp"));
        assertTrue(new DatabaseNamePattern("database1").matches("database1"));
        assertTrue(new DatabaseNamePattern("my.Vaild-D*b1?3").matches("my.Vaild-Daweeb123"));
    }

    @Test
    void shouldNotMatchWithProvidedDatabaseNames() {
        assertFalse(new DatabaseNamePattern("C?").matches("Customer01"));
        assertFalse(new DatabaseNamePattern("C?tomer01").matches("Customer01"));
        assertFalse(new DatabaseNamePattern("temp").matches("temp2"));
        assertFalse(new DatabaseNamePattern("r*r").matches("tur"));
    }

    @Test
    void shouldGetAnErrorForAnEmptyDatabaseName() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> assertValid(""));
        assertEquals("The provided database name is empty.", e.getMessage());

        Exception e2 = assertThrows(NullPointerException.class, () -> assertValid(null));
        assertEquals("The provided database name is empty.", e2.getMessage());
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidCharacters() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> assertValid("database%"));
        assertEquals(
                "Database name 'database%' contains illegal characters. Use simple ascii characters, numbers,"
                        + " dots, question marks, asterisk and dashes.",
                e.getMessage());

        Exception e2 = assertThrows(IllegalArgumentException.class, () -> assertValid("data{base}"));
        assertEquals(
                "Database name 'data{base}' contains illegal characters. Use simple ascii characters, numbers,"
                        + " dots, question marks, asterisk and dashes.",
                e2.getMessage());

        Exception e3 = assertThrows(IllegalArgumentException.class, () -> assertValid("data/base"));
        assertEquals(
                "Database name 'data/base' contains illegal characters. Use simple ascii characters, numbers,"
                        + " dots, question marks, asterisk and dashes.",
                e3.getMessage());

        Exception e4 = assertThrows(IllegalArgumentException.class, () -> assertValid("dataåäö"));
        assertEquals(
                "Database name 'dataåäö' contains illegal characters. Use simple ascii characters, numbers, "
                        + "dots, question marks, asterisk and dashes.",
                e4.getMessage());
    }

    @Test
    void shouldGetAnErrorForADatabaseNameWithInvalidLength() {
        // Too short
        Exception e1 = assertThrows(IllegalArgumentException.class, () -> assertValid(" "));
        assertEquals("The provided database name is empty.", e1.getMessage());

        Exception e2 = assertThrows(IllegalArgumentException.class, () -> assertValid(""));
        assertEquals("The provided database name is empty.", e2.getMessage());

        Exception e3 = assertThrows(IllegalArgumentException.class, () -> assertValid("a" + randomAscii(64)));

        assertEquals("The provided database name must have a length between 1 and 63 characters.", e3.getMessage());
    }

    private static void assertValid(String name) {
        new DatabaseNamePattern(name);
    }
}
