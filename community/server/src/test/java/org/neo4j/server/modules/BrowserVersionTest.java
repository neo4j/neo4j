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
package org.neo4j.server.modules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.server.modules.BrowserVersion.fromPath;

import java.nio.file.Path;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class BrowserVersionTest {

    public static Stream<Arguments> validBrowserVersions() {
        return Stream.of(
                Arguments.of(
                        "neo4j-browser-2026.01.29+0.zip",
                        new BrowserVersion(Path.of("neo4j-browser-2026.01.29+0.zip"), LocalDate.of(2026, 1, 29), 0)),
                Arguments.of("neo4j-browser.zip", new BrowserVersion(Path.of("neo4j-browser.zip"), LocalDate.EPOCH, 0)),
                Arguments.of(
                        "neo4j-browser-2016.03.12+4.zip",
                        new BrowserVersion(Path.of("neo4j-browser-2016.03.12+4.zip"), LocalDate.of(2016, 3, 12), 4)));
    }

    @ParameterizedTest
    @MethodSource("validBrowserVersions")
    void shouldParseDateCorrectly(String input, BrowserVersion expectedVersion) throws ParseException {
        var actualVersion = BrowserVersion.fromPath(Path.of(input));
        assertThat(actualVersion).isEqualTo(expectedVersion);
    }

    @Test
    void shouldParseEpochWithoutVersion() throws ParseException {
        var v = fromPath(Path.of("neo4j-browser.zip"));
        assertThat(v).isNotNull();
        assertThat(v.versionDate()).isEqualTo(LocalDate.EPOCH);
        assertThat(v.subVersionNo()).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "invalid-version.zip",
                "neo4j-browser-2026.01.29+invalid.zip",
                "neo4j-browser-2026.01.29+0-invalid",
                "neo4j-browser-2026.01.29.zip",
                "neo4j-browser-01.29+invalid.zip",
                "neo4j-browser-.zip",
                "neo4j-browser-2016.zip"
            })
    void shouldThrowForInvalidEntry(String input) {
        assertThrows(ParseException.class, () -> fromPath(Path.of(input)));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "neo4j-browser-2026.01.32.zip",
                "neo4j-browser-2026.01.00.zip",
                "neo4j-browser-2026.13.29.zip",
                "neo4j-browser-90000000.12.01+0.zip"
            })
    void shouldThrowForInvalidDate(String input) {
        assertThrows(ParseException.class, () -> fromPath(Path.of(input)));
    }

    @Test
    void shouldOrderBySubversion() throws ParseException {
        var v1 = fromPath(Path.of("neo4j-browser-2026.01.29+0.zip"));
        var v2 = fromPath(Path.of("neo4j-browser-2026.01.29+1.zip"));
        var v3 = fromPath(Path.of("neo4j-browser-2026.01.29+2.zip"));

        var versions = new ArrayList<>(List.of(v2, v1, v3));
        versions.sort(BrowserVersion::compareTo);

        assertThat(versions).isSorted();
        assertThat(versions.get(versions.size() - 1)).isEqualTo(v3);
    }

    @Test
    void shouldOrderByDate() throws ParseException {
        var v1 = fromPath(Path.of("neo4j-browser-2024.01.29+0.zip"));
        var v2 = fromPath(Path.of("neo4j-browser-2025.05.29+0.zip"));
        var v3 = fromPath(Path.of("neo4j-browser-2026.10.29+0.zip"));

        var versions = new ArrayList<>(List.of(v2, v1, v3));
        versions.sort(BrowserVersion::compareTo);

        assertThat(versions).isSorted();
        assertThat(versions.get(versions.size() - 1)).isEqualTo(v3);
    }
}
