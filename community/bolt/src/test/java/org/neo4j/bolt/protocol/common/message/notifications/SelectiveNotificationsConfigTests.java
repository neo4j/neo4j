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
package org.neo4j.bolt.protocol.common.message.notifications;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.packstream.error.reader.PackstreamReaderException;

public class SelectiveNotificationsConfigTests {
    @Test
    public void shouldAcceptAndMapNullValues() throws PackstreamReaderException {
        var config = new SelectiveNotificationsConfig(null, null);

        var serverConfig = config.buildConfiguration(null);
        assertThat(serverConfig.severityLevel()).isEqualTo(NotificationConfiguration.Severity.INFORMATION);
        assertThat(serverConfig.disabledCategories()).isEqualTo(Collections.emptySet());
    }

    @Test
    public void shouldAcceptEmptyListForCategories() throws PackstreamReaderException {
        var config = new SelectiveNotificationsConfig(null, List.of());

        var serverConfig = config.buildConfiguration(null);

        assertThat(serverConfig.disabledCategories()).isEqualTo(Collections.emptySet());
    }

    @Test
    public void shouldThrowWhenUnrecognizedSeverity() {
        assertThatThrownBy(() -> {
                    new SelectiveNotificationsConfig("made up", null);
                })
                .isInstanceOf(PackstreamReaderException.class)
                .hasMessageContaining("severity");
    }

    @Test
    public void shouldThrowWhenUnrecognizedCategory() {
        assertThatThrownBy(() -> {
                    new SelectiveNotificationsConfig(null, List.of("made up"));
                })
                .isInstanceOf(PackstreamReaderException.class)
                .hasMessageContaining("category");
    }

    private static Stream<Arguments> severities() {
        return Stream.of(
                Arguments.of("INFORMATION", NotificationConfiguration.Severity.INFORMATION),
                Arguments.of("WARNING", NotificationConfiguration.Severity.WARNING),
                Arguments.of("information", NotificationConfiguration.Severity.INFORMATION),
                Arguments.of("warning", NotificationConfiguration.Severity.WARNING));
    }

    @ParameterizedTest
    @MethodSource("severities")
    public void shouldMapSeverityValues(String severityString, NotificationConfiguration.Severity expected)
            throws PackstreamReaderException {
        var config = new SelectiveNotificationsConfig(severityString, null);

        var serverConfig = config.buildConfiguration(null);
        assertThat(serverConfig.severityLevel()).isEqualTo(expected);
    }

    private static Stream<Arguments> categories() {
        return Stream.of(
                Arguments.of("PERFORMANCE", Set.of(NotificationConfiguration.Category.PERFORMANCE)),
                Arguments.of("DEPRECATION", Set.of(NotificationConfiguration.Category.DEPRECATION)),
                Arguments.of("UNRECOGNIZED", Set.of(NotificationConfiguration.Category.UNRECOGNIZED)),
                Arguments.of("HINT", Set.of(NotificationConfiguration.Category.HINT)),
                Arguments.of("GENERIC", Set.of(NotificationConfiguration.Category.GENERIC)),
                Arguments.of("UNSUPPORTED", Set.of(NotificationConfiguration.Category.UNSUPPORTED)),
                Arguments.of("performance", Set.of(NotificationConfiguration.Category.PERFORMANCE)),
                Arguments.of("deprecation", Set.of(NotificationConfiguration.Category.DEPRECATION)),
                Arguments.of("unrecognized", Set.of(NotificationConfiguration.Category.UNRECOGNIZED)),
                Arguments.of("hint", Set.of(NotificationConfiguration.Category.HINT)),
                Arguments.of("generic", Set.of(NotificationConfiguration.Category.GENERIC)),
                Arguments.of("unsupported", Set.of(NotificationConfiguration.Category.UNSUPPORTED)));
    }

    @ParameterizedTest
    @MethodSource("categories")
    public void shouldMapCategoryValues(String category, Set<NotificationConfiguration.Category> expected)
            throws PackstreamReaderException {
        var config = new SelectiveNotificationsConfig(null, List.of(category));

        var serverConfig = config.buildConfiguration(null);
        assertThat(serverConfig.disabledCategories()).isEqualTo(expected);
    }

    @Test
    public void shouldMergeConfigWithSpecified() throws PackstreamReaderException {
        var driver = new SelectiveNotificationsConfig("WARNING", null);
        var session = new SelectiveNotificationsConfig(null, List.of("hint"));

        var serverConfig = session.buildConfiguration(driver);
        assertThat(serverConfig.severityLevel()).isEqualTo(NotificationConfiguration.Severity.WARNING);
        assertThat(serverConfig.disabledCategories()).isEqualTo(Set.of(NotificationConfiguration.Category.HINT));
    }

    @Test
    public void shouldIgnoreParentConfigsWithSpecified() throws PackstreamReaderException {
        var driver = new SelectiveNotificationsConfig("WARNING", List.of("hint"));
        var session = new SelectiveNotificationsConfig("INFORMATION", List.of());

        var serverConfig = session.buildConfiguration(driver);
        assertThat(serverConfig.severityLevel()).isEqualTo(NotificationConfiguration.Severity.INFORMATION);
        assertThat(serverConfig.disabledCategories()).isEqualTo(Set.of());
    }
}
