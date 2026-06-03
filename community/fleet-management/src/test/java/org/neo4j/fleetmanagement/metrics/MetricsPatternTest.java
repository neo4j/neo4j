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
package org.neo4j.fleetmanagement.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.neo4j.fleetmanagement.utils.PatternCompiler;

class MetricsPatternTest {
    @Test
    void shouldMatchExactPattern() {
        // Given
        String patternStr = "metrics.test.value";
        Pattern pattern = PatternCompiler.constructPattern(patternStr);

        // Then
        assertThat(pattern.matcher("metrics.test.value").matches()).isTrue();
        assertThat(pattern.matcher("metrics.test.other").matches()).isFalse();
    }

    @Test
    void shouldMatchPatternWithNamedGroups() {
        // Given
        String patternStr = "metrics.<name>.value";
        Pattern pattern = PatternCompiler.constructPattern(patternStr);

        // When
        var matcher = pattern.matcher("metrics.test.value");

        // Then
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group("name")).isEqualTo("test");
    }

    @Test
    void shouldMatchPatternWithMultipleNamedGroups() {
        // Given
        String patternStr = "metrics.<group>.<name>.value";
        Pattern pattern = PatternCompiler.constructPattern(patternStr);

        // When
        var matcher = pattern.matcher("metrics.system.test.value");

        // Then
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group("group")).isEqualTo("system");
        assertThat(matcher.group("name")).isEqualTo("test");
    }

    @Test
    void shouldMatchPatternWithWildcards() {
        // Given
        String patternStr = "**.<name>.value";
        Pattern pattern = PatternCompiler.constructPattern(patternStr);

        // Then
        assertThat(pattern.matcher("prefix.test.value").matches()).isTrue();
        assertThat(pattern.matcher("a.b.c.test.value").matches()).isTrue();
        assertThat(pattern.matcher("test.value").matches()).isFalse(); // Missing wildcard content
    }

    @Test
    void shouldMatchPatternWithMultipleWildcards() {
        // Given
        String patternStr = "**.<name>.*.value";
        Pattern pattern = PatternCompiler.constructPattern(patternStr);

        // When
        var matcher = pattern.matcher("a.b.c.test.e.value");

        // Then
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group("name")).isEqualTo("test");
        assertThat(pattern.matcher("prefix.test.middle.value").matches()).isTrue();
        assertThat(pattern.matcher("test.value").matches()).isFalse(); // Missing wildcard content
    }

    @Test
    void shouldRequireAllSegments() {
        // Given
        String patternStr = "metrics.<name>.value";
        Pattern pattern = PatternCompiler.constructPattern(patternStr);

        // Then
        assertThat(pattern.matcher("metrics.test.value").matches()).isTrue();
        assertThat(pattern.matcher("metrics.value").matches()).isFalse(); // Missing name
        assertThat(pattern.matcher("metrics.test").matches()).isFalse(); // Missing value
    }

    @Test
    void shouldHandleMultipleWildcardSegments() {
        // Given
        String patternStr = "**.<database>.*.<type>";
        Pattern pattern = PatternCompiler.constructPattern(patternStr);

        // Then
        assertThat(pattern.matcher("prefix.neo4j.middle.committed").matches()).isTrue();
        assertThat(pattern.matcher("a.b.c.system.d.active").matches()).isTrue();
        assertThat(pattern.matcher("prefix.neo4j").matches()).isFalse(); // Missing type
        assertThat(pattern.matcher("prefix.type").matches()).isFalse(); // Missing database
    }

    @Test
    void shouldEscapeDotsProperly() {
        // Given
        String patternStr = "metrics.test.<database>.value";
        Pattern pattern = PatternCompiler.constructPattern(patternStr);

        // Then
        assertThat(pattern.matcher("metrics.test.neo4j.value").matches()).isTrue();
        assertThat(pattern.matcher("metricsxtestxneo4jxvalue").matches()).isFalse(); // Different separator
        assertThat(pattern.matcher("metrics.test.neo4j_value").matches()).isFalse(); // Wrong separator
    }
}
