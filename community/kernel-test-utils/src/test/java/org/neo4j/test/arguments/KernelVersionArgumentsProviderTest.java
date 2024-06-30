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
package org.neo4j.test.arguments;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.test.arguments.KernelVersionArgumentsProvider.fromKernelVersion;
import static org.neo4j.test.arguments.KernelVersionArgumentsProvider.fromString;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.neo4j.kernel.KernelVersion;

class KernelVersionArgumentsProviderTest {

    @ParameterizedTest
    @KernelVersionSource(lessThan = "5.0")
    void lessThan(KernelVersion kernelVersion) {
        assertThat(fromKernelVersion(kernelVersion)).isLessThan(v(5, 0));
    }

    @ParameterizedTest
    @KernelVersionSource(greaterThan = "5.0")
    void greaterThan(KernelVersion kernelVersion) {
        assertThat(fromKernelVersion(kernelVersion)).isGreaterThan(v(5, 0));
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void atLeast(KernelVersion kernelVersion) {
        assertThat(fromKernelVersion(kernelVersion)).isGreaterThanOrEqualTo(v(5, 0));
    }

    @ParameterizedTest
    @KernelVersionSource(greaterThan = "4.0", lessThan = "5.0")
    void intersection1(KernelVersion kernelVersion) {
        var version = fromKernelVersion(kernelVersion);
        assertThat(version.major()).isEqualTo(4);
        assertThat(version.minor()).isPositive();
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "4.0", lessThan = "5.0")
    void intersection2(KernelVersion kernelVersion) {
        var version = fromKernelVersion(kernelVersion);
        assertThat(version.major()).isEqualTo(4);
        assertThat(version.minor()).isNotNegative();
    }

    @ParameterizedTest
    @KernelVersionSource(greaterThan = "5.0", lessThan = "4.0")
    void disjunctive(KernelVersion kernelVersion) {
        var version = fromKernelVersion(kernelVersion);
        assertThat(version.major()).isNotEqualTo(4);
    }

    @ParameterizedTest
    @KernelVersionSource(greaterThan = "4")
    void greaterThanMajor(KernelVersion kernelVersion) {
        assertThat(fromKernelVersion(kernelVersion)).isGreaterThanOrEqualTo(v(5, 0));
    }

    @ParameterizedTest
    @KernelVersionSource(lessThan = "4.0", greaterThan = "4.0")
    void myTest(KernelVersion kernelVersion) {
        assertThat(fromKernelVersion(kernelVersion)).isNotEqualByComparingTo(v(4, 0));
    }

    @Test
    void parseAllKernelVersions() {
        assertThatCode(() -> KernelVersion.VERSIONS.forEach(KernelVersionArgumentsProvider::fromKernelVersion))
                .doesNotThrowAnyException();
    }

    @Test
    void versionParsing() {
        assertThat(fromString("5.0")).isEqualTo(v(5, 0));
        assertThat(fromString("5_0")).isEqualTo(v(5, 0));
        assertThat(fromString("V5.0")).isEqualTo(v(5, 0));
        assertThat(fromString("V5_0")).isEqualTo(v(5, 0));
        assertThat(fromString("v5.0")).isEqualTo(v(5, 0));
        assertThat(fromString("v5_0")).isEqualTo(v(5, 0));

        // Accept major only
        assertThat(fromString("4")).isEqualTo(v(4, null));
        assertThat(fromString("V4")).isEqualTo(v(4, null));
        assertThat(fromString("v4")).isEqualTo(v(4, null));

        // Ignore patch versions
        assertThat(fromString("3.0.10")).isEqualTo(v(3, 0));
        assertThat(fromString("3.2.5-beta")).isEqualTo(v(3, 2));

        // Don't allow characters
        assertThatThrownBy(() -> fromString("v")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fromString("5.3b")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void comparable() {
        assertThat(v(5, 0)).isGreaterThan(v(4, Integer.MAX_VALUE));
        assertThat(v(5, null)).isGreaterThan(v(4, Integer.MAX_VALUE));
        assertThat(v(5, 0)).isLessThan(v(5, 1));

        var v5Wildcard = v(5, null);
        var v51 = v(5, 1);
        assertThat(v51).isEqualByComparingTo(v5Wildcard);
        assertThat(v5Wildcard).isEqualByComparingTo(v51);
    }

    private static KernelVersionArgumentsProvider.Version v(Integer major, Integer minor) {
        return new KernelVersionArgumentsProvider.Version(major, minor);
    }
}
