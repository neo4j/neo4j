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
package org.neo4j.kernel.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.internal.Version.CUSTOM_VERSION_SETTING;

import org.junit.jupiter.api.Test;

class VersionTest {
    @Test
    void shouldExposeCleanAndDetailedVersions() {
        assertThat(version("1.2.3-M01,abcdef012345").getReleaseVersion()).isEqualTo("1.2.3-M01");
        assertThat(version("1.2.3-M01,abcdef012345").getVersion()).isEqualTo("1.2.3-M01,abcdef012345");
        assertThat(version("1.2.3-M01,abcdef012345-dirty").getVersion()).isEqualTo("1.2.3-M01,abcdef012345-dirty");

        assertThat(version("1.2.3,abcdef012345").getReleaseVersion()).isEqualTo("1.2.3");
        assertThat(version("1.2.3,abcdef012345").getVersion()).isEqualTo("1.2.3,abcdef012345");
        assertThat(version("1.2.3,abcdef012345-dirty").getVersion()).isEqualTo("1.2.3,abcdef012345-dirty");

        assertThat(version("1.2.3-GA,abcdef012345").getReleaseVersion()).isEqualTo("1.2.3-GA");
        assertThat(version("1.2.3-GA,abcdef012345").getVersion()).isEqualTo("1.2.3-GA,abcdef012345");
        assertThat(version("1.2.3-GA,abcdef012345-dirty").getVersion()).isEqualTo("1.2.3-GA,abcdef012345-dirty");

        assertThat(version("1.2.3M01,abcdef012345").getReleaseVersion()).isEqualTo("1.2.3M01");
        assertThat(version("1.2.3M01,abcdef012345").getVersion()).isEqualTo("1.2.3M01,abcdef012345");
        assertThat(version("1.2.3M01,abcdef012345-dirty").getVersion()).isEqualTo("1.2.3M01,abcdef012345-dirty");

        assertThat(version("1.2").getReleaseVersion()).isEqualTo("1.2");
        assertThat(version("1.2").getVersion()).isEqualTo("1.2");

        assertThat(version("0").getReleaseVersion()).isEqualTo("0");
        assertThat(version("0").getVersion()).isEqualTo("0");
    }

    @Test
    void versionWithCustomString() {
        var planetExpress = "planetExpress";
        var planetExpressVersion = version(planetExpress);
        assertThat(planetExpressVersion.getVersion()).isEqualTo(planetExpress);
        assertThat(planetExpressVersion.getReleaseVersion()).isEqualTo(planetExpress);
    }

    @Test
    void versionStringSelection() {
        var planetExpress = "planetExpress";
        try {
            System.setProperty(CUSTOM_VERSION_SETTING, planetExpress);
            assertEquals(planetExpress, Version.selectVersion());
        } finally {
            System.clearProperty(CUSTOM_VERSION_SETTING);
        }
    }

    private static Version version(String version) {
        return new Version("test-component", version);
    }
}
