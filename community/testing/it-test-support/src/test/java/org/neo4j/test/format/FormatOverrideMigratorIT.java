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
package org.neo4j.test.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.system_database_format;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.test.format.FormatOverrideMigrator.OVERRIDE_STORE_FORMAT_KEY;
import static org.neo4j.test.format.FormatOverrideMigrator.OVERRIDE_SYSTEM_STORE_FORMAT_KEY;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.util.ClearSystemProperty;
import org.junit.jupiter.api.util.SetSystemProperty;
import org.neo4j.configuration.Config;
import org.neo4j.test.extension.RequireAlignedFormat;

@Isolated
class FormatOverrideMigratorIT {
    @Test
    @RequireAlignedFormat
    @ClearSystemProperty(key = OVERRIDE_STORE_FORMAT_KEY)
    @ClearSystemProperty(key = OVERRIDE_SYSTEM_STORE_FORMAT_KEY)
    void noOverridesKeepDeclaredDefaults() {
        Config config = Config.defaults();

        assertThat(config.get(db_format)).isEqualTo(db_format.defaultValue());
        assertThat(config.get(system_database_format)).isEqualTo(system_database_format.defaultValue());
    }

    @Test
    @SetSystemProperty(key = OVERRIDE_STORE_FORMAT_KEY, value = "standard")
    @SetSystemProperty(key = OVERRIDE_SYSTEM_STORE_FORMAT_KEY, value = "multiversion_block")
    void overridesReplaceDefaultsOfMatchingSettings() {
        Config config = Config.defaults();

        assertThat(config.get(db_format)).isEqualTo("standard");
        assertThat(config.get(system_database_format)).isEqualTo("multiversion_block");
        assertThat(config.isExplicitlySet(db_format)).isFalse();
        assertThat(config.isExplicitlySet(system_database_format)).isFalse();
    }

    @Test
    @SetSystemProperty(key = OVERRIDE_STORE_FORMAT_KEY, value = "standard")
    @SetSystemProperty(key = OVERRIDE_SYSTEM_STORE_FORMAT_KEY, value = "standard")
    void explicitlySetValuesWinOverOverrides() {
        Config config = Config.newBuilder()
                .set(db_format, "block")
                .set(system_database_format, "block")
                .build();

        assertThat(config.get(db_format)).isEqualTo("block");
        assertThat(config.get(system_database_format)).isEqualTo("block");
    }

    @Test
    @SetSystemProperty(key = OVERRIDE_STORE_FORMAT_KEY, value = "standard")
    @SetSystemProperty(key = OVERRIDE_SYSTEM_STORE_FORMAT_KEY, value = "standard")
    void explicitlySetRawValuesWinOverOverrides() {
        Config config = Config.newBuilder()
                .setRaw(Map.of(db_format.name(), "block", system_database_format.name(), "block"))
                .build();

        assertThat(config.get(db_format)).isEqualTo("block");
        assertThat(config.get(system_database_format)).isEqualTo("block");
    }
}
