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
package org.neo4j.server.configuration;

import java.nio.file.Path;
import java.util.Map;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.server.WebContainerTestUtils;

public class ConfigFileBuilder {
    private final Path directory;
    private final Map<String, String> config;

    public static ConfigFileBuilder builder(Path directory) {
        return new ConfigFileBuilder(directory);
    }

    private ConfigFileBuilder(Path directory) {
        this.directory = directory;

        // initialize config with defaults that doesn't pollute
        // workspace with generated data
        this.config = MapUtil.stringMap(
                GraphDatabaseSettings.data_directory.name(),
                directory.toAbsolutePath().toString() + "/data",
                ServerSettings.db_api_path.name(),
                "http://localhost:7474/db/data/");
    }

    public Path build() {
        Path path = directory.resolve("config");
        WebContainerTestUtils.writeConfigToFile(config, path);
        return path;
    }

    public ConfigFileBuilder withNameValue(String name, String value) {
        config.put(name, value);
        return this;
    }

    public ConfigFileBuilder withSetting(Setting<?> setting, String value) {
        config.put(setting.name(), value);
        return this;
    }

    public ConfigFileBuilder withoutSetting(Setting<?> setting) {
        config.remove(setting.name());
        return this;
    }
}
