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
package org.neo4j.commandline.dbms;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.logging.InternalLog;

/**
 * As of Neo4j 5.0 apoc settings should be in a separate file. Here we try to add any we find in neo4j.conf
 * to a separate apoc.conf.
 */
class ApocSettingsMigrator {
    private final Map<String, String> rawConfig;
    private final List<String> apocKeys;
    private final PrintStream out;
    private final Path destinationConfigFile;

    ApocSettingsMigrator(
            PrintStream out, Path destinationConfigFile, Map<String, String> rawConfig, List<String> apocKeys) {
        this.out = out;
        this.destinationConfigFile = destinationConfigFile;
        this.rawConfig = rawConfig;
        this.apocKeys = apocKeys;
    }

    static boolean isApocSetting(String keyString) {
        return keyString.startsWith(Config.APOC_NAMESPACE);
    }

    public void migrate() throws IOException {
        Path apocConfig = destinationConfigFile.resolveSibling("apoc.conf");
        preserveOriginal(apocConfig);

        StringBuilder sb = new StringBuilder();
        for (String key : apocKeys) {
            String value = rawConfig.get(key);
            if (value != null) { // Should always be the case since we found the keys while building this map
                sb.append(key);
                sb.append('=');
                sb.append(value);
                sb.append(System.lineSeparator());
            }
        }

        Files.writeString(apocConfig, sb.toString());
        out.println("APOC settings moved to separate file: " + apocConfig);
    }

    private void preserveOriginal(Path configFile) throws IOException {
        if (Files.exists(configFile)) {
            Path preservedFilePath = configFile.getParent().resolve(configFile.getFileName() + ".old");
            out.println("Keeping original " + configFile.getFileName() + " file at: " + preservedFilePath);
            Files.move(configFile, preservedFilePath);
        }
    }

    // Special migrator that just removes all apoc settings.
    // Not service provided as we don't want it to run any time except for this migration.
    static class ApocSettingRemover implements SettingMigrator {
        static ApocSettingRemover INSTANCE = new ApocSettingRemover();

        private ApocSettingRemover() {}

        @Override
        public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
            List<String> apocSettings = values.keySet().stream()
                    .filter(ApocSettingsMigrator::isApocSetting)
                    .toList();
            for (String apocSetting : apocSettings) {
                values.remove(apocSetting);
            }
        }
    }
}
