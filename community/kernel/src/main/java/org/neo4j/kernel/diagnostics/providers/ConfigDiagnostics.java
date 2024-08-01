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
package org.neo4j.kernel.diagnostics.providers;

import static java.lang.String.format;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.NamedDiagnosticsProvider;
import org.neo4j.io.fs.FileSystemAbstraction;

public class ConfigDiagnostics extends NamedDiagnosticsProvider {
    private final Config config;

    public ConfigDiagnostics(Config config) {
        super("DBMS config");
        this.config = config;
    }

    @Override
    public void dump(DiagnosticsLogger logger) {
        if (config.getDeclaredSettings().values().stream().noneMatch(config::isExplicitlySet)) {
            logger.log("No provided DBMS settings.");
        } else {
            logger.log("DBMS provided settings:");
            config.getDeclaredSettings().entrySet().stream()
                    .filter(entry -> config.isExplicitlySet(entry.getValue()))
                    .sorted(Map.Entry.comparingByKey())
                    .forEachOrdered(e -> {
                        String value = ((SettingImpl<Object>) e.getValue()).valueToString(config.get(e.getValue()));
                        logger.log(format("%s=%s", e.getKey(), value));
                    });
        }

        logger.log("Directories in use:");
        config.getDeclaredSettings().values().stream()
                .filter(setting -> isImmutablePathSetting(setting, config.get(setting)))
                .sorted(Comparator.comparing(Setting::name))
                .forEachOrdered(setting -> {
                    String value = ((SettingImpl<Object>) setting).valueToString(config.get(setting));
                    logger.log(format("%s=%s", setting.name(), value));
                });
    }

    private static boolean isImmutablePathSetting(Setting<Object> setting, Object value) {
        SettingImpl<Object> settingImpl = (SettingImpl<Object>) setting;
        if (SettingValueParsers.PATH.getType().equals(settingImpl.parser().getType()) && value instanceof Path path) {
            return isDirectory(path) && !settingImpl.internal() && !settingImpl.dynamic();
        }
        return false;
    }

    /**
     * Poor man's check for directory, but good enough for debug.log.
     * Cannot use {@link FileSystemAbstraction#isDirectory(Path)} because path might not exist yet.
     */
    private static boolean isDirectory(Path path) {
        var fileName = path.getFileName();
        return fileName == null || !fileName.toString().contains(".");
    }
}
