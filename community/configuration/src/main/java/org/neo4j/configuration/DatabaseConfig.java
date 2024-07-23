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
package org.neo4j.configuration;

import static java.util.Collections.emptyMap;

import java.util.Map;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class DatabaseConfig extends LocalConfig implements Lifecycle {
    private final Map<Setting<?>, Object> databaseSpecificSettings;
    private final Config globalConfig;
    private final Map<Setting<?>, Object> overriddenSettings;

    public DatabaseConfig(Config globalConfig) {
        this(emptyMap(), globalConfig);
    }

    public DatabaseConfig(Map<Setting<?>, Object> databaseSpecificSettings, Config globalConfig) {
        super(globalConfig);
        this.databaseSpecificSettings = databaseSpecificSettings;
        this.globalConfig = globalConfig;
        overriddenSettings = null;
    }

    @Override
    public <T> T get(Setting<T> setting) {
        if (overriddenSettings != null) {
            Object o = overriddenSettings.get(setting);
            if (o != null) {
                //noinspection unchecked
                return (T) o;
            }
        }
        Object dbSpecific = databaseSpecificSettings.get(setting);
        if (dbSpecific != null) {
            return (T) dbSpecific;
        }
        return super.get(setting);
    }

    @Override
    public <T> ValueSource getValueSource(Setting<T> setting) {
        boolean overridden = overriddenSettings != null && overriddenSettings.containsKey(setting)
                || databaseSpecificSettings != null && databaseSpecificSettings.containsKey(setting);
        return overridden ? ValueSource.SYSTEM : super.getValueSource(setting);
    }

    @Override
    public void init() {}

    @Override
    public void start() {}

    @Override
    public void stop() throws Exception {}

    @Override
    public void shutdown() {
        removeAllLocalListeners();
    }

    Config getGlobalConfig() {
        return globalConfig;
    }
}
