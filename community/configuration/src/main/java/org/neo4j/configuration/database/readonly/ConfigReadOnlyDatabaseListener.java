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
package org.neo4j.configuration.database.readonly;

import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;

import java.util.Set;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingChangeListener;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public final class ConfigReadOnlyDatabaseListener extends LifecycleAdapter {
    private final ReadOnlyDatabases readOnlyDatabases;
    private final Config config;

    private SettingChangeListener<Boolean> readOnlyDatabaseDefault;
    private SettingChangeListener<Set<String>> readOnlyDatabasesList;
    private SettingChangeListener<Set<String>> writableDatabasesList;

    public ConfigReadOnlyDatabaseListener(ReadOnlyDatabases readOnlyDatabases, Config config) {
        this.readOnlyDatabases = readOnlyDatabases;
        this.config = config;
    }

    private <T> SettingChangeListener<T> getConfigChangeListener(Setting<T> ignored) {
        return (oldValue, newValue) -> readOnlyDatabases.refresh();
    }

    private <T> SettingChangeListener<T> addConfigListener(Setting<T> setting) {
        var changeListener = getConfigChangeListener(setting);
        config.addListener(setting, changeListener);
        return changeListener;
    }

    private <T> void removeConfigListener(Setting<T> setting, SettingChangeListener<T> changeListener) {
        if (changeListener != null) {
            config.removeListener(setting, changeListener);
        }
    }

    @Override
    public void init() throws Exception {
        readOnlyDatabaseDefault = addConfigListener(read_only_database_default);
        readOnlyDatabasesList = addConfigListener(read_only_databases);
        writableDatabasesList = addConfigListener(writable_databases);
        readOnlyDatabases.refresh();
    }

    @Override
    public void shutdown() throws Exception {
        removeConfigListener(read_only_database_default, readOnlyDatabaseDefault);
        removeConfigListener(read_only_databases, readOnlyDatabasesList);
        removeConfigListener(writable_databases, writableDatabasesList);
    }
}
