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
package org.neo4j.dbms.api;

import java.nio.file.Path;
import java.util.Map;
import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.logging.LogProvider;

/**
 * Creates a {@link DatabaseManagementService} with Community Edition features.
 */
@PublicApi
public final class DatabaseManagementServiceBuilder implements Neo4jDatabaseManagementServiceBuilder {
    private final Neo4jDatabaseManagementServiceBuilder implementation;

    public DatabaseManagementServiceBuilder(Path homeDirectory) {
        implementation = new DatabaseManagementServiceBuilderImplementation(homeDirectory, extension -> true);
    }

    @Override
    public DatabaseManagementService build() {
        return implementation.build();
    }

    @Override
    public DatabaseManagementServiceBuilder addDatabaseListener(DatabaseEventListener databaseEventListener) {
        implementation.addDatabaseListener(databaseEventListener);
        return this;
    }

    @Override
    public DatabaseManagementServiceBuilder setUserLogProvider(LogProvider userLogProvider) {
        implementation.setUserLogProvider(userLogProvider);
        return this;
    }

    @Override
    public <T> DatabaseManagementServiceBuilder setConfig(Setting<T> setting, T value) {
        implementation.setConfig(setting, value);
        return this;
    }

    @Override
    public DatabaseManagementServiceBuilder setConfig(Map<Setting<?>, Object> config) {
        implementation.setConfig(config);
        return this;
    }

    @Override
    public DatabaseManagementServiceBuilder loadPropertiesFromFile(Path path) {
        implementation.loadPropertiesFromFile(path);
        return this;
    }
}
