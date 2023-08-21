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
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.logging.LogProvider;

/**
 * Constructs a {@link DatabaseManagementService}.
 *
 * @see DatabaseManagementService
 * @see DatabaseEventListener
 */
@PublicApi
public interface Neo4jDatabaseManagementServiceBuilder {
    /**
     * Construct the service matching the configuration in this builder.
     *
     * @return a {@link DatabaseManagementService} from the provided configuration.
     */
    DatabaseManagementService build();

    /**
     * Attach an event listener for database lifecycle events. For database specific event, see
     * {@link DatabaseManagementService#registerTransactionEventListener(String, TransactionEventListener)}
     *
     * @param databaseEventListener the event listener to be invoked on events.
     * @return the builder.
     */
    Neo4jDatabaseManagementServiceBuilder addDatabaseListener(DatabaseEventListener databaseEventListener);

    /**
     * Set a specific log provider for the service.
     *
     * @param userLogProvider a log provider that will handle logging of user events/messages.
     * @return the builder.
     */
    Neo4jDatabaseManagementServiceBuilder setUserLogProvider(LogProvider userLogProvider);

    /**
     * Configure a specific setting.
     *
     * @param setting the setting to configure a value for.
     * @param value the value to set the provided setting to. Passing {@code null} will revert it back to the default value.
     * @param <T> the type of the value.
     * @return the builder.
     */
    <T> Neo4jDatabaseManagementServiceBuilder setConfig(Setting<T> setting, T value);

    /**
     * Configure a set of different settings.
     *
     * @param config a map with settings to configure.
     * @return the builder.
     */
    Neo4jDatabaseManagementServiceBuilder setConfig(Map<Setting<?>, Object> config);

    /**
     * Read configuration from a file.
     * <p>
     * The settings will be applied in order and can be used in conjunction with {@link #setConfig(Setting, Object)}. For example, settings
     * configured before calling this method will be overridden if present in the file, and any setting applied after will override the values read before.
     *
     * @param path to a file containing neo4j configuration statements.
     * @return the builder.
     */
    Neo4jDatabaseManagementServiceBuilder loadPropertiesFromFile(Path path);
}
