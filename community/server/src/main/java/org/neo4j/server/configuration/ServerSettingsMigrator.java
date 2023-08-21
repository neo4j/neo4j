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

import static org.neo4j.configuration.SettingMigrators.migrateSettingNameChange;
import static org.neo4j.configuration.SettingMigrators.migrateSettingRemoval;
import static org.neo4j.server.configuration.ServerSettings.allow_telemetry;
import static org.neo4j.server.configuration.ServerSettings.http_auth_allowlist;
import static org.neo4j.server.configuration.ServerSettings.http_enabled_modules;
import static org.neo4j.server.configuration.ServerSettings.third_party_packages;
import static org.neo4j.server.configuration.ServerSettings.webserver_max_threads;

import java.util.Map;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.logging.InternalLog;

@ServiceProvider
public class ServerSettingsMigrator implements SettingMigrator {
    @Override
    public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
        migrateSettingNameChange(values, log, "dbms.security.http_auth_whitelist", http_auth_allowlist);
        migrateSettingNameChange(values, log, "dbms.unmanaged_extension_classes", third_party_packages);
        migrateSettingNameChange(values, log, "dbms.threads.worker_count", webserver_max_threads);
        migrateSettingNameChange(values, log, "dbms.http_enabled_modules", http_enabled_modules);
        migrateSettingNameChange(values, log, "clients.allow_telemetry", allow_telemetry);

        migrateSettingRemoval(values, log, "dbms.rest.transaction.idle_timeout", "It no longer has any effect");
    }
}
