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
package org.neo4j.bolt;

import java.util.Map;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingMigrator;
import org.neo4j.logging.InternalLog;

@ServiceProvider
public class BoltSettingsMigrator implements SettingMigrator {

    public static final String SETTING_KEEP_ALIVE_FOR_REQUESTS = "server.bolt.connection_keep_alive_for_requests";
    public static final String SETTING_KEEP_ALIVE_FOR_REQUESTS_STREAMING = "STREAMING";

    @Override
    public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {

        if (SETTING_KEEP_ALIVE_FOR_REQUESTS_STREAMING.equalsIgnoreCase(values.get(SETTING_KEEP_ALIVE_FOR_REQUESTS))) {
            log.warn(
                    "Use of deprecated value %s for setting %s. Support for this value will be removed in a future release.",
                    SETTING_KEEP_ALIVE_FOR_REQUESTS_STREAMING, SETTING_KEEP_ALIVE_FOR_REQUESTS);
        }
    }
}
