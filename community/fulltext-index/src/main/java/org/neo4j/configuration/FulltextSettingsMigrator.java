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

import static org.neo4j.configuration.FulltextSettings.eventually_consistent;
import static org.neo4j.configuration.FulltextSettings.eventually_consistent_index_update_queue_max_length;
import static org.neo4j.configuration.FulltextSettings.fulltext_default_analyzer;
import static org.neo4j.configuration.SettingMigrators.migrateSettingNameChange;

import java.util.Map;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.logging.InternalLog;

@ServiceProvider
public class FulltextSettingsMigrator implements SettingMigrator {
    @Override
    public void migrate(Map<String, String> values, Map<String, String> defaultValues, InternalLog log) {
        migrateSettingNameChange(values, log, "dbms.index.fulltext.default_analyzer", fulltext_default_analyzer);
        migrateSettingNameChange(values, log, "dbms.index.fulltext.eventually_consistent", eventually_consistent);
        migrateSettingNameChange(
                values,
                log,
                "dbms.index.fulltext.eventually_consistent_index_update_queue_max_length",
                eventually_consistent_index_update_queue_max_length);
    }
}
