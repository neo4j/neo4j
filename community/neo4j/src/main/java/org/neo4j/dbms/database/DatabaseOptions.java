/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.database;

import java.util.HashMap;
import java.util.Map;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;

public record DatabaseOptions(Map<Setting<?>, Object> settings, TopologyGraphDbmsModel.HostedOnMode mode) {
    public static final DatabaseOptions RAFT = empty(TopologyGraphDbmsModel.HostedOnMode.raft);
    public static final DatabaseOptions SINGLE = empty(TopologyGraphDbmsModel.HostedOnMode.single);

    public static DatabaseOptions empty(TopologyGraphDbmsModel.HostedOnMode mode) {
        return new DatabaseOptions(new HashMap<>(), mode);
    }

    public static DatabaseOptions fromProperties(
            Map<String, Object> allProperties, TopologyGraphDbmsModel.HostedOnMode mode) {
        var storageEngineName = (String) allProperties.get(TopologyGraphDbmsModel.DATABASE_STORAGE_ENGINE_PROPERTY);
        var storeFormat = (String) allProperties.get(TopologyGraphDbmsModel.DATABASE_STORE_FORMAT_NEW_DB_PROPERTY);

        Map<Setting<?>, Object> settings = new HashMap<>();

        if (storageEngineName != null) {
            settings.put(GraphDatabaseInternalSettings.storage_engine, storageEngineName);
        }
        if (storeFormat != null) {
            settings.put(
                    GraphDatabaseSettings.record_format_created_db,
                    GraphDatabaseSettings.DatabaseRecordFormat.valueOf(storeFormat));
        }

        return new DatabaseOptions(settings, mode);
    }
}
