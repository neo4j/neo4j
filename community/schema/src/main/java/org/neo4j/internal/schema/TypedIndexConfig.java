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
package org.neo4j.internal.schema;

import static org.neo4j.internal.schema.IndexConfigUtils.INDEX_SETTING_COMPARATOR;
import static org.neo4j.internal.schema.IndexConfigUtils.unrecognizedSetting;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;
import org.neo4j.values.storable.Value;

public abstract class TypedIndexConfig {
    private final IndexProviderDescriptor descriptor;
    private final Map<IndexSetting, Object> settings;
    private final IndexConfig config;
    private final IndexConfig effectiveFullConfig;
    private final Set<IndexSetting> acceptedSettings;

    /// @param descriptor the [IndexProviderDescriptor] associate with this configuration
    /// @param acceptedSettings the [IndexSetting]s accepted as persisted input
    /// @param records the valid records collected via a read of persisted settings or validation of provided
    // settings
    protected TypedIndexConfig(
            IndexProviderDescriptor descriptor, Set<IndexSetting> acceptedSettings, Iterable<Valid> records) {
        SortedMap<IndexSetting, Object> settings = new TreeMap<>(INDEX_SETTING_COMPARATOR);
        Map<String, Value> allStorables = new HashMap<>();
        Map<String, Value> acceptedStorables = new HashMap<>();
        for (Valid record : records) {
            IndexSetting setting = record.setting();
            settings.put(setting, record.value());
            Value storable = record.storable();
            if (storable != null && storable != NO_VALUE) {
                allStorables.put(setting.getSettingName(), storable);
                if (acceptedSettings.contains(setting)) {
                    acceptedStorables.put(setting.getSettingName(), storable);
                }
            }
        }

        this.descriptor = descriptor;
        this.settings = Collections.unmodifiableSortedMap(settings);
        this.config = IndexConfig.with(acceptedStorables);
        this.effectiveFullConfig = IndexConfig.with(allStorables);
        this.acceptedSettings = Collections.unmodifiableSet(acceptedSettings);
    }

    public IndexProviderDescriptor descriptor() {
        return descriptor;
    }

    /// The [IndexConfig] for the corresponding [IndexProviderDescriptor],
    /// which may not contain all encapsulated [IndexSetting]s
    public IndexConfig config() {
        return config;
    }

    /// An [IndexConfig] that contains all encapsulated [IndexSetting]s
    public IndexConfig effectiveFullConfig() {
        return effectiveFullConfig;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(IndexSetting setting) {
        Object value = settings.get(setting);
        if (value == null && !settings.containsKey(setting)) {
            throw unrecognizedSetting(setting.getSettingName(), settings.keySet());
        }
        return (T) value;
    }

    public Value getValue(IndexSetting setting) {
        if (!acceptedSettings.contains(setting)) {
            throw unrecognizedSetting(setting.getSettingName(), acceptedSettings);
        }
        return config.getOrDefault(setting.getSettingName(), NO_VALUE);
    }
}
