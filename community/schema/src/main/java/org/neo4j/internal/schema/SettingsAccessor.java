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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingImpl;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

public interface SettingsAccessor {
    boolean containsSetting(IndexSetting setting);

    AnyValue get(IndexSetting setting);

    Set<String> settingNames();

    default Set<IndexSetting> settings() {
        Set<String> settingNames = settingNames();
        Set<IndexSetting> settings = new HashSet<>(settingNames.size());
        for (String settingName : settingNames) {
            settings.add(lookup(settingName));
        }
        return Collections.unmodifiableSet(settings);
    }

    class IndexSettingObjectMapAccessor implements SettingsAccessor {
        private final Map<IndexSetting, Object> settings;

        public IndexSettingObjectMapAccessor(Map<IndexSetting, Object> settings) {
            this.settings = Collections.unmodifiableMap(settings);
        }

        @Override
        public boolean containsSetting(IndexSetting setting) {
            return settings.containsKey(setting);
        }

        @Override
        public AnyValue get(IndexSetting setting) {
            return Values.of(settings.get(setting));
        }

        @Override
        public Set<String> settingNames() {
            Set<String> settingNames = new HashSet<>(settings.size());
            for (IndexSetting setting : settings.keySet()) {
                settingNames.add(setting.getSettingName());
            }
            return Collections.unmodifiableSet(settingNames);
        }
    }

    class IndexConfigAccessor implements SettingsAccessor {
        private final IndexConfig config;

        public IndexConfigAccessor(IndexConfig config) {
            this.config = config;
        }

        @Override
        public boolean containsSetting(IndexSetting setting) {
            return config.asMap().containsKey(setting.getSettingName());
        }

        @Override
        public AnyValue get(IndexSetting setting) {
            return config.getOrDefault(setting.getSettingName(), Values.NO_VALUE);
        }

        @Override
        public Set<String> settingNames() {
            return config.settingNames();
        }
    }

    class MapValueAccessor implements SettingsAccessor {
        private final MapValue map;

        public MapValueAccessor(MapValue map) {
            this.map = map;
        }

        @Override
        public boolean containsSetting(IndexSetting setting) {
            return map.containsKey(setting.getSettingName());
        }

        @Override
        public AnyValue get(IndexSetting setting) {
            return map.get(setting.getSettingName());
        }

        @Override
        public Set<String> settingNames() {
            Set<String> settingNames = new HashSet<>();
            for (String settingName : map.keySet()) {
                settingNames.add(settingName);
            }
            return Collections.unmodifiableSet(settingNames);
        }
    }

    Map<String, IndexSetting> INDEX_SETTING_LOOKUP = Stream.of(IndexSettingImpl.values(), InternalIndexSetting.values())
            .flatMap(Arrays::stream)
            .collect(Collectors.toUnmodifiableMap(IndexSetting::getSettingName, setting -> setting));

    static IndexSetting lookup(String settingName) {
        return INDEX_SETTING_LOOKUP.get(settingName);
    }
}
