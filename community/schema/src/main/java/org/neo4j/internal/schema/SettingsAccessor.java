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

import java.util.Map;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.map.mutable.MapAdapter;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingImpl;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

public interface SettingsAccessor {
    <T extends AnyValue> T get(IndexSetting setting);

    ImmutableSet<String> settingNames();

    default ImmutableSet<IndexSetting> settings() {
        return settingNames().asLazy().collect(INDEX_SETTING_LOOKUP::get).toImmutableSet();
    }

    class IndexSettingObjectMapAccessor implements SettingsAccessor {
        private final MapIterable<IndexSetting, Object> settings;

        public IndexSettingObjectMapAccessor(Map<IndexSetting, Object> settings) {
            this.settings = MapAdapter.adapt(settings);
        }

        @Override
        public <T extends AnyValue> T get(IndexSetting setting) {
            return (T) Values.of(settings.get(setting));
        }

        @Override
        public ImmutableSet<String> settingNames() {
            return settings.keysView()
                    .asLazy()
                    .collect(IndexSetting::getSettingName)
                    .toImmutableSet();
        }
    }

    class IndexConfigAccessor implements SettingsAccessor {
        private final IndexConfig config;

        public IndexConfigAccessor(IndexConfig config) {
            this.config = config;
        }

        @Override
        public <T extends AnyValue> T get(IndexSetting setting) {
            return (T) config.get(setting.getSettingName());
        }

        @Override
        public ImmutableSet<String> settingNames() {
            return config.entries().asLazy().collect(Pair::getOne).toImmutableSet();
        }
    }

    class MapValueAccessor implements SettingsAccessor {
        private final MapValue map;

        public MapValueAccessor(MapValue map) {
            this.map = map;
        }

        @Override
        public <T extends AnyValue> T get(IndexSetting setting) {
            return (T) map.get(setting.getSettingName());
        }

        @Override
        public ImmutableSet<String> settingNames() {
            return Sets.immutable.ofAll(map.keySet());
        }
    }

    ImmutableMap<String, IndexSetting> INDEX_SETTING_LOOKUP = Lists.mutable
            .of(IndexSettingImpl.values())
            .toImmutableMap(IndexSetting::getSettingName, setting -> setting);
}
