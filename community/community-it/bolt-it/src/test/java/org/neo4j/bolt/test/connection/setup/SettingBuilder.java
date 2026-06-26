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
package org.neo4j.bolt.test.connection.setup;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.neo4j.graphdb.config.Setting;

/**
 * Type safe wrapper for configuration maps.
 */
public final class SettingBuilder {

    private final Map<Setting<?>, Object> settings;

    private SettingBuilder(Map<Setting<?>, Object> settings) {
        this.settings = settings;
    }

    public static SettingBuilder wrap(Map<Setting<?>, Object> settings) {
        return new SettingBuilder(settings);
    }

    public Map<Setting<?>, Object> build() {
        return new HashMap<>(this.settings);
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T> get(Setting<T> key) {
        var value = this.settings.get(key);
        return Optional.ofNullable((T) value);
    }

    public <T> SettingBuilder set(Setting<T> key, T value) {
        this.settings.put(key, value);
        return this;
    }
}
