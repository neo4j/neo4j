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

import org.neo4j.graphdb.config.Setting;

/**
 * Provides methods to construct new settings. Setting are service loaded from all classes the implements the {@link SettingsDeclaration} interface.
 */
public interface SettingBuilder<T> {
    /**
     * Start construction of a new setting.
     *
     * @param name Full name of the setting, e.g. 'db.logs.query.enabled'.
     * @param parser A parser for converting a string representation into the setting type.
     * @param defaultValue Default value to use if no user specific value is provided.
     * @param <T> type of the setting.
     *
     * @return A new builder, call {@link #build()} to complete.
     */
    static <T> SettingBuilder<T> newBuilder(String name, SettingValueParser<T> parser, T defaultValue) {
        return new SettingImpl.Builder<>(name, parser, defaultValue);
    }

    /**
     * Make this setting dynamic. A dynamic setting can be modified by the user through the 'dbms.setConfigValue' procedure.
     *
     * @return The builder.
     */
    SettingBuilder<T> dynamic();

    /**
     * Make this setting immutable. An immutable setting can only be set during the construction of a configuration object.
     *
     * @return The builder.
     */
    SettingBuilder<T> immutable();

    /**
     * Make this setting internal.
     *
     * @return The builder.
     */
    SettingBuilder<T> internal();

    /**
     * Set the parent setting. The parent setting must be immutable. The value from the parent will be available
     * in the {@link SettingValueParser#solveDependency(Object, Object)} callback.
     *
     * @param setting The parent setting.
     *
     * @return The builder.
     */
    SettingBuilder<T> setDependency(Setting<T> setting);

    /**
     * Add a constraint to the setting value. You can define multiple.
     * @param constraint A constraint to apply to the setting value.
     * @return The builder.
     */
    SettingBuilder<T> addConstraint(SettingConstraint<T> constraint);

    /**
     * Finalize the construction of the setting.
     * @return The setting.
     */
    Setting<T> build();
}
