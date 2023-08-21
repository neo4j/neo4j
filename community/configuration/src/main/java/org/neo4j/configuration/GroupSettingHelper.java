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

public final class GroupSettingHelper {
    private GroupSettingHelper() {}

    /**
     * Helper method when creating settings for this group.
     * This is the preferred method if the group contains multiple settings.
     *
     * @param suffix The unique name of the setting to be created
     * @param parser The parser to be used in the setting
     * @param defaultValue The default value to be associated with the setting
     * @param <T> the type of the objects represented by the setting
     * @return the builder of the setting
     */
    public static <T> SettingBuilder<T> getBuilder(
            String prefix, String name, String suffix, SettingValueParser<T> parser, T defaultValue) {
        return SettingImpl.newBuilder(String.format("%s.%s.%s", prefix, name, suffix), parser, defaultValue);
    }

    /**
     * Helper method when creating settings for this group.
     * This is the preferred method if the group contains only a single setting.
     *
     * @param parser The parser to be used in the setting
     * @param defaultValue The default value to be associated with the setting
     * @param <T> the type of the objects represented by the setting
     * @return the builder of the setting
     */
    public static <T> SettingBuilder<T> getBuilder(
            String prefix, String name, SettingValueParser<T> parser, T defaultValue) {
        return SettingImpl.newBuilder(String.format("%s.%s", prefix, name), parser, defaultValue);
    }
}
