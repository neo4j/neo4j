/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.graphdb.config;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This interface represents a setting group. One example can be group defined by a common prefix, such as
 * `dbms.connector.*`. The important aspect is that config keys can only be known after a config has been parsed.
 *
 * @deprecated The settings API will be completely rewritten in 4.0
 */
@Deprecated
public interface SettingGroup<T> extends SettingValidator
{
    /**
     * Apply this setting group to the config and return all of its configured keys and their corresponding values.
     *
     * @param validConfig which can be examined.
     * @return the map of this group's configured keys and values.
     */
    Map<String,T> values( Map<String,String> validConfig );

    /**
     * This will return a list of all settings beloning to this group based on the settings in {@code params}
     * @param params a map of all settings
     * @return a list of the settings this group contains.
     */
    List<Setting<T>> settings( Map<String,String> params );

    /**
     * @return {@code true} if this setting is deprecated, false otherwise.
     */
    boolean deprecated();

    /**
     * @return the key of the setting which replaces this when its deprecated, empty if not deprecated.
     */
    Optional<String> replacement();

    /**
     * @return {@code true} if internal setting, false otherwise.
     */
    boolean internal();

    /**
     * @return {@code true} if secret setting (should be hidden), false otherwise.
     */
    default boolean secret()
    {
        return false;
    }

    /**
     * @return the documented default value if it needs special documentation, empty if default value is good as is.
     */
    Optional<String> documentedDefaultValue();

    /**
     * @return description of which values are good
     */
    String valueDescription();

    /**
     * @return description of setting, empty in case no description exists.
     */
    Optional<String> description();

    /**
     * @return {@code true} if the setting can be changed at runtime.
     */
    default boolean dynamic()
    {
        return false;
    }
}
