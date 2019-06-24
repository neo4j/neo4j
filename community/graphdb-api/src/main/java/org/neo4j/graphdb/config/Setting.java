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

import org.neo4j.annotations.api.PublicApi;

/**
 * Settings that can be provided in configurations are represented by instances of this interface.
 *
 * @param <T> The type of the values associated with this setting.
 */
@PublicApi
public interface Setting<T>
{
    /**
     * The full (unique) name, identifying a specific setting.
     *
     * @return the name.
     */
    String name();

    /**
     * The default value of this setting
     *
     * @return the typed default value.
     */
    T defaultValue();

    /**
     * A dynamic setting have its value changed in a config at any time
     *
     * @return true if the setting is dynamic, false otherwise
     */
    boolean dynamic();

    /**
     * An internal setting should not be accessed nor altered by any user
     * Internal settings may be changed or removed between versions without notice
     *
     * @return true if the setting is internal, false otherwise
     */
    boolean internal();

    /**
     * A textual representation describing the usage if this setting
     *
     * @return the description of this setting
     */
    String description();
}
