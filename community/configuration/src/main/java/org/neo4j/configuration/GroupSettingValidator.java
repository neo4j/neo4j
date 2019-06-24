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
package org.neo4j.configuration;

import java.util.Map;

import org.neo4j.graphdb.config.Setting;

/**
 * Validates a set of {@link Setting} value pairs from a specific {@link Config}
 * The validated settings all share the same name prefix
 */
public interface GroupSettingValidator
{
    /**
     * The prefix used filter the Settings to validate
     * @return the prefix
     */
    String getPrefix();

    /**
     * A textual representation of what's validated
     *
     * @return the description
     */
    String getDescription();

    /**
     * Validates a set of Setting value pairs, called when the Config is created
     *
     * @param values the Setting value pairs that match the prefix, and to be validated
     * @param config the associated Config
     * @throws IllegalArgumentException if the values are not valid
     */
    void validate( Map<Setting<?>,Object> values, Config config );
}
