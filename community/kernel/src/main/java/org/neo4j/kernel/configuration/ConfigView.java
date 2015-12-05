/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.configuration;

import org.neo4j.graphdb.config.Setting;

/**
 * Provide the basic operation that one could perform on a set of configurations.
 */
public interface ConfigView
{
    /**
     * Retrieve the value of a configuration property from this {@link ConfigView}.
     *
     * @param setting The configuration property
     * @param <T> The type of the configuration property
     * @return The value of the configuration property if the property is found, otherwise, return the default value
     * of the given property.
     */
    <T> T get( Setting<T> setting );
}
