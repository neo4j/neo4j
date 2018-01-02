/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.graphdb.config;

import org.neo4j.helpers.Function;

/**
 * Settings that can be provided in configurations are represented by instances of this interface, and are available
 * as static fields in various *Settings classes, such as {@link org.neo4j.graphdb.factory.GraphDatabaseSettings}.
 * Use these with the methods in {@link org.neo4j.graphdb.factory.GraphDatabaseBuilder} to provide your own
 * configuration when constructing new databases.
 *
 * This interface is available only for use, not for implementing. Implementing this interface is not expected, and
 * backwards compatibility is not guaranteed for implementors.
 */
public interface Setting<T>
        extends Function<Function<String, String>, T>
{
    /**
     * Get the name of the setting. This typically corresponds to a key in a properties file, or similar.
     *
     * @return the name
     */
    String name();

    /**
     * Get the default value of this setting, as a string.
     *
     * @return the default value
     */
    String getDefaultValue();
}
