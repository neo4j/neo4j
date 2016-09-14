/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.function.Function;

/**
 * Settings that can be provided in configurations are represented by instances of this interface, and are available
 * as static fields in various *Settings classes.
 *
 * This interface is available only for use, not for implementing. Implementing this interface is not expected, and
 * backwards compatibility is not guaranteed for implementors.
 *
 * @param <T> type of value this setting will parse input string into and return.
 */
public interface Setting<T> extends Function<Function<String,String>,T>
{
    /**
     * Get the name of the setting. This typically corresponds to a key in a properties file, or similar.
     *
     * @return the name
     */
    String name();

    /**
     * Make this setting bound to a scope
     *
     * @param scopingRule The scoping rule to be applied to this setting
     */
    void withScope( Function<String, String> scopingRule );

    /**
     * Get the default value of this setting, as a string.
     *
     * @return the default value
     */
    String getDefaultValue();

    T from( Configuration config );
}
