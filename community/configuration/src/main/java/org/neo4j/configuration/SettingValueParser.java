/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.annotations.api.IgnoreApiCheck;
import org.neo4j.graphdb.config.Setting;

/**
 * Handling the values associated with a {@link Setting} object
 *
 * @param <T> the type of the object handled by a specific parser
 */
@IgnoreApiCheck
public interface SettingValueParser<T>
{
    /**
     * Parsing a textual representation of an object into a typed object
     *
     * @param value The String representation the object to be parsed
     * @throws IllegalArgumentException if the text representation can not be parsed into an object of type T
     * @return the parsed value
     */
    T parse( String value );

    /**
     * Validates if an object is accepted by the parser
     * @param value The object to be validated
     * @throws IllegalArgumentException if the object is not accepted by the parser
     */
    default void validate( T value )
    {
    }
    /**
     * The description describing the parser
     *
     * @return the description
     */
    String getDescription();

    /**
     *  The type of the object this parser is working on.
     * @return the type of T
     */
    Class<T> getType();

    /**
     * Solving a value against the default value
     *
     * @param value the value associated with the Setting using this parser
     * @param defaultValue the value associated with the Setting using this parser
     * @return the solved value
     */

    default T solveDefault( T value, T defaultValue )
    {
        return value;
    }

    /**
     * Solving a value against a value the Setting using this parser is depending on.
     *
     * @param value the value associated with the Setting using this parser
     * @param dependencyValue  the value associated with the Setting that the Setting using this parser is depending on
     * @return
     */
    default T solveDependency( T value, T dependencyValue )
    {
        if ( value != null )
        {
            return value;
        }
        return dependencyValue;
    }

    default String getSolverDescription()
    {
        return "If unset the value is inherited";
    }

    /**
     * Converting an object to a textual representation of that object.
     * @param value the object to be turned in to an textual representation
     * @return the textual representation
     */
    default String valueToString( T value )
    {
        return value.toString();
    }
}
