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
 * Handling the values associated with a {@link Setting} object.
 *
 * @param <T> the type of the object handled by a specific parser.
 */
public interface SettingValueParser<T> {
    /**
     * Parse a textual representation of a value into a typed object.
     *
     * @param value The textual representation of the value.
     * @throws IllegalArgumentException if the text representation can not be parsed into an object of type {@code T}.
     * @return The parsed value.
     */
    T parse(String value);

    /**
     * Validate a value.
     *
     * @param value The value to be validated.
     * @throws IllegalArgumentException if the value is not accepted by the parser.
     */
    default void validate(T value) {}

    /**
     * The description describing the parser.
     *
     * @return The description.
     */
    String getDescription();

    /**
     * String acting as a conjunction when joining the parser description with a constraint description clause.
     *
     * @return String to act as a conjunction.
     */
    default String constraintConjunction() {
        return " that ";
    }

    /**
     *  The type of the object this parser is working on.
     * @return the type of {@code T}.
     */
    Class<T> getType();

    /**
     * Solving a value against the default value.
     *
     * @param value The configured value for the given setting.
     * @param defaultValue The default value for the given setting.
     * @return The solved value.
     */
    default T solveDefault(T value, T defaultValue) {
        return value;
    }

    /**
     * Callback for settings with a dependency, also referred to as a parent setting.
     * The default behaviour is that when the value for the setting is {@code null}, the final value is taken from the parent.
     * If the default behaviour is changed, the {@link #getSolverDescription()} must be changed as well.
     *
     * @param value The configured value for the given setting.
     * @param dependencyValue The configured value from the parent setting.
     * @return The solved value.
     */
    default T solveDependency(T value, T dependencyValue) {
        if (value != null) {
            return value;
        }
        return dependencyValue;
    }

    /**
     * Description of the behaviour in {@link #solveDependency(Object, Object)}. Used to generate documentation.
     *
     * @return A natural language description of the dependency
     */
    default String getSolverDescription() {
        return "If unset, the value is inherited";
    }

    /**
     * Converting an object to a textual representation of that object.
     *
     * @param value The object to be turned in to a textual representation.
     * @return The textual representation.
     */
    default String valueToString(T value) {
        return value.toString();
    }
}
