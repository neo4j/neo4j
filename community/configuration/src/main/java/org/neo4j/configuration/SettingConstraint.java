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

import java.util.function.Function;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;

/**
 * A constraint limiting the set of accepted values of the associated {@link Setting}.
 * @param <T> the type of the objects this constraint is working on.
 */
public abstract class SettingConstraint<T> {
    private Function<T, String> valueToString = T::toString;

    /**
     * Validates if an object is satisfying the constraint.
     *
     * @param value The object to be checked if it satisfies the constraint.
     * @param config The config the value belongs to.
     * @throws IllegalArgumentException if the constraint is not satisfied.
     */
    public abstract void validate(T value, Configuration config);

    /**
     * A textual representation of the constraint, including information about valid/invalid values
     *
     * @return The description
     */
    public abstract String getDescription();

    /**
     * Get a string representation of the provided value. Used to generate documentation.
     *
     * @param value A value of type {@code T}.
     * @return String representation of the provided value.
     */
    protected String valueToString(T value) {
        return valueToString.apply(value);
    }

    void setParser(SettingValueParser<T> parser) {
        this.valueToString = parser::valueToString;
    }
}
