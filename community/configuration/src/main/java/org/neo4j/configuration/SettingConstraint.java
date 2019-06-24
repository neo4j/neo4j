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

import org.neo4j.graphdb.config.Setting;

/**
 * A constraint limiting the set of accepted values of the associated {@link Setting}
 * @param <T> the type of the objects this constraint is working on
 */

public interface SettingConstraint<T>
{
    /**
     * Validates if an object is satisfying the constraint
     *
     * @param value the object to be checked if it satisfies the constraint
     * @throws IllegalArgumentException if the constraint is not satisfied
     */
    void validate( T value );

    /**
     * A textual representation of the constraint, including information about valid/invalid values
     *
     * @return the description
     */
    String getDescription();
}
