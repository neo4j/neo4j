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
package org.neo4j.function;

/**
 * An action that takes no parameters and returns no values, but may have a side-effect and may throw an exception.
 *
 * @param <E> The type of exception this action may throw.
 */
public interface ThrowingAction<E extends Exception>
{
    /**
     * Apply the action for some or all of its side-effects to take place, possibly throwing an exception.
     *
     * @throws E the exception that performing this action may throw.
     */
    void apply() throws E;

    static <E extends Exception> ThrowingAction<E> noop()
    {
        return () ->
        {
        };
    }
}
