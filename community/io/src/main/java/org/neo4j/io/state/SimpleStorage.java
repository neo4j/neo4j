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
package org.neo4j.io.state;

import java.io.IOException;

/**
 * Instances of SimpleStorage provide a mechanism for reading/writing simple durable pieces of state to and from individual files. State is simple if it is
 * largely static and does not need to be recovered.
 *
 * @param <T> Type of state
 */
public interface SimpleStorage<T> extends StateStorage<T>
{
    T readState() throws IOException;

    void removeState() throws IOException;

    @Override
    default T getInitialState()
    {
        return null;
    }
}
