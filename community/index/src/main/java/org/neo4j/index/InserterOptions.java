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
package org.neo4j.index;

public interface InserterOptions
{
    /**
     * Decides relatively where a split happens, i.e. which position will be the split key.
     * Keys (and its values/children) including the split key will go to the right tree node,
     * everything before it goes into the left.
     *
     * @return a factor between 0..1 where 0 means far to the left, 1 means far to the right and
     * as an example 0.5 will select the middle item (floor division).
     */
    float splitLeftChildSize();

    class Defaults implements InserterOptions
    {
        @Override
        public float splitLeftChildSize()
        {
            return 0.5f;
        }
    }

    /**
     * Default options best suitable for most occasions.
     */
    InserterOptions DEFAULTS = new Defaults();

    /**
     * Options best suitable in batching scenarios, where insertions come in sequentially (by order of key)
     * and are typically densely packed.
     */
    InserterOptions BATCHING_SEQUENTIAL = new Defaults()
    {
        @Override
        public float splitLeftChildSize()
        {
            return 1f;
        }
    };
}
