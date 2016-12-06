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
package org.neo4j.index.gbptree;

/**
 * Means of communicating information about splits, caused by insertion, from lower levels of the tree up to parent
 * and potentially all the way up to the root.
 *
 * @param <KEY> type of key.
 */
class StructurePropagation<KEY>
{
    boolean hasNewGen;
    boolean hasSplit;
    final KEY primKey;
    long left;
    long right;

    StructurePropagation( KEY primKey )
    {
        this.primKey = primKey;
    }

    /**
     * Clear booleans indicating change has occurred.
     */
    void clear()
    {
        hasNewGen = false;
        hasSplit = false;
    }
}
