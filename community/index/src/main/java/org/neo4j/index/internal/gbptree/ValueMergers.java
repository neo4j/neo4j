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
package org.neo4j.index.internal.gbptree;

/**
 * Common {@link ValueMerger} implementations.
 */
public class ValueMergers
{
    private static final ValueMerger OVERWRITE = ( existingKey, newKey, existingValue, newValue ) -> newValue;

    private static final ValueMerger KEEP_EXISTING = ( existingKey, newKey, existingValue, newValue ) -> null;

    private ValueMergers()
    {
    }

    /**
     * @return {@link ValueMerger} which overwrites value for existing key when inserting.
     * This merger guarantees unique keys in index.
     */
    @SuppressWarnings( "unchecked" )
    public static <KEY,VALUE> ValueMerger<KEY,VALUE> overwrite()
    {
        return OVERWRITE;
    }

    /**
     * @return {@link ValueMerger} which keeps existing key/value otherwise adds new key/value pair.
     * This merger guarantees unique keys in index.
     */
    @SuppressWarnings( "unchecked" )
    public static <KEY,VALUE> ValueMerger<KEY,VALUE> keepExisting()
    {
        return KEEP_EXISTING;
    }
}
