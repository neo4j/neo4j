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
package org.neo4j.values;

import org.neo4j.values.storable.HashMemoizingScalarValue;
import org.neo4j.values.storable.HashMemoizingValue;

/**
 * AnyValue that caches the hash code so that it doesn't require recomputation.
 * <p>
 * In order for the hierarchy to work out this is a duplicate of {@link HashMemoizingValue} and {@link HashMemoizingScalarValue}
 */
public abstract class HashMemoizingAnyValue extends AnyValue
{
    private int hash;

    @Override
    protected final int computeHash()
    {
        //We will always recompute hashcode for values
        //where `hashCode == 0`, e.g. empty strings and empty lists
        //however that shouldn't be shouldn't be too costly
        if ( hash == 0 )
        {
            hash = computeHashToMemoize();
        }
        return hash;
    }

    protected abstract int computeHashToMemoize();
}
