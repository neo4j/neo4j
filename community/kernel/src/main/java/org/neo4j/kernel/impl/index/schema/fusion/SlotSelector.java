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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.Arrays;
import java.util.function.Function;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.values.storable.ValueGroup;

/**
 * Given a set of values selects a slot to use.
 */
interface SlotSelector
{
    int INSTANCE_COUNT = 5;

    int UNKNOWN = -1;
    int STRING = 0;
    int NUMBER = 1;
    int SPATIAL = 2;
    int TEMPORAL = 3;
    int LUCENE = 4;

    void validateSatisfied( IndexProvider[] instances );

    /**
     * Selects a slot to use based on the given values. The values can be anything that can yield a {@link ValueGroup value group},
     * which is what the {@code groupOf} function extracts from each value.
     *
     * @param values values, something which can yield a {@link ValueGroup}.
     * @param groupOf {@link Function} to get {@link ValueGroup} for the given values.
     * @param <V> type of value to extract {@link ValueGroup} from.
     * @return a slot number, or {@link #UNKNOWN} if no single slot could be selected. This typically means that all slots are needed.
     */
    <V> int selectSlot( V[] values, Function<V,ValueGroup> groupOf );

    /**
     * Standard utility method for typical implementation of {@link SlotSelector#validateSatisfied(IndexProvider[])}.
     *
     * @param instances instances to validate.
     * @param aliveIndex slots to ensure have been initialized with non-empty instances.
     */
    static void validateSelectorInstances( Object[] instances, int... aliveIndex )
    {
        for ( int i = 0; i < instances.length; i++ )
        {
            boolean expected = PrimitiveIntCollections.contains( aliveIndex, i );
            boolean actual = instances[i] != IndexProvider.EMPTY;
            if ( expected != actual )
            {
                throw new IllegalArgumentException(
                        String.format( "Only indexes expected to be separated from IndexProvider.EMPTY are %s but was %s",
                                Arrays.toString( aliveIndex ), Arrays.toString( instances ) ) );
            }
        }
    }
}
