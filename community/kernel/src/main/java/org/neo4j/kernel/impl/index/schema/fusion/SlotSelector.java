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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.Arrays;
import java.util.function.Function;

import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.values.storable.ValueGroup;

import static org.apache.commons.lang3.ArrayUtils.contains;

/**
 * Given a set of values selects a slot to use.
 */
public interface SlotSelector
{
    SlotSelector nullInstance = new NullInstance();

    void validateSatisfied( InstanceSelector<IndexProvider> instances );

    /**
     * Selects a slot to use based on the given values. The values can be anything that can yield a {@link ValueGroup value group},
     * which is what the {@code groupOf} function extracts from each value.
     *
     * @param <V> type of value to extract {@link ValueGroup} from.
     * @param values values, something which can yield a {@link ValueGroup}.
     * @param groupOf {@link Function} to get {@link ValueGroup} for the given values.
     * @return {@link IndexSlot} or {@code null} if no single slot could be selected. This means that all slots are needed.
     */
    <V> IndexSlot selectSlot( V[] values, Function<V,ValueGroup> groupOf );

    /**
     * Standard utility method for typical implementation of {@link SlotSelector#validateSatisfied(InstanceSelector)}.
     *
     * @param instances instances to validate.
     * @param aliveIndex slots to ensure have been initialized with non-empty instances.
     */
    static void validateSelectorInstances( InstanceSelector<IndexProvider> instances, IndexSlot... aliveIndex )
    {
        for ( IndexSlot indexSlot : IndexSlot.values() )
        {
            boolean expected = contains( aliveIndex, indexSlot );
            boolean actual = instances.select( indexSlot ) != IndexProvider.EMPTY;
            if ( expected != actual )
            {
                throw new IllegalArgumentException(
                        String.format( "Only indexes expected to be separated from IndexProvider.EMPTY are %s but was %s",
                                Arrays.toString( aliveIndex ), instances ) );
            }
        }
    }

    class NullInstance implements SlotSelector
    {
        @Override
        public void validateSatisfied( InstanceSelector<IndexProvider> instances )
        {   // no-op
        }

        @Override
        public <V> IndexSlot selectSlot( V[] values, Function<V,ValueGroup> groupOf )
        {
            throw new UnsupportedOperationException( "NullInstance cannot select a slot for you. Please use the real deal." );
        }
    }
}
