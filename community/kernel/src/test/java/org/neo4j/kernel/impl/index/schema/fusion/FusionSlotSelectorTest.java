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

import org.junit.jupiter.api.Test;

import java.util.EnumMap;

import org.neo4j.kernel.api.index.IndexProvider;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.validateSelectorInstances;

class FusionSlotSelectorTest
{
    @Test
    void throwIfToFewInstances()
    {
        // given
        EnumMap<IndexSlot,IndexProvider> instances = new EnumMap<>( IndexSlot.class );
        for ( IndexSlot indexSlot : IndexSlot.values() )
        {
            instances.put( indexSlot, IndexProvider.EMPTY );
        }
        InstanceSelector<IndexProvider> instanceSelector = new InstanceSelector<>( instances );

        // when, then
        assertThrows( IllegalArgumentException.class, () -> validateSelectorInstances( instanceSelector, NUMBER ) );
    }

    @Test
    void throwIfToManyInstances()
    {
        // given
        EnumMap<IndexSlot,IndexProvider> instances = new EnumMap<>( IndexSlot.class );
        for ( IndexSlot indexSlot : IndexSlot.values() )
        {
            instances.put( indexSlot, IndexProvider.EMPTY );
        }
        IndexProvider mockedIndxProvider = mock( IndexProvider.class );
        instances.put( NUMBER, mockedIndxProvider );
        InstanceSelector<IndexProvider> instanceSelector = new InstanceSelector<>( instances );

        // when, then
        assertThrows( IllegalArgumentException.class, () -> validateSelectorInstances( instanceSelector ) );
    }

    @Test
    void shouldValidateSelectorInstances()
    {
        // given
        EnumMap<IndexSlot,IndexProvider> instances = new EnumMap<>( IndexSlot.class );
        for ( IndexSlot indexSlot : IndexSlot.values() )
        {
            instances.put( indexSlot, IndexProvider.EMPTY );
        }
        IndexProvider mockedIndxProvider = mock( IndexProvider.class );
        instances.put( NUMBER, mockedIndxProvider );
        InstanceSelector<IndexProvider> selector = new InstanceSelector<>( instances );

        // when
        validateSelectorInstances( selector, NUMBER );

        // then this should be fine
    }
}
