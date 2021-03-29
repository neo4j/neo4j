/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.id.indexed;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.id.IdSlotDistribution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_MONITOR;

class IdCacheTest
{
    @Test
    void shouldReportCorrectSpaceAvailableById()
    {
        // given
        IdCache cache = new IdCache( new IdSlotDistribution.Slot( 8, 1 ), new IdSlotDistribution.Slot( 8, 4 ) );
        assertThat( cache.availableSpaceById() ).isEqualTo( 40 );

        // when
        PendingIdQueue toOffer = new PendingIdQueue( cache.slotsByAvailableSpace() );
        toOffer.offer( 1, 4 );
        toOffer.offer( 10, 6 );
        cache.offer( toOffer, NO_MONITOR );

        // then
        assertThat( cache.availableSpaceById() ).isEqualTo( 30 );
    }

    @Test
    void shouldReportCorrectSlotsByAvailableSpace()
    {
        // given
        IdCache cache = new IdCache(
                new IdSlotDistribution.Slot( 8, 1 ),
                new IdSlotDistribution.Slot( 8, 2 ),
                new IdSlotDistribution.Slot( 4, 4 ) );
        assertThat( cache.slotsByAvailableSpace() ).isEqualTo( new IdSlotDistribution.Slot[]{
                new IdSlotDistribution.Slot( 8, 1 ),
                new IdSlotDistribution.Slot( 8, 2 ),
                new IdSlotDistribution.Slot( 4, 4 )} );

        // when
        PendingIdQueue toOffer = new PendingIdQueue( cache.slotsByAvailableSpace() );
        toOffer.offer( 1, 4 );
        toOffer.offer( 10, 6 );
        cache.offer( toOffer, NO_MONITOR );

        // then
        assertThat( cache.slotsByAvailableSpace() ).isEqualTo( new IdSlotDistribution.Slot[]{
                new IdSlotDistribution.Slot( 8, 1 ),
                new IdSlotDistribution.Slot( 7, 2 ),
                new IdSlotDistribution.Slot( 2, 4 )} );
    }
}

