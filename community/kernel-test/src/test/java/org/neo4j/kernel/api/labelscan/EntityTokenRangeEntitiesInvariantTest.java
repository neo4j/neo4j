/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.labelscan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.common.EntityType.NODE;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.index.schema.EntityTokenRange;
import org.neo4j.kernel.impl.index.schema.EntityTokenRangeImpl;
import org.neo4j.kernel.impl.index.schema.TokenIndexIdLayout;

class EntityTokenRangeEntitiesInvariantTest {
    private static final int RANGE_SIZE = 64;

    @Test
    void entitiesShouldMatchCoveredIdsForFullRange() {
        assertEntitiesMatchesCoveredIds(new PageSplitIdLayout(73), 0);
    }

    @Test
    void entitiesShouldMatchCoveredIdsForShortRange() {
        assertEntitiesMatchesCoveredIds(new PageSplitIdLayout(73), 1);
    }

    private static void assertEntitiesMatchesCoveredIds(TokenIndexIdLayout idLayout, long idRange) {
        int[][] tokens = new int[RANGE_SIZE][];
        EntityTokenRange range = new EntityTokenRangeImpl(idRange, tokens, NODE, idLayout);

        long lowRangeId = idLayout.firstIdOfRange(idRange);
        long highRangeId = idLayout.firstIdOfRange(idRange + 1) - 1;

        long[] entities = range.entities();
        long previousId = lowRangeId - 1;
        for (long entityId : entities) {
            assertThat(range.covers(entityId))
                    .as("entities() member should be covered")
                    .isTrue();
            assertThat(entityId)
                    .as("entities() should be contiguous and ascending")
                    .isEqualTo(previousId + 1);
            previousId = entityId;
        }

        long expectedCount = Math.min(highRangeId - lowRangeId + 1, tokens.length);
        assertThat(entities).hasSize((int) expectedCount);
        if (expectedCount > 0) {
            assertThat(entities[0]).isEqualTo(lowRangeId);
            assertThat(entities[entities.length - 1]).isEqualTo(lowRangeId + expectedCount - 1);
        }

        assertThat(range.covers(lowRangeId + expectedCount)).isEqualTo(lowRangeId + expectedCount <= highRangeId);
    }

    private static class PageSplitIdLayout implements TokenIndexIdLayout {
        private static final int RANGE_MASK = RANGE_SIZE - 1;
        private static final int RANGE_SHIFT = 31 - Integer.numberOfLeadingZeros(RANGE_SIZE);

        private final int entitiesPerPage;
        private final int rangesPerPage;

        PageSplitIdLayout(int entitiesPerPage) {
            this.entitiesPerPage = entitiesPerPage;
            this.rangesPerPage = (entitiesPerPage - 1) / RANGE_SIZE + 1;
        }

        @Override
        public int idWithinRange(long entityId) {
            int entityIdOnPage = (int) (entityId % entitiesPerPage);
            return entityIdOnPage & RANGE_MASK;
        }

        @Override
        public long rangeOf(long entityId) {
            long pageId = entityId / entitiesPerPage;
            long entityIdOnPage = entityId % entitiesPerPage;
            return rangesPerPage * pageId + (entityIdOnPage >> RANGE_SHIFT);
        }

        @Override
        public long firstIdOfRange(long idRange) {
            long pageId = idRange / rangesPerPage;
            return pageId * entitiesPerPage + ((idRange % rangesPerPage) << RANGE_SHIFT);
        }
    }
}
