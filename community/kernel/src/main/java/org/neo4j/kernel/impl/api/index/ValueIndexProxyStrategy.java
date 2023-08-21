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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;

class ValueIndexProxyStrategy implements IndexProxyStrategy {
    private final IndexDescriptor indexDescriptor;
    private final IndexStatisticsStore statisticsStore;
    private final String indexUserDescription;

    ValueIndexProxyStrategy(
            IndexDescriptor indexDescriptor, IndexStatisticsStore statisticsStore, TokenNameLookup tokenNameLookup) {
        this.indexDescriptor = indexDescriptor;
        this.statisticsStore = statisticsStore;
        indexUserDescription = indexDescriptor.userDescription(tokenNameLookup);
    }

    @Override
    public IndexDescriptor getIndexDescriptor() {
        return indexDescriptor;
    }

    @Override
    public void removeStatisticsForIndex() {
        statisticsStore.removeIndex(indexDescriptor.getId());
    }

    @Override
    public void incrementUpdateStatisticsForIndex(long delta) {
        statisticsStore.incrementIndexUpdates(indexDescriptor.getId(), delta);
    }

    @Override
    public void replaceStatisticsForIndex(IndexSample sample) {
        statisticsStore.setSampleStats(indexDescriptor.getId(), sample);
    }

    @Override
    public String getIndexUserDescription() {
        return indexUserDescription;
    }
}
