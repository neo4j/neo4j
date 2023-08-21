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

/**
 * Collecting statistics on token indexes is not needed and therefore not supported.
 */
class TokenIndexProxyStrategy implements IndexProxyStrategy {
    private final IndexDescriptor descriptor;
    private final TokenNameLookup tokenNameLookup;

    TokenIndexProxyStrategy(IndexDescriptor descriptor, TokenNameLookup tokenNameLookup) {
        this.descriptor = descriptor;
        this.tokenNameLookup = tokenNameLookup;
    }

    @Override
    public IndexDescriptor getIndexDescriptor() {
        return descriptor;
    }

    @Override
    public void removeStatisticsForIndex() {}

    @Override
    public void incrementUpdateStatisticsForIndex(long delta) {}

    @Override
    public void replaceStatisticsForIndex(IndexSample sample) {}

    @Override
    public String getIndexUserDescription() {
        return descriptor.userDescription(tokenNameLookup);
    }
}
