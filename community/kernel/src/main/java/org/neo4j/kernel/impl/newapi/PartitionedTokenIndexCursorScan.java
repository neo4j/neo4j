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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.impl.index.schema.PartitionedTokenScan;

public class PartitionedTokenIndexCursorScan<Cursor extends org.neo4j.internal.kernel.api.Cursor>
        implements PartitionedScan<Cursor> {
    private final TokenPredicate query;
    private final PartitionedTokenScan tokenScan;

    PartitionedTokenIndexCursorScan(TokenPredicate query, PartitionedTokenScan tokenScan) {
        this.query = query;
        this.tokenScan = tokenScan;
    }

    @Override
    public int getNumberOfPartitions() {
        return tokenScan.getNumberOfPartitions();
    }

    @Override
    public boolean reservePartition(Cursor cursor, ExecutionContext executionContext) {
        final var indexCursor = (InternalTokenIndexCursor) cursor;
        indexCursor.setRead((Read) executionContext.dataRead());
        final var indexProgressor = tokenScan.reservePartition(indexCursor, executionContext.cursorContext());
        if (indexProgressor == IndexProgressor.EMPTY) {
            return false;
        }
        indexCursor.initialize(indexProgressor, query.tokenId(), null, null);
        return true;
    }

    PartitionedTokenScan getTokenScan() {
        return tokenScan;
    }
}
