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
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.impl.index.schema.PartitionedTokenScan;

public class PartitionedTokenIndexCursorScan<Cursor extends org.neo4j.internal.kernel.api.Cursor>
        implements PartitionedScan<Cursor> {
    // Read is generally not a thread-safe object.
    // This one belongs to the thread that initialised the partitioned scan.
    // Even thought the operations performed by the partitioned scan on the Read should be OK, we prefer the threads
    // working with the partition to use their own Reads.
    // In other words, using this "global" Read makes thread-safety of the scan a bit questionable.
    private final Read fallbackRead;
    private final TokenPredicate query;
    private final PartitionedTokenScan tokenScan;

    PartitionedTokenIndexCursorScan(Read read, TokenPredicate query, PartitionedTokenScan tokenScan) {
        if (read.hasTxStateWithChanges()) {
            throw new IllegalStateException(
                    "Transaction contains changes; PartitionScan is only valid in Read-Only transactions.");
        }
        this.fallbackRead = read;
        this.query = query;
        this.tokenScan = tokenScan;
    }

    @Override
    public int getNumberOfPartitions() {
        return tokenScan.getNumberOfPartitions();
    }

    @Override
    public boolean reservePartition(Cursor cursor, CursorContext cursorContext, AccessMode accessMode) {
        return reservePartition(cursor, fallbackRead, cursorContext, accessMode);
    }

    @Override
    public boolean reservePartition(Cursor cursor, ExecutionContext executionContext) {
        return reservePartition(
                cursor,
                (org.neo4j.kernel.impl.newapi.Read) executionContext.dataRead(),
                executionContext.cursorContext(),
                executionContext.securityContext().mode());
    }

    private boolean reservePartition(Cursor cursor, Read read, CursorContext cursorContext, AccessMode accessMode) {
        final var indexCursor = (InternalTokenIndexCursor) cursor;
        indexCursor.setRead(read);
        final var indexProgressor = tokenScan.reservePartition(indexCursor, cursorContext);
        if (indexProgressor == IndexProgressor.EMPTY) {
            return false;
        }
        indexCursor.initialize(indexProgressor, query.tokenId(), null, null, accessMode);
        return true;
    }

    PartitionedTokenScan getTokenScan() {
        return tokenScan;
    }
}
