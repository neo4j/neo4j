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
package org.neo4j.internal.counts;

import org.eclipse.collections.api.set.primitive.LongSet;

/**
 * Information about applied transaction, written during checkpoint and read on opening the {@link GBPTreeCountsStore}.
 */
class TxIdInformation
{
    final long highestGapFreeTxId;
    final LongSet strayTxIds;

    TxIdInformation( long highestGapFreeTxId, LongSet strayTxIds )
    {
        this.highestGapFreeTxId = highestGapFreeTxId;
        this.strayTxIds = strayTxIds;
    }

    /**
     * Given the transaction id information decides whether or not the given txId has already been applied to the counts store.
     * @param txId the transaction id to check whether or not it has already been applied to the counts store.
     * @return whether or not the supplied txId has already been applied to the tree.
     */
    boolean txIdIsAlreadyApplied( long txId )
    {
        return txId <= highestGapFreeTxId || strayTxIds.contains( txId );
    }
}
