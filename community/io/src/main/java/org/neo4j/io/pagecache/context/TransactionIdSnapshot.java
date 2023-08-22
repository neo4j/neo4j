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
package org.neo4j.io.pagecache.context;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;

import java.util.Arrays;
import org.neo4j.util.concurrent.OutOfOrderSequence;

public record TransactionIdSnapshot(long lastClosedTxId, long highestEverSeen, long[] notVisibleTransactions) {
    public static final TransactionIdSnapshot EMPTY_ID_SNAPSHOT = new TransactionIdSnapshot(1);

    public static boolean isNotVisible(long[] notVisibleVersions, long version) {
        if (notVisibleVersions.length == 0) {
            return false;
        }
        return Arrays.binarySearch(notVisibleVersions, version) >= 0;
    }

    public TransactionIdSnapshot(long lastClosedTxId) {
        this(lastClosedTxId, lastClosedTxId, EMPTY_LONG_ARRAY);
    }

    public TransactionIdSnapshot(OutOfOrderSequence.ReverseSnapshot reverseSnapshot) {
        this(reverseSnapshot.highestGapFree(), reverseSnapshot.highestEverSeen(), reverseSnapshot.missingIds());
    }

    @Override
    public String toString() {
        return "TransactionIdSnapshot{" + "lastClosedTxId="
                + lastClosedTxId + ", highestEverSeen="
                + highestEverSeen + ", notVisibleTransactions="
                + Arrays.toString(notVisibleTransactions) + '}';
    }
}
