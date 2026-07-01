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
package org.neo4j.kernel.impl.api.transaction.monitor;

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

public class TransactionMonitoringRecord {

    private final KernelTransactionImplementation tx;
    private final long highestGapFreeTxId;
    private final long transactionHorizon;

    public TransactionMonitoringRecord(KernelTransactionImplementation tx, CursorContext cursorContext) {
        this.tx = tx;
        VersionContext versionContext = cursorContext.getVersionContext();
        this.highestGapFreeTxId = versionContext.highestGapFree();
        this.transactionHorizon = transactionHorizon(versionContext);
    }

    private static long transactionHorizon(VersionContext versionContext) {
        // if transaction has already started committing its horizon is oldestVisibleTransactionNumber which was
        // recorded at the time commit started
        if (versionContext.initializedForWrite()) {
            return versionContext.oldestVisibilityHorizon();
        }
        // otherwise, its horizon is the latest gap free closed transaction at the time it started
        return versionContext.highestGapFree();
    }

    public long getHighestGapFreeTxId() {
        return highestGapFreeTxId;
    }

    public long getTransactionHorizon() {
        return transactionHorizon;
    }

    @Override
    public String toString() {
        return "KernelTransactionMonitoringRecord{" + "tx="
                + tx + ", highestGapFreeTxId="
                + highestGapFreeTxId + ", transactionHorizon="
                + transactionHorizon + '}';
    }
}
