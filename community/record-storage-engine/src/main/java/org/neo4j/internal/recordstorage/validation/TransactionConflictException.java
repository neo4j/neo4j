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
package org.neo4j.internal.recordstorage.validation;

import java.util.Arrays;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.store.StoreType;

public class TransactionConflictException extends RuntimeException implements Status.HasStatus {

    private final StoreType storeType;
    private final long observedVersion;
    private final long highestClosed;
    private final long[] nonVisibleTransactions;
    private final String message;

    public TransactionConflictException(StoreType type, VersionContext versionContext) {
        this.storeType = type;
        this.observedVersion = versionContext.currentInvisibleChainHeadVersion();
        this.highestClosed = versionContext.highestClosed();
        this.nonVisibleTransactions = versionContext.notVisibleTransactionIds();
        this.message = createMessage();
    }

    @Override
    public String getMessage() {
        return message;
    }

    public StoreType getStoreType() {
        return storeType;
    }

    public long getObservedVersion() {
        return observedVersion;
    }

    public long getHighestClosed() {
        return highestClosed;
    }

    public long[] getNonVisibleTransactions() {
        return nonVisibleTransactions;
    }

    @Override
    public Status status() {
        return Status.Transaction.Outdated;
    }

    private String createMessage() {
        return "Concurrent modification exception. Page in "
                + storeType.getDatabaseFile().getName() + " store is modified already by transaction "
                + observedVersion + ", while ongoing transaction highest visible is: " + highestClosed
                + ", with not yet visible transaction ids are: " + Arrays.toString(nonVisibleTransactions) + ".";
    }
}
