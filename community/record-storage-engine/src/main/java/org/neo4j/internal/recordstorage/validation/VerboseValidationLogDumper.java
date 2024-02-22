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
package org.neo4j.internal.recordstorage.validation;

import static java.lang.System.lineSeparator;
import static org.neo4j.internal.recordstorage.MultiversionResourceLocker.PAGE_ID_BITS;
import static org.neo4j.internal.recordstorage.MultiversionResourceLocker.PAGE_ID_MASK;

import java.util.EnumMap;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.lock.ActiveLock;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidator;
import org.neo4j.storageengine.api.txstate.validation.ValidationLockDumper;

public class VerboseValidationLogDumper implements ValidationLockDumper {
    private static final long UNKNOWN_PAGE_VERSION = -1;
    private final Log log;
    private final NeoStores neoStores;

    public VerboseValidationLogDumper(LogProvider logProvider, NeoStores neoStores) {
        this.log = logProvider.getLog(getClass());
        this.neoStores = neoStores;
    }

    @Override
    public void dumpLocks(TransactionValidator validator, LockManager.Client lockClient, int chunkNumber, long txId) {
        var tcm = (TransactionCommandValidator) validator;
        var observedVersions = tcm.getObservedPageVersions();

        StringBuilder locksDumpBuilder = new StringBuilder();
        locksDumpBuilder
                .append("Transaction sequence number: ")
                .append(lockClient.getTransactionId())
                .append(" with tx id(chunk): ")
                .append(txId)
                .append("(")
                .append(chunkNumber)
                .append(")");
        var locks = lockClient.activeLocks();
        if (locks.isEmpty()) {
            locksDumpBuilder.append(" does not have any validation page locks.");
        } else {
            locksDumpBuilder.append(" locked page(s):").append(lineSeparator());

            var storyTypeRecords = new EnumMap<StoreType, Integer>(StoreType.class);

            for (ActiveLock activeLock : locks) {
                long resourceId = activeLock.resourceId();
                var storeType = StoreType.values()[(int) (resourceId >> PAGE_ID_BITS)];
                int recordsPerPage = storyTypeRecords.computeIfAbsent(
                        storeType, type -> neoStores.getRecordStore(type).getRecordsPerPage());
                long pageId = resourceId & PAGE_ID_MASK;
                locksDumpBuilder
                        .append(pageId)
                        .append(" of ")
                        .append(storeType)
                        .append(" store, with records per page ")
                        .append(recordsPerPage)
                        .append(" observed page version: ")
                        .append(observedVersions.getOrDefault(new PageEntry(pageId, storeType), UNKNOWN_PAGE_VERSION))
                        .append(lineSeparator());
            }
        }
        log.error(locksDumpBuilder.toString());
    }
}
