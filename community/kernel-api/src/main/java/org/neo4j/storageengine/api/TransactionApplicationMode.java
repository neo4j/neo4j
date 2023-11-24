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
package org.neo4j.storageengine.api;

import static org.neo4j.storageengine.api.CommandVersion.AFTER;
import static org.neo4j.storageengine.api.CommandVersion.BEFORE;

/**
 * Mode of {@link StorageEngine#apply(CommandBatchToApply, TransactionApplicationMode) applying transactions}.
 * Depending on how transaction state have been built, additional work may need to be performed during
 * application of it.
 */
public enum TransactionApplicationMode {
    /**
     * Transaction that is created in the "normal" way and has changed transaction state, which goes
     * to commit and produces commands from that. Such a transaction is able to alter cache since it has
     * all such high level information directly from the transaction state.
     */
    INTERNAL(
            false, // id tracking not needed since that is done in the transaction before commit
            true, // include all stores
            false, AFTER),

    /**
     * Transaction that comes from an external source and consists only of commands, i.e. it may not
     * contain enough information to f.ex. update cache, but applies to the store just like an internal
     * transaction does.
     */
    EXTERNAL(
            true, // id tracking needed since that hasn't been done prior to receiving this external transaction
            true, // include all stores
            false, AFTER),

    /**
     * Transaction that is recovered, where commands are read, much like {@link #EXTERNAL}, but should
     * be applied differently where extra care should be taken to ensure idempotency. This is because
     * a recovered transaction may have already been applied previously to the store.
     */
    RECOVERY(
            true, // id tracking not needed because id generators will be rebuilt after recovery anyway
            true, // include all stores
            false, AFTER),

    /**
     * Transaction that is recovered during a phase of reverse recovery in order to rewind neo store back
     * to a state where forward recovery then can commence from. Rewinding the store back to the point
     * if the last checkpoint will allow for correct updates to indexes, because indexes reads from
     * a mix of log and store to produce its updates.
     */
    REVERSE_RECOVERY(
            false, // id tracking not needed because this is for the initial reverse recovery
            false, // only apply to neo store
            false, BEFORE),

    /**
     * MVCC recovery uncommitted transaction appliers
     * Has additional processor for ids that should released or acquired back as transaction never happened
     */
    MVCC_ROLLBACK(
            false, // id tracking not needed because this is for the initial reverse recovery
            true, // include indexes and counts
            true, BEFORE);

    private final boolean needsHighIdTracking;
    private final boolean indexesAndCounts;
    private final boolean rollbackIdProcessing;
    private final CommandVersion version;

    TransactionApplicationMode(
            boolean needsHighIdTracking,
            boolean indexesAndCounts,
            boolean rollbackIdProcessing,
            CommandVersion version) {
        this.needsHighIdTracking = needsHighIdTracking;
        this.indexesAndCounts = indexesAndCounts;
        this.rollbackIdProcessing = rollbackIdProcessing;
        this.version = version;
    }

    /**
     * @return whether or not applying a transaction need to track and update high ids of underlying stores.
     */
    public boolean needsHighIdTracking() {
        return needsHighIdTracking;
    }

    /**
     * @return whether or not to include auxiliary stores, such as indexing, counts and statistics.
     */
    public boolean needsAuxiliaryStores() {
        return indexesAndCounts;
    }

    /**
     * @return which version of commands to apply, where some commands have before/after versions.
     */
    public CommandVersion version() {
        return version;
    }

    public boolean rollbackIdProcessing() {
        return rollbackIdProcessing;
    }

    public boolean isReverseStep() {
        return BEFORE == version;
    }
}
