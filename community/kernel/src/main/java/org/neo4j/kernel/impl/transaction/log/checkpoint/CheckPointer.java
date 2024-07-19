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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.TransactionId;

/**
 * This interface represent a check pointer which is responsible to write check points in the transaction log.
 */
public interface CheckPointer {
    /**
     * This method will verify that the conditions for triggering a check point hold and in such a case it will write
     * a check point in the transaction log.
     *
     * This method does NOT handle concurrency since there should be only one check point thread running.
     *
     * @param triggerInfo the info describing why check pointing has been triggered
     * pending approval of the threshold check
     * @return the append index used for the check pointing or -1 if check pointing wasn't needed
     * @throws IOException if writing the check point fails
     */
    long checkPointIfNeeded(TriggerInfo triggerInfo) throws IOException;

    /**
     * This method tries the write of a check point in the transaction log. If there is no running check pointing it
     * will check point otherwise it will wait for the running check pointing to complete.
     *
     * @param triggerInfo the info describing why check pointing has been triggered
     * @return the append index used for the check pointing or -1 if check pointing wasn't performed
     * @throws IOException if writing the check point fails
     */
    long tryCheckPoint(TriggerInfo triggerInfo) throws IOException;

    /**
     * This method tries the write of a check point in the transaction log. If there is no running check pointing it
     * will check point otherwise it will wait for the running check pointing to complete.
     *
     * It is mostly used for testing purpose and to force a check point when shutting down the database.
     *
     * @param triggerInfo the info describing why check pointing has been triggered
     * @param timeout a boolean supplier that, if it returns {@code true}, will signal that we should stop waiting for any on-going checkpoint to complete.
     * @return the append index used for the check pointing, or -1 if we ended up waiting for an on-going checkpoint and the timeout returned {@code false}
     * telling us to give up waiting.
     * @throws IOException if writing the check point fails
     */
    long tryCheckPoint(TriggerInfo triggerInfo, BooleanSupplier timeout) throws IOException;

    /**
     * This method tries the write of a check point in the transaction log. If there is no running check pointing it
     * will check point otherwise it will return {@code -1}.
     *
     * @param triggerInfo the info describing why check pointing has been triggered
     * @return the append index used for the check pointing or {@code -1} when the invocation did not trigger a check point.
     * @throws IOException if writing the check point fails
     */
    long tryCheckPointNoWait(TriggerInfo triggerInfo) throws IOException;

    /**
     * This method forces the write of a check point in the transaction log.
     *
     * It is mostly used for testing purpose and to force a check point when shutting down the database.
     *
     * @param triggerInfo the info describing why check pointing has been triggered
     * @return the append index used for the check pointing
     * @throws IOException if writing the check point fails
     */
    long forceCheckPoint(TriggerInfo triggerInfo) throws IOException;

    /**
     * This method forces the write of a check point in the transaction log for specific transaction and position.
     *
     * It is used by clustering to force checkpoint on logs after store copy.
     *
     * @param transactionId transaction id to checkpoint.
     * @param appendIndex append index to checkpoint
     * @param position position of provided transaction id to checkpoint.
     * @param triggerInfo the info describing why check pointing has been triggered.
     * @return the append index used for the check pointing
     * @throws IOException if writing the check point fails
     */
    long forceCheckPoint(TransactionId transactionId, long appendIndex, LogPosition position, TriggerInfo triggerInfo)
            throws IOException;

    /**
     * @return Info about latest checkpoint that was made. If there's no checkpoint then
     * unknown values are returned for all the fields
     */
    LatestCheckpointInfo latestCheckPointInfo();

    /**
     * Shutdown checkpointer and prevent any new checkpoints from happening.
     * Any checkpoint requested on a shutdown checkpointer will not write any checkpoint and will
     * return {@code -1} since checkpoint wasn't executed.
     */
    void shutdown();
}
