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
package org.neo4j.kernel.api.database.transaction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.OptionalLong;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.TransactionId;

/**
 * Service to access database transaction logs
 */
public interface TransactionLogService {
    /**
     * Provide list of channels for underlying transaction log files starting with requested initial append index.
     * Provided channels can be closed for any reason by underlying database, if it will choose to do so in any moment.
     * Its client responsibility to repeat request in such cases to retrieve new set of readers.
     *
     * @param startAppendIndex initial append index to retrieve channels from. Should be positive number and be in range of existing append indexes.
     * @return object with access to read only transaction log channels to be able to access requested transaction logs content
     * @throws IOException on failure performing underlying transaction logs operation
     * @throws IllegalArgumentException invalid append index, append index not found in any of the log files
     */
    TransactionLogChannels logFilesChannels(long startAppendIndex) throws IOException;

    /**
     * Append buffer content to the end of transaction logs. Buffer content is only appended and is not interpreted at this point.
     * Meaning transactions will be replayed as subsequent recovery. There is no guarantee what buffer will contain in terms of transactions: it can
     * be completely empty, contain only part of transaction, several of those etc.
     * Unlike transactional log append log rotation will be performed only when transaction id and append index is provided and rotations is required.
     * Transactions that are appended by this method do not perform commit, as result there no updates to metadata store will be executed.
     * Mixing bulk append and applying standard transactional workload is not supported and will result in corrupted database.
     * Provided byte buffer can be reused, if required, after append operation completion.
     * @param byteBuffer buffer with transactional content
     * @param transactionId optional known transaction id
     * @param appendIndex optional known append index
     * @return log position before any buffer content updates happen
     * @throws IOException on failure performing underlying transaction logs operation
     */
    LogPosition append(ByteBuffer byteBuffer, OptionalLong transactionId, OptionalLong appendIndex) throws IOException;

    /**
     * Reset writer position after failed transactional log bulk update.
     * In case if provided log position does not cover area of transaction logs that is batch appended various runtime exceptions will be thrown.
     * @param position log position to which bulk writer should be reset to.
     * @throws IOException on failure performing underlying transaction logs operation
     */
    void restore(LogPosition position) throws IOException;

    /**
     * Append checkpoint record to log files that uses provided {@link TransactionId transactionId} as last closed transaction.
     * @param transactionId last closed transaction id to be used in a checkpoint record.
     * @param appendIndex last append index to be used in a checkpoint record.
     * @param reason checkpoint reason
     * @throws IOException on failure performing underlying log files operation
     */
    void appendCheckpoint(TransactionId transactionId, long appendIndex, String reason) throws IOException;
}
