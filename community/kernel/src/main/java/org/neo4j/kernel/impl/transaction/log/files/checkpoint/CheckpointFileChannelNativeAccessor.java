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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold;

import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.kernel.impl.transaction.log.files.LogFileChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;

class CheckpointFileChannelNativeAccessor extends LogFileChannelNativeAccessor {
    CheckpointFileChannelNativeAccessor(TransactionLogFilesContext context) {
        super(
                context.getFileSystem(),
                context.getNativeAccess(),
                context.getLogProvider(),
                checkpointRotation(context),
                context.getConfig(),
                context.getDatabaseName());
    }

    private static AtomicLong checkpointRotation(TransactionLogFilesContext context) {
        return new AtomicLong(context.getConfig().get(checkpoint_logical_log_rotation_threshold));
    }
}
