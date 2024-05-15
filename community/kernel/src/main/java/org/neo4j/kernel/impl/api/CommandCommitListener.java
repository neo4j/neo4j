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
package org.neo4j.kernel.impl.api;

import org.neo4j.storageengine.api.CommandBatch;

/**
 * This is a listener for events happening inside the commit process. Processes which must react to failures or successful commits deep down in kernel commit process (for example InternalTransactionCommitProcess) can listen to these
 * events.
 */
public interface CommandCommitListener {

    /**
     * Notifies listeners that something bad happened while committing the commandBatch.
     * @param commandBatch the first command in a batch which failed at commit. Note that this is the first command in a failing batch, might not necessarily mean this command in particular failed. Only that this was the first one in the failing chain.
     * @param exception the exception that occurred while committing to kernel.
     */
    void onCommandBatchCommitFailure(CommandBatch commandBatch, Exception exception);

    /**
     * Notifies listeners that the entire commandBatch committed successfully.
     * @param commandBatch the first command in a batch which committed fully, i.e. all commands in the batch committed successfully.
     * @param lastCommittedTx the last committed transaction after all commands in the batch committed successfully.
     */
    void onCommandBatchCommitSuccess(CommandBatch commandBatch, long lastCommittedTx);
}
