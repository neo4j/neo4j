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

import java.util.List;
import org.neo4j.storageengine.api.CommandBatch;

public class CommandCommitListeners {
    public static final CommandCommitListeners NO_LISTENERS = new CommandCommitListeners();
    private final List<CommandCommitListener> listeners;

    public CommandCommitListeners(CommandCommitListener... listeners) {
        this.listeners = List.of(listeners);
    }

    public void registerFailure(CommandBatch commandBatch, Exception exception) {
        for (CommandCommitListener listener : listeners) {
            listener.onCommandBatchCommitFailure(commandBatch, exception);
        }
    }

    public void registerSuccess(CommandBatch commandBatch, long lastCommittedTx) {
        for (CommandCommitListener listener : listeners) {
            listener.onCommandBatchCommitSuccess(commandBatch, lastCommittedTx);
        }
    }
}
