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
package org.neo4j.kernel.impl.factory;

import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.kernel.impl.api.CommandCommitListeners;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.DatabaseTransactionCommitProcess;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

public class CommunityCommitProcessFactory implements CommitProcessFactory {
    @Override
    public TransactionCommitProcess create(
            TransactionAppender appender,
            StorageEngine storageEngine,
            DatabaseReadOnlyChecker readOnlyChecker,
            boolean preAllocateSpaceInStoreFiles,
            CommandCommitListeners commandCommitListeners) {
        return new DatabaseTransactionCommitProcess(
                new InternalTransactionCommitProcess(
                        appender, storageEngine, preAllocateSpaceInStoreFiles, commandCommitListeners),
                readOnlyChecker);
    }
}
