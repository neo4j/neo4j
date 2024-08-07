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

import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public class DatabaseTransactionCommitProcess implements TransactionCommitProcess {
    private final TransactionCommitProcess commitProcess;
    private final DatabaseReadOnlyChecker readOnlyDatabaseChecker;

    public DatabaseTransactionCommitProcess(
            InternalTransactionCommitProcess commitProcess, DatabaseReadOnlyChecker readOnlyDatabaseChecker) {
        this.commitProcess = commitProcess;
        this.readOnlyDatabaseChecker = readOnlyDatabaseChecker;
    }

    @Override
    public long commit(
            StorageEngineTransaction batch,
            TransactionWriteEvent transactionWriteEvent,
            TransactionApplicationMode mode)
            throws TransactionFailureException {
        readOnlyDatabaseChecker.check();
        return commitProcess.commit(batch, transactionWriteEvent, mode);
    }
}
