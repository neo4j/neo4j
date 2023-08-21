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
package org.neo4j.fabric.transaction;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.eval.Catalog;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.planning.StatementType;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseReference;

public interface FabricTransaction {

    void commit();

    void rollback();

    StatementResult execute(Function<FabricExecutionContext, StatementResult> runLogic);

    boolean markForTermination(Status reason);

    default Optional<Status> getReasonIfTerminated() {
        return getTerminationMark().map(TerminationMark::getReason);
    }

    Optional<TerminationMark> getTerminationMark();

    FabricTransactionInfo getTransactionInfo();

    TransactionBookmarkManager getBookmarkManager();

    void setMetaData(Map<String, Object> txMeta);

    boolean isOpen();

    interface FabricExecutionContext {
        FabricRemoteExecutor.RemoteTransactionContext getRemote();

        FabricLocalExecutor.LocalTransactionContext getLocal();

        void validateStatementType(StatementType type);

        DatabaseReference getSessionDatabaseReference();

        Location locationOf(Catalog.Graph graph, Boolean requireWritable);
    }

    Catalog getCatalogSnapshot();

    CancellationChecker cancellationChecker();

    ExecutingQuery.TransactionBinding transactionBinding();

    Procedures contextlessProcedures();
}
