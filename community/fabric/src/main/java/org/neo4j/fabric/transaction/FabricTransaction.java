/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.transaction;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.fabric.executor.FabricStatementLifecycles.StatementLifecycle;
import org.neo4j.fabric.planning.StatementType;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.kernel.api.exceptions.Status;

public interface FabricTransaction
{

    void commit();

    void rollback();

    StatementResult execute( Function<FabricExecutionContext,StatementResult> runLogic );

    void markForTermination( Status reason );

    Optional<Status> getReasonIfTerminated();

    FabricTransactionInfo getTransactionInfo();

    TransactionBookmarkManager getBookmarkManager();

    void setLastSubmittedStatement( StatementLifecycle statement );

    Optional<StatementLifecycle> getLastSubmittedStatement();

    void setMetaData( Map<String, Object> txMeta );

    interface FabricExecutionContext
    {
        FabricRemoteExecutor.RemoteTransactionContext getRemote();

        FabricLocalExecutor.LocalTransactionContext getLocal();

        void validateStatementType( StatementType type );
    }
}
