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
package org.neo4j.fabric.executor;

import reactor.core.publisher.Mono;

import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.fabric.transaction.CompositeTransaction;
import org.neo4j.fabric.transaction.FabricTransactionInfo;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.values.virtual.MapValue;

public class ThrowingFabricRemoteExecutor implements FabricRemoteExecutor
{
    @Override
    public RemoteTransactionContext startTransactionContext( CompositeTransaction compositeTransaction, FabricTransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager )
    {
        return new RemoteTransactionContextImpl();
    }

    private static class RemoteTransactionContextImpl implements RemoteTransactionContext
    {

        @Override
        public Mono<StatementResult> run( Location.Remote location, String query, TransactionMode transactionMode, MapValue params )
        {
            throw new IllegalStateException( "Remote query execution not supported" );
        }

        @Override
        public void close()
        {

        }
    }
}
