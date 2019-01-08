/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.storageengine.api.StorageCommand;

/**
 * Responsible for a single transaction. See also {@link BatchTransactionApplier} which returns an implementation of
 * this class. Should typically be used in a try-with-resources block, f.ex.:
 * <pre>
 * {@code
 *     try ( TransactionApplier txApplier = batchTxApplier.startTx( txToApply )
 *     {
 *         // Apply the transaction
 *         txToApply.transactionRepresentation().accept( txApplier );
 *         // Or apply other commands
 *         // txApplier.visit( command );
 *     }
 * }
 * </pre>
 */
public interface TransactionApplier extends Visitor<StorageCommand,IOException>, CommandVisitor, AutoCloseable
{
    /**
     * A {@link TransactionApplier} which does nothing.
     */
    TransactionApplier EMPTY = new Adapter();

    /**
     * Delegates to individual visit methods (see {@link CommandVisitor}) which need to be implemented, as well as
     * {@link #close()} if applicable.
     */
    class Adapter extends CommandVisitor.Adapter implements TransactionApplier
    {
        @Override
        public void close() throws Exception
        {
            // Do nothing
        }

        @Override
        public boolean visit( StorageCommand element ) throws IOException
        {
            return ((Command)element).handle( this );
        }
    }
}
