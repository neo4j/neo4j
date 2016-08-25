/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.bolt.v1.runtime;

import java.time.Duration;
import java.util.Map;

import org.neo4j.bolt.v1.runtime.spi.StatementRunner;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.txtracking.TransactionIdTracker;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

class TransactionStateMachineSPI implements TransactionStateMachine.SPI
{
    private final GraphDatabaseAPI db;
    private final ThreadToStatementContextBridge txBridge;
    private final QueryExecutionEngine queryExecutionEngine;
    private final StatementRunner statementRunner;
    private final TransactionIdTracker transactionIdTracker;

    TransactionStateMachineSPI( GraphDatabaseAPI db,
                                ThreadToStatementContextBridge txBridge,
                                QueryExecutionEngine queryExecutionEngine,
                                StatementRunner statementRunner,
                                TransactionIdStore transactionIdStoreSupplier )
    {
        this.db = db;
        this.txBridge = txBridge;
        this.queryExecutionEngine = queryExecutionEngine;
        this.statementRunner = statementRunner;
        this.transactionIdTracker = new TransactionIdTracker( transactionIdStoreSupplier );
    }

    @Override
    public void awaitUpToDate( long oldestAcceptableTxId, Duration timeout ) throws TransactionFailureException
    {
        transactionIdTracker.awaitUpToDate( oldestAcceptableTxId, timeout );
    }

    @Override
    public long newestEncounteredTxId()
    {
        return transactionIdTracker.newestEncounteredTxId();
    }

    @Override
    public KernelTransaction beginTransaction( AuthSubject authSubject )
    {
        db.beginTransaction( KernelTransaction.Type.explicit, authSubject );
        return txBridge.getKernelTransactionBoundToThisThread( false );
    }

    @Override
    public void bindTransactionToCurrentThread( KernelTransaction tx )
    {
        txBridge.bindTransactionToCurrentThread( tx );
    }

    @Override
    public void unbindTransactionFromCurrentThread()
    {
        txBridge.unbindTransactionFromCurrentThread();
    }

    @Override
    public boolean isPeriodicCommit( String query )
    {
        return queryExecutionEngine.isPeriodicCommit( query );
    }

    @Override
    public Result executeQuery( String querySource,
                                AuthSubject authSubject,
                                String statement,
                                Map<String, Object> params ) throws QueryExecutionKernelException
    {
        try
        {
            return statementRunner.run( querySource, authSubject, statement, params );
        }
        catch ( KernelException e )
        {
            throw new QueryExecutionKernelException( e );
        }
    }

}
