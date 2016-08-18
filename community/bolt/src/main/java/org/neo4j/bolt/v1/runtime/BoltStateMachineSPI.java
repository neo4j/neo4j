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

import java.util.Map;

import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v1.runtime.spi.StatementRunner;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

class BoltStateMachineSPI implements BoltStateMachine.SPI
{
    private final String connectionDescriptor;
    private final UsageData usageData;
    private final ErrorReporter errorReporter;
    private final BoltConnectionTracker connectionTracker;
    private final Authentication authentication;

    final TransactionStateMachine.SPI transactionSpi;

    BoltStateMachineSPI( String connectionDescriptor,
                         UsageData usageData,
                         GraphDatabaseAPI db,
                         QueryExecutionEngine queryExecutionEngine,
                         LogService logging,
                         Authentication authentication,
                         ThreadToStatementContextBridge txBridge,
                         StatementRunner statementRunner,
                         BoltConnectionTracker connectionTracker )
    {
        this.connectionDescriptor = connectionDescriptor;
        this.usageData = usageData;
        this.errorReporter = new ErrorReporter( logging );
        this.connectionTracker = connectionTracker;
        Log log = logging.getInternalLog( BoltStateMachine.class );
        this.authentication = authentication;
        this.transactionSpi = new TransactionStateMachine.SPI()
        {
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

        };
    }

    @Override
    public String connectionDescriptor()
    {
        return connectionDescriptor;
    }

    @Override
    public void register( BoltStateMachine machine, String owner )
    {
        connectionTracker.onRegister( machine, owner );
    }

    @Override
    public TransactionStateMachine.SPI transactionSpi()
    {
        return transactionSpi;
    }

    @Override
    public void onTerminate( BoltStateMachine machine )
    {
        connectionTracker.onTerminate( machine );
    }

    @Override
    public void reportError( Neo4jError err )
    {
        errorReporter.report( err );
    }

    @Override
    public AuthenticationResult authenticate( Map<String,Object> authToken ) throws AuthenticationException
    {
        return authentication.authenticate( authToken );
    }
    @Override
    public void udcRegisterClient( String clientName )
    {
        usageData.get( UsageDataKeys.clientNames ).add( clientName );
    }
}
