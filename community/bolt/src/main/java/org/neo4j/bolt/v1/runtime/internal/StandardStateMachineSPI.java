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
package org.neo4j.bolt.v1.runtime.internal;

import java.util.Map;

import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.bolt.v1.runtime.spi.StatementRunner;
import org.neo4j.concurrent.DecayingFlags;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

class StandardStateMachineSPI implements SessionStateMachine.SPI
{
    private final String connectionDescriptor;
    private final UsageData usageData;
    private final GraphDatabaseFacade db;
    private final StatementRunner statementRunner;
    private final ErrorReporter errorReporter;
    private final Log log;
    private final Authentication authentication;
    private final ThreadToStatementContextBridge txBridge;
    private final DecayingFlags featureUsage;

    StandardStateMachineSPI( String connectionDescriptor, UsageData usageData, GraphDatabaseFacade db, StatementRunner statementRunner,
            LogService logging, Authentication authentication, ThreadToStatementContextBridge txBridge )
    {
        this.connectionDescriptor = connectionDescriptor;
        this.usageData = usageData;
        this.db = db;
        this.statementRunner = statementRunner;
        this.txBridge = txBridge;
        this.featureUsage = usageData.get( UsageDataKeys.features );
        this.errorReporter = new ErrorReporter( logging );
        this.log = logging.getInternalLog( SessionStateMachine.class );
        this.authentication = authentication;
    }

    @Override
    public String connectionDescriptor()
    {
        return connectionDescriptor;
    }

    @Override
    public void reportError( Neo4jError err )
    {
        errorReporter.report( err );
    }

    @Override
    public void reportError( String message, Throwable cause )
    {
        log.error( message, cause );
    }

    @Override
    public KernelTransaction beginTransaction( KernelTransaction.Type type, AccessMode mode )
    {
        db.beginTransaction( type, mode );
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
    public RecordStream run( SessionStateMachine ctx, String statement, Map<String,Object> params )
            throws KernelException
    {

        featureUsage.flag( UsageDataKeys.Features.bolt );
        return statementRunner.run( ctx, statement, params );
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

    @Override
    public Statement currentStatement()
    {
        return txBridge.get();
    }
}
