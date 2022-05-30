/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.protocol.common.fsm;

import java.time.Clock;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.protocol.common.MutableConnectionState;
import org.neo4j.bolt.protocol.common.transaction.statement.StatementProcessorProvider;
import org.neo4j.bolt.protocol.common.transaction.statement.StatementProcessorReleaseManager;
import org.neo4j.bolt.protocol.v41.message.request.RoutingContext;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.transaction.CleanUpTransactionContext;
import org.neo4j.bolt.transaction.InitializeContext;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.memory.HeapEstimator;

public class StateMachineContextImpl implements StateMachineContext, StatementProcessorReleaseManager {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(StateMachineContextImpl.class);

    private final StateMachine machine;
    private final BoltChannel channel;
    private final StateMachineSPI spi;
    private final MutableConnectionState connectionState;
    private final Clock clock;
    private final DefaultDatabaseResolver defaultDatabaseResolver;
    private final TransactionManager transactionManager;

    private String defaultDatabase;
    private LoginContext primaryLoginContext;
    private LoginContext impersonationLoginContext;

    public StateMachineContextImpl(
            StateMachine machine,
            BoltChannel channel,
            StateMachineSPI spi,
            MutableConnectionState connectionState,
            Clock clock,
            DefaultDatabaseResolver defaultDatabaseResolver,
            TransactionManager transactionManager) {
        this.machine = machine;
        this.channel = channel;
        this.spi = spi;
        this.connectionState = connectionState;
        this.clock = clock;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
        this.transactionManager = transactionManager;
    }

    @Override
    public String connectionId() {
        return machine.id();
    }

    @Override
    public BoltChannel channel() {
        return channel;
    }

    @Override
    public Clock clock() {
        return clock;
    }

    @Override
    public TransactionManager transactionManager() {
        return transactionManager;
    }

    @Override
    public StateMachineSPI boltSpi() {
        return spi;
    }

    @Override
    public MutableConnectionState connectionState() {
        return connectionState;
    }

    @Override
    public String defaultDatabase() {
        return this.defaultDatabase;
    }

    @Override
    public void authenticatedAsUser(LoginContext loginContext, String userAgent) {
        this.primaryLoginContext = loginContext;

        channel.updateUser(loginContext.subject().authenticatedUser(), userAgent);
        this.resolveDefaultDatabase();
    }

    @Override
    public void impersonateUser(LoginContext loginContext) {
        this.impersonationLoginContext = loginContext;
        this.resolveDefaultDatabase();
    }

    @Override
    public LoginContext getLoginContext() {
        if (this.impersonationLoginContext != null) {
            return this.impersonationLoginContext;
        }

        return this.primaryLoginContext;
    }

    private void resolveDefaultDatabase() {
        var defaultDatabase = defaultDatabaseResolver.defaultDatabase(
                this.getLoginContext().subject().executingUser());

        this.defaultDatabase = defaultDatabase;
        this.channel.updateDefaultDatabase(defaultDatabase);
    }

    @Override
    public void handleFailure(Throwable cause, boolean fatal) throws BoltConnectionFatality {
        machine.handleFailure(cause, fatal);
    }

    @Override
    public boolean resetMachine() throws BoltConnectionFatality {
        return machine.reset();
    }

    @Override
    public void initStatementProcessorProvider(RoutingContext routingContext) {
        var transactionSpiProvider = spi.transactionStateMachineSPIProvider();
        var statementProcessorProvider = new StatementProcessorProvider(
                transactionSpiProvider, clock, this, routingContext, channel.memoryTracker());
        var initializeContext = new InitializeContext(connectionId(), statementProcessorProvider);

        transactionManager.initialize(initializeContext);
    }

    @Override
    public void releaseStatementProcessor(String transactionId) {
        transactionManager.cleanUp(new CleanUpTransactionContext(transactionId));
        connectionState.clearCurrentTransactionId();
    }
}
