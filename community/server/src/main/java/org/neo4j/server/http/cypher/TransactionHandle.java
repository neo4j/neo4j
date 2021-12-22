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
package org.neo4j.server.http.cypher;

import io.netty.channel.embedded.EmbeddedChannel;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.runtime.statemachine.MutableConnectionState;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.runtime.statemachine.impl.BoltStateMachineContextImpl;
import org.neo4j.bolt.runtime.statemachine.impl.BoltStateMachineSPIImpl;
import org.neo4j.bolt.runtime.statemachine.impl.StatementProcessorProvider;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.transaction.CleanUpTransactionContext;
import org.neo4j.bolt.transaction.InitializeContext;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.bolt.transaction.TransactionNotFoundException;
import org.neo4j.bolt.transport.pipeline.ChannelProtector;
import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.bolt.v43.BoltStateMachineV43;
import org.neo4j.bolt.v44.runtime.TransactionStateMachineSPIProviderV44;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.configuration.GraphDatabaseSettings.UNSPECIFIED_TIMEOUT;
import static org.neo4j.kernel.impl.util.ValueUtils.asParameterMapValue;

/**
 * Encapsulates executing statements in a transaction, committing the transaction, or rolling it back.
 *
 * Constructing a {@link TransactionHandle} does not immediately ask the kernel to create a
 * {@link org.neo4j.kernel.api.KernelTransaction}; instead a {@link org.neo4j.kernel.api.KernelTransaction} is
 * only created when the first statements need to be executed.
 *
 * At the end of each statement-executing method, the {@link org.neo4j.kernel.api.KernelTransaction} is either
 * suspended (ready to be resumed by a later operation), or committed, or rolled back.
 *
 * If you acquire instances of this class from {@link TransactionHandleRegistry}, it will prevent concurrent access to
 * the same instance. Therefore the implementation assumes that a single instance will only be accessed from
 * a single thread.
 *
 * All of the public methods on this class are "single-shot"; once you have called one method, the handle returns
 * itself
 * to the registry. If you want to use it again, you'll need to acquire it back from the registry to ensure exclusive
 * use.
 */
public class TransactionHandle implements TransactionTerminationHandle
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( TransactionHandle.class );

    private final String databaseName;
    private final QueryExecutionEngine engine;
    private final TransactionRegistry registry;
    private final TransactionUriScheme uriScheme;
    private final TransactionManager transactionManager;
    private final Type type;
    private final Duration customTransactionTimeout;
    private final long id;
    private long expirationTimestamp = -1;
    private final InternalLogProvider userLogProvider;
    private final BoltGraphDatabaseManagementServiceSPI boltSPI;
    private String txManagerTxId;
    LoginContext loginContext;
    private final ClientConnectionInfo clientConnectionInfo;
    MemoryTracker memoryTracker;
    AuthManager authManager;
    private final SystemNanoClock clock;
    private final boolean readByDefault;

    TransactionHandle( String databaseName, QueryExecutionEngine engine,
                       TransactionRegistry registry, TransactionUriScheme uriScheme, boolean implicitTransaction, LoginContext loginContext,
                       ClientConnectionInfo clientConnectionInfo, long customTransactionTimeoutMillis, TransactionManager transactionManager,
                       InternalLogProvider logProvider, BoltGraphDatabaseManagementServiceSPI boltSPI, MemoryTracker memoryTracker, AuthManager authManager,
                       SystemNanoClock clock, boolean readByDefault )
    {
        this.databaseName = databaseName;
        this.engine = engine;
        this.registry = registry;
        this.uriScheme = uriScheme;
        this.type = implicitTransaction ? Type.IMPLICIT : Type.EXPLICIT;
        this.customTransactionTimeout = customTransactionTimeoutMillis != UNSPECIFIED_TIMEOUT ? Duration.ofMillis( customTransactionTimeoutMillis ) : null;
        this.id = registry.begin( this );
        this.transactionManager = transactionManager;
        this.userLogProvider = logProvider;
        this.boltSPI = boltSPI;
        this.loginContext = loginContext;
        this.clientConnectionInfo = clientConnectionInfo;
        this.memoryTracker = memoryTracker;
        this.authManager = authManager;
        this.clock = clock;
        this.readByDefault = readByDefault;
        setUpStatementProcessor();
    }

    URI uri()
    {
        return uriScheme.txUri( id );
    }

    boolean isImplicit()
    {
        return type == Type.IMPLICIT;
    }

    long getExpirationTimestamp()
    {
        return expirationTimestamp;
    }

    /**
     * this is the id of the transaction from the user's perspective.
     */
    long getId()
    {
        return id;
    }

    @Override
    public boolean terminate()
    {
        transactionManager.interrupt( txManagerTxId );
        return true;
    }

    boolean isPeriodicCommit( String statement )
    {
        return engine.isPeriodicCommit( statement );
    }

    void ensureActiveTransaction() throws KernelException
    {
        if ( txManagerTxId == null )
        {
            beginTransaction();
        }
    }

    StatementMetadata executeStatement( Statement statement, boolean periodicCommit ) throws KernelException, TransactionNotFoundException
    {
        if ( periodicCommit )
        {
            var programResultReference = transactionManager.runProgram(
                    UUID.randomUUID().toString(), // todo: verify the uuid sent here
                    loginContext, databaseName, statement.getStatement(),
                    asParameterMapValue( statement.getParameters() ), Collections.emptyList(),
                    readByDefault, Collections.emptyMap(),
                    customTransactionTimeout,
                    Long.toString( id ) );
            txManagerTxId = programResultReference.transactionId();
            return programResultReference.statementMetadata();
        }

        return transactionManager.runQuery( txManagerTxId, statement.getStatement(), asParameterMapValue( statement.getParameters() ) );
    }

    void forceRollback() throws TransactionNotFoundException
    {
        transactionManager.rollback( txManagerTxId );
    }

    void suspendTransaction()
    {
        expirationTimestamp = registry.release( id, this );
    }

    void commit() throws KernelException, TransactionNotFoundException
    {
        try
        {
            transactionManager.commit( txManagerTxId );
        }
        finally
        {
            transactionManager.cleanUp( new CleanUpTransactionContext( txManagerTxId ) );
            registry.forget( id );
        }
    }

    void rollback()
    {
        try
        {
            transactionManager.rollback( txManagerTxId );
        }
        catch ( TransactionNotFoundException ex )
        {
            //ignore - Transaction already rolled back by release mechanism.
        }
        finally
        {
            registry.forget( id );
            transactionManager.cleanUp( new CleanUpTransactionContext( Long.toString( id ) ) );
        }
    }

    boolean hasTransactionContext()
    {
        return txManagerTxId != null;
    }

    void closeTransactionForPeriodicCommit() throws TransactionNotFoundException
    {
        transactionManager.rollback( txManagerTxId );
    }

    void reopenAfterPeriodicCommit() throws KernelException
    {
        beginTransaction();
    }

    /*
    This is an ugly temporary measure to enable the HTTP server to use the Bolt Implementation of TransactionManager.
    This will be removed completely when a global transaction aware TransactionManager is implemented.
     */
    private void setUpStatementProcessor()
    {
        var boltChannel = new DummyBoltChannel( Long.toString( id ), clientConnectionInfo );

        var transactionStateMachineSPIProvider =
                new TransactionStateMachineSPIProviderV44( boltSPI, boltChannel, clock, memoryTracker );

        var boltStateMachineSPI = new BoltStateMachineSPIImpl( new SimpleLogService( userLogProvider ),
                                                               new BasicAuthentication( authManager ), transactionStateMachineSPIProvider, boltChannel );

        var boltStateMachine = new BoltStateMachineV43( boltStateMachineSPI, boltChannel, clock,
                                                        fixedHttpDatabaseResolver(), MapValue.EMPTY, memoryTracker, transactionManager );

        var statementProcessorReleaseManager = new BoltStateMachineContextImpl( boltStateMachine, boltChannel, boltStateMachineSPI,
                                                                                new MutableConnectionState(), clock, fixedHttpDatabaseResolver(),
                                                                                memoryTracker, transactionManager );

        var statementProcessorProvider =
                new StatementProcessorProvider( transactionStateMachineSPIProvider, clock, statementProcessorReleaseManager,
                                                new RoutingContext( true, Collections.emptyMap() ),
                                                memoryTracker );

        transactionManager.initialize( new InitializeContext( Long.toString( getId() ), statementProcessorProvider ) );
    }

    /**
     * Since the selected database comes from the URI we have a custom resolver that simply returns it.
     */
    private DefaultDatabaseResolver fixedHttpDatabaseResolver()
    {
        return new DefaultDatabaseResolver()
        {
            @Override
            public String defaultDatabase( String ignored )
            {
                return databaseName;
            }

            @Override
            public void clearCache()
            {
                //not needed
            }
        };
    }

    public void beginTransaction() throws KernelException
    {
        txManagerTxId = transactionManager.begin( loginContext, databaseName, Collections.emptyList(), readByDefault,
                                                  Collections.emptyMap(), customTransactionTimeout,
                                                  Long.toString( id ) );
    }

    public TransactionManager transactionManager()
    {
        return transactionManager;
    }

    public String getTxManagerTxId()
    {
        return txManagerTxId;
    }

    private static class DummyBoltChannel extends BoltChannel
    {
        private final ClientConnectionInfo info;

        DummyBoltChannel( String id, ClientConnectionInfo info )
        {
            super( id, info.protocol(), new EmbeddedChannel(), ChannelProtector.NULL );
            this.info = info;
        }

        @Override
        public ClientConnectionInfo info()
        {
            return info;
        }

    }
}
