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
package org.neo4j.bolt.transaction;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.runtime.statemachine.impl.StatementProcessorProvider;
import org.neo4j.bolt.v4.messaging.DiscardResultConsumer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.virtual.MapValue;

/**
 * Implementation which uses the bolt server's existing Transaction management mechanism ({@link StatementProcessor}).
 */
public class StatementProcessorTxManager implements TransactionManager
{
    private final Map<String,StatementProcessor> statementProcessors = new ConcurrentHashMap<>();
    private final Map<String,StatementProcessorProvider> statementProcessorProviders = new ConcurrentHashMap<>();

    @Override
    public String begin( LoginContext loginContext, String defaultDb, List<Bookmark> bookmarks, boolean isReadOnly, Map<String,Object> transactionMetadata,
                         Duration transactionTimeout, String connectionId )
            throws KernelException
    {
        String txId = UUID.randomUUID().toString();
        StatementProcessor newTxProcessor = retrieveStatementProcessor( connectionId, loginContext, defaultDb, txId );
        var accessMode = isReadOnly ? AccessMode.READ : AccessMode.WRITE;
        newTxProcessor.beginTransaction( bookmarks, transactionTimeout, accessMode, transactionMetadata );
        statementProcessors.put( txId, newTxProcessor );
        return txId;
    }

    @Override
    public Bookmark commit( String txId ) throws KernelException, TransactionNotFoundException
    {
        Bookmark bookmark = retrieveTx( txId ).commitTransaction();
        statementProcessors.remove( txId );
        return bookmark;
    }

    @Override
    public void rollback( String txId ) throws TransactionNotFoundException
    {
        try
        {
            retrieveTx( txId ).reset();
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            statementProcessors.remove( txId );
        }
    }

    @Override
    public StatementMetadata runQuery( String txReference, String cypherQuery, MapValue params ) throws KernelException, TransactionNotFoundException
    {
        StatementProcessor statementProcessor = retrieveTx( txReference );
        return statementProcessor.run( cypherQuery, params );
    }

    @Override
    public ProgramResultReference runProgram( String programId, LoginContext loginContext, String defaultDb, String cypherProgram, MapValue params,
                                              List<Bookmark> bookmarks, boolean isReadOnly, Map<String,Object> programMetadata, Duration programTimeout,
                                              String connectionId ) throws KernelException
    {
        var statementProcessor = retrieveStatementProcessor( connectionId, loginContext, defaultDb, programId );
        statementProcessors.put( programId, statementProcessor );
        var accessMode = isReadOnly ? AccessMode.READ : AccessMode.WRITE;
        var metadata = statementProcessor.run( cypherProgram, params, bookmarks, programTimeout, accessMode, programMetadata );
        return new DefaultProgramResultReference( programId, metadata );
    }

    @Override
    public Bookmark pullData( String txId, int statementId, long numberToPull, ResultConsumer recordConsumer )
            throws ResultNotFoundException, TransactionNotFoundException
    {
        return streamResults( txId, statementId, recordConsumer );
    }

    @Override
    public Bookmark discardData( String txId, int statementId, long numberToDiscard, ResultConsumer resultConsumer )
            throws ResultNotFoundException, TransactionNotFoundException
    {
        return streamResults( txId, statementId, resultConsumer );
    }

    @Override
    public void cancelData( String txId, int statementId ) throws ResultNotFoundException, TransactionNotFoundException
    {
        discardData( txId, statementId, -1, new DiscardResultConsumer( null, -1 ) );
    }

    @Override
    public void interrupt( String txReference )
    {
        var statementProcessor = txReference != null ? statementProcessors.get( txReference ) : null;
        if ( statementProcessor != null )
        {
            statementProcessor.markCurrentTransactionForTermination();
        }
    }

    @Override
    public TransactionStatus transactionStatus( String txId )
    {
        var tx = statementProcessors.get( txId );

        if ( tx != null )
        {
            try
            {
                var status = tx.validateTransaction();
                if ( status != null )
                {
                    return new TransactionStatus( TransactionStatus.Value.INTERRUPTED, status );
                }
                else if ( tx.hasOpenStatement() )
                {
                    return new TransactionStatus( TransactionStatus.Value.IN_TRANSACTION_OPEN_STATEMENT );
                }
                else
                {
                    return new TransactionStatus( TransactionStatus.Value.IN_TRANSACTION_NO_OPEN_STATEMENTS );
                }
            }
            catch ( KernelException ex )
            {
                throw new RuntimeException( ex );
            }
        }
        else
        {
            return new TransactionStatus( TransactionStatus.Value.CLOSED_OR_DOES_NOT_EXIST );
        }
    }

    @Override
    public void cleanUp( CleanUpTransactionContext cleanUpTransactionContext )
    {
        statementProcessors.remove( cleanUpTransactionContext.transactionId() );
    }

    @Override
    public void cleanUp( CleanUpConnectionContext cleanUpConnectionContext )
    {
        statementProcessorProviders.remove( cleanUpConnectionContext.connectionId() );
    }

    @Override
    public void initialize( InitializeContext initializeContext )
    {
        statementProcessorProviders.computeIfAbsent( initializeContext.connectionId(), key -> initializeContext.statementProcessorProvider() );
    }

    public void removeStatementProcessorProvider( String connectionId )
    {
        statementProcessorProviders.remove( connectionId );
    }

    @VisibleForTesting
    public int getCurrentNoOfOpenTx()
    {
        return statementProcessors.size();
    }

    private StatementProcessor retrieveTx( String txId ) throws TransactionNotFoundException
    {
        var statementProcessor = statementProcessors.get( txId );

        if ( statementProcessor == null )
        {
            throw new TransactionNotFoundException( txId );
        }

        return statementProcessor;
    }

    private StatementProcessor retrieveStatementProcessor( String connectionId, LoginContext loginContext, String databaseName, String txId )
    {
        StatementProcessor statementProcessor;
        try
        {
            StatementProcessorProvider statementProcessorProvider = retrieveStatementProcessorProvider( connectionId );
            statementProcessor = statementProcessorProvider.getStatementProcessor( loginContext, databaseName, txId );
        }
        catch ( BoltProtocolBreachFatality | BoltIOException boltProtocolBreachFatality )
        {
            throw new RuntimeException( boltProtocolBreachFatality );
        }

        if ( statementProcessor == null )
        {
            throw new RuntimeException( "StatementProcessor for connectionId: " + connectionId + " not found." );
        }

        return statementProcessor;
    }

    private StatementProcessorProvider retrieveStatementProcessorProvider( String connectionId )
    {
        StatementProcessorProvider statementProcessorProvider = statementProcessorProviders.get( connectionId );
        if ( statementProcessorProvider == null )
        {
            throw new RuntimeException( "StatementProcessorProvider for connectionId: " + connectionId + " not found." );
        }
        else
        {
            return statementProcessorProvider;
        }
    }

    private Bookmark streamResults( String txId, int statementId, ResultConsumer recordConsumer ) throws TransactionNotFoundException, ResultNotFoundException
    {
        var statementProcessor = retrieveTx( txId );
        try
        {
            return statementProcessor.streamResult( statementId, recordConsumer );
        }
        catch ( IllegalArgumentException ex )
        {
            throw new ResultNotFoundException( txId, statementId );
        }
        catch ( RuntimeException ex )
        {
            // preserve RuntimeException as is
            throw ex;
        }
        catch ( Throwable ex )
        {
            // just rethrow
            throw new RuntimeException( ex );
        }
    }
}
