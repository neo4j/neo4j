/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.replication.token;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.InMemoryTokenCache;
import org.neo4j.kernel.impl.core.NonUniqueTokenException;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.collection.NoSuchEntryException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.TokenFactory;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.ResourceLocker;

import static org.neo4j.coreedge.raft.replication.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;

public abstract class ReplicatedTokenHolder<TOKEN extends Token, RECORD extends TokenRecord> extends LifecycleAdapter
        implements TokenHolder<TOKEN>, Replicator.ReplicatedContentListener
{
    protected final Dependencies dependencies;

    private final InMemoryTokenCache<TOKEN> tokenCache;
    private final Replicator replicator;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IdType tokenIdType;
    private final TokenFactory<TOKEN> tokenFactory;
    private final TokenType type;
    private final long timeoutMillis;

    private final TokenFutures tokenFutures = new TokenFutures();
    private final Log log;
    private long lastCommittedIndex = Long.MAX_VALUE;

    // TODO: Clean up all the resolving, which now happens every time with special selection strategies.

    public ReplicatedTokenHolder( Replicator replicator, IdGeneratorFactory idGeneratorFactory, IdType tokenIdType,
                                  Dependencies dependencies, TokenFactory<TOKEN> tokenFactory, TokenType type,
                                  long timeoutMillis, LogProvider logProvider )
    {
        this.replicator = replicator;
        this.idGeneratorFactory = idGeneratorFactory;
        this.tokenIdType = tokenIdType;
        this.dependencies = dependencies;
        this.tokenFactory = tokenFactory;
        this.type = type;
        this.timeoutMillis = timeoutMillis;
        this.tokenCache = new InMemoryTokenCache<>( this.getClass() );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start()
    {
        if ( lastCommittedIndex == Long.MAX_VALUE )
        {
            throw new IllegalStateException( "lastCommittedIndex must be set before start." );
        }
        replicator.subscribe( this );
    }

    @Override
    public void stop() throws Throwable
    {
        replicator.unsubscribe( this );
    }

    @Override
    public void setInitialTokens( List<TOKEN> tokens ) throws NonUniqueTokenException
    {
        tokenCache.clear();
        tokenCache.putAll( tokens );
    }

    @Override
    public void addToken( TOKEN token ) throws NonUniqueTokenException
    {
        tokenCache.put( token );
    }

    @Override
    public int getOrCreateId( String tokenName )
    {
        Integer tokenId = tokenCache.getId( tokenName );
        if ( tokenId != null )
        {
            return tokenId;
        }

        return requestToken( tokenName );
    }

    private int requestToken( String tokenName )
    {
        try( TokenFutures.CompletableFutureTokenId tokenFuture = tokenFutures.createFuture( tokenName ) )
        {
            ReplicatedTokenRequest tokenRequest = new ReplicatedTokenRequest( type, tokenName, createCommands( tokenName ) );
            try
            {
                replicator.replicate( tokenRequest );
                return tokenFuture.get( timeoutMillis, TimeUnit.MILLISECONDS );
            }
            catch ( Replicator.ReplicationFailedException | InterruptedException | ExecutionException | TimeoutException e )
            {
                throw new org.neo4j.graphdb.TransactionFailureException(  "Could not create token", e  );
            }
        }
    }

    private byte[] createCommands( String tokenName )
    {
        StorageEngine storageEngine = dependencies.resolveDependency( StorageEngine.class );
        Collection<StorageCommand> commands = new ArrayList<>();
        TransactionState txState = new TxState();
        int tokenId = Math.toIntExact( idGeneratorFactory.get( tokenIdType ).nextId() );
        createToken( txState, tokenName, tokenId );
        try
        {
            storageEngine.createCommands( commands, txState, ResourceLocker.NONE, Long.MAX_VALUE );
        }
        catch ( CreateConstraintFailureException | TransactionFailureException | ConstraintValidationKernelException e )
        {
            throw new RuntimeException( "Unable to create token '" + tokenName + "'", e );
        }

        return ReplicatedTokenRequestSerializer.createCommandBytes( commands );
    }

    protected abstract void createToken( TransactionState txState, String tokenName, int tokenId );

    @Override
    public TOKEN getTokenById( int id ) throws TokenNotFoundException
    {
        TOKEN result = getTokenByIdOrNull( id );
        if ( result == null )
        {
            throw new TokenNotFoundException( "Token for id " + id );
        }
        return result;
    }

    @Override
    public TOKEN getTokenByIdOrNull( int id )
    {
        return tokenCache.getToken( id );
    }

    @Override
    public int getIdByName( String name )
    {
        Integer id = tokenCache.getId( name );
        if ( id == null )
        {
            return NO_ID;
        }
        return id;
    }

    @Override
    public Iterable<TOKEN> getAllTokens()
    {
        return tokenCache.allTokens();
    }

    @Override
    public void onReplicated( ReplicatedContent content, long logIndex )
    {
        if ( content instanceof ReplicatedTokenRequest && ((ReplicatedTokenRequest) content).type().equals( type ) )
        {
            if ( logIndex > lastCommittedIndex )
            {
                ReplicatedTokenRequest tokenRequest = (ReplicatedTokenRequest) content;

                Integer tokenId = tokenCache.getId( tokenRequest.tokenName() );

                if ( tokenId == null )
                {
                    try
                    {
                        Collection<StorageCommand> commands =  ReplicatedTokenRequestSerializer.extractCommands( tokenRequest.commandBytes() );
                        tokenId = applyToStore( commands, logIndex );
                    }
                    catch ( NoSuchEntryException e )
                    {
                        throw new IllegalStateException( "Commands did not contain token command" );
                    }

                    tokenCache.put( tokenFactory.newToken( tokenRequest.tokenName(), tokenId ) );
                }

                tokenFutures.complete( tokenRequest.tokenName(), tokenId );
            }
            else
            {
                log.info( "Ignoring content at index %d, since already applied up to %d",
                        logIndex, lastCommittedIndex );
            }
        }
    }

    private int applyToStore( Collection<StorageCommand> commands, long logIndex ) throws NoSuchEntryException
    {
        int tokenId = extractTokenId( commands );

        PhysicalTransactionRepresentation representation = new PhysicalTransactionRepresentation( commands );
        representation.setHeader( encodeLogIndexAsTxHeader(logIndex), 0, 0, 0, 0L, 0L, 0 );

        TransactionCommitProcess commitProcess = dependencies.resolveDependency(
                TransactionRepresentationCommitProcess.class );

        try ( LockGroup lockGroup = new LockGroup() )
        {
            commitProcess.commit( new TransactionToApply( representation ),
                    CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }

        return tokenId;
    }

    private int extractTokenId( Collection<StorageCommand> commands ) throws NoSuchEntryException
    {
        for ( StorageCommand command : commands )
        {
            if( command instanceof Command.TokenCommand )
            {
                return ((Command.TokenCommand<? extends TokenRecord>) command).getAfter().getIntId();
            }
        }
        throw new NoSuchEntryException( "Expected command not found" );
    }

    protected abstract RecordAccess.Loader<Integer,RECORD,Void> resolveLoader( TokenStore<RECORD,TOKEN> tokenStore );

    protected abstract Command.TokenCommand<RECORD> createCommand( RECORD before, RECORD after );

    public void setLastCommittedIndex( long lastCommittedIndex )
    {
        this.lastCommittedIndex = lastCommittedIndex;
    }

    @Override
    public int size()
    {
        return tokenCache.size();
    }
}
