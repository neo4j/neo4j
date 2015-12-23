/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.InMemoryTokenCache;
import org.neo4j.kernel.impl.core.NonUniqueTokenException;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.core.TokenFactory;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.transaction.state.RecordChanges;
import org.neo4j.kernel.impl.transaction.state.TokenCreator;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.collection.NoSuchEntryException;
import org.neo4j.kernel.impl.util.statistics.IntCounter;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

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

    private final TokenFutures tokenFutures = new TokenFutures();
    private long lastCommittedIndex = Long.MAX_VALUE;

    // TODO: Clean up all the resolving, which now happens every time with special selection strategies.

    public ReplicatedTokenHolder( Replicator replicator, IdGeneratorFactory idGeneratorFactory, IdType tokenIdType,
            Dependencies dependencies, TokenFactory<TOKEN> tokenFactory, TokenType type )
    {
        this.replicator = replicator;
        this.idGeneratorFactory = idGeneratorFactory;
        this.tokenIdType = tokenIdType;
        this.dependencies = dependencies;
        this.tokenFactory = tokenFactory;
        this.type = type;
        this.tokenCache = new InMemoryTokenCache<>( this.getClass() );
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
        {
            Integer tokenId = tokenCache.getId( tokenName );
            if ( tokenId != null )
            {
                return tokenId;
            }
        }

        return requestToken( tokenName );
    }

    private int requestToken( String tokenName )
    {
        ReplicatedTokenRequest tokenRequest = new ReplicatedTokenRequest( type, tokenName, createCommands( tokenName ) );

        try( TokenFutures.CompletableFutureTokenId tokenFuture = tokenFutures.createFuture( tokenName ) )
        {
            try
            {
                replicator.replicate( tokenRequest );
            }
            catch ( Replicator.ReplicationFailedException e )
            {
                // TODO: This is really a NoLeaderTimeoutException... should clarify exception and semantics of the
                // TODO: replicator interface. e.g. who is responsible for retry, does the exception imply that replication
                // TODO: might have occurred, etc.
                throw new RuntimeException( "Replication failures not currently handled." );
            }

            try
            {
                return tokenFuture.get();
            }
            catch ( InterruptedException | ExecutionException e )
            {
                // TODO: Handle exceptions.
                throw new RuntimeException( "Future exceptions currently not handled." );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error closing future.", e );
        }
    }

    private byte[] createCommands( String tokenName )
    {
        long tokenId = idGeneratorFactory.get( tokenIdType ).nextId();

        TokenStore<RECORD,TOKEN> tokenStore = resolveStore();
        RecordAccess.Loader<Integer,RECORD,Void> recordLoader = resolveLoader( tokenStore );

        RecordChanges<Integer,RECORD,Void> recordAccess = new RecordChanges<>( recordLoader, new IntCounter() );
        TokenCreator<RECORD,TOKEN> tokenCreator = new TokenCreator<>( tokenStore );
        tokenCreator.createToken( tokenName, (int) tokenId, recordAccess );

        Collection<Command> commands = new ArrayList<>();
        for ( RecordAccess.RecordProxy<Integer,RECORD, Void> record : recordAccess.changes() )
        {
            commands.add( createCommand( record.getBefore(), record.forReadingLinkage() ) );
        }

        return ReplicatedTokenRequestSerializer.createCommandBytes( commands );
    }

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
        if ( logIndex <= lastCommittedIndex )
        {
            return;
        }
        if ( content instanceof ReplicatedTokenRequest && ((ReplicatedTokenRequest) content).type().equals( type ) )
        {
            ReplicatedTokenRequest tokenRequest = (ReplicatedTokenRequest) content;

            Integer tokenId = tokenCache.getId( tokenRequest.tokenName() );

            if ( tokenId == null )
            {
                try
                {
                    Collection<Command> commands =  ReplicatedTokenRequestSerializer.extractCommands( tokenRequest.commandBytes() );
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
    }

    private int applyToStore( Collection<Command> commands, long logIndex ) throws NoSuchEntryException
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

    private int extractTokenId( Collection<Command> commands ) throws NoSuchEntryException
    {
        for ( Command command : commands )
        {
            if( command instanceof Command.TokenCommand )
            {
                return ((Command.TokenCommand<? extends TokenRecord>) command).getAfter().getId();
            }
        }
        throw new NoSuchEntryException( "Expected command not found" );
    }

    protected abstract RecordAccess.Loader<Integer,RECORD,Void> resolveLoader( TokenStore<RECORD,TOKEN> tokenStore );

    protected abstract TokenStore<RECORD,TOKEN> resolveStore();

    protected abstract Command.TokenCommand<RECORD> createCommand( RECORD before, RECORD after );

    public void setLastCommittedIndex( long lastCommittedIndex )
    {
        this.lastCommittedIndex = lastCommittedIndex;
    }
}
