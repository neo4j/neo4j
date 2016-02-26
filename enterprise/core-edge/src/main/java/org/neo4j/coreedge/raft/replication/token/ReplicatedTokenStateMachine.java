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

import java.io.IOException;
import java.util.Collection;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.server.core.RecoverTransactionLogState;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.collection.NoSuchEntryException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.TokenFactory;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.coreedge.raft.replication.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;

public class ReplicatedTokenStateMachine<TOKEN extends Token> implements StateMachine
{
    protected final Dependencies dependencies;

    private final TokenRegistry<TOKEN> tokenRegistry;
    private final TokenFactory<TOKEN> tokenFactory;
    private final TokenType type;

    private final Log log;
    private long lastCommittedIndex = Long.MAX_VALUE;

    // TODO: Clean up all the resolving, which now happens every time with special selection strategies.

    public ReplicatedTokenStateMachine( TokenRegistry<TOKEN> tokenRegistry,
                                        Dependencies dependencies, TokenFactory<TOKEN> tokenFactory, TokenType type,
                                        LogProvider logProvider, RecoverTransactionLogState txLogState )
    {
        this.tokenRegistry = tokenRegistry;
        this.dependencies = dependencies;
        this.tokenFactory = tokenFactory;
        this.type = type;
        this.log = logProvider.getLog( getClass() );
        this.lastCommittedIndex = txLogState.findLastCommittedIndex();
        verifyLastCommittedIndexInBounds();
    }

    private void verifyLastCommittedIndexInBounds()
    {
        if ( lastCommittedIndex == Long.MAX_VALUE )
        {
            throw new IllegalStateException( "lastCommittedIndex must be set before start." );
        }
    }

    @Override
    public void applyCommand( ReplicatedContent content, long logIndex )
    {
        if ( content instanceof ReplicatedTokenRequest && ((ReplicatedTokenRequest) content).type().equals( type ) )
        {
            if ( logIndex > lastCommittedIndex )
            {
                ReplicatedTokenRequest tokenRequest = (ReplicatedTokenRequest) content;

                Integer tokenId = tokenRegistry.getId( tokenRequest.tokenName() );

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

                    tokenRegistry.addToken( tokenFactory.newToken( tokenRequest.tokenName(), tokenId ) );
                }

                tokenRegistry.complete( tokenRequest.tokenName(), tokenId );
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

    public void setLastCommittedIndex( long lastCommittedIndex )
    {
        this.lastCommittedIndex = lastCommittedIndex;
    }

    @Override
    public void flush() throws IOException
    {
        // already implicitly flushed to the transaction log.
    }
}
