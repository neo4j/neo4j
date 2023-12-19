/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state.machines.token;

import java.util.Collection;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.StateMachine;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.TokenFactory;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static java.lang.String.format;
import static org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestSerializer.extractCommands;
import static org.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;

public class ReplicatedTokenStateMachine<TOKEN extends Token> implements StateMachine<ReplicatedTokenRequest>
{
    private TransactionCommitProcess commitProcess;

    private final TokenRegistry<TOKEN> tokenRegistry;
    private final TokenFactory<TOKEN> tokenFactory;
    private final VersionContext versionContext;

    private final Log log;
    private long lastCommittedIndex = -1;

    public ReplicatedTokenStateMachine( TokenRegistry<TOKEN> tokenRegistry, TokenFactory<TOKEN> tokenFactory,
            LogProvider logProvider, VersionContextSupplier versionContextSupplier )
    {
        this.tokenRegistry = tokenRegistry;
        this.tokenFactory = tokenFactory;
        this.versionContext = versionContextSupplier.getVersionContext();
        this.log = logProvider.getLog( getClass() );
    }

    public synchronized void installCommitProcess( TransactionCommitProcess commitProcess, long lastCommittedIndex )
    {
        this.commitProcess = commitProcess;
        this.lastCommittedIndex = lastCommittedIndex;
        log.info( format("(%s) Updated lastCommittedIndex to %d", tokenRegistry.getTokenType(), lastCommittedIndex) );
    }

    @Override
    public synchronized void applyCommand( ReplicatedTokenRequest tokenRequest, long commandIndex,
            Consumer<Result> callback )
    {
        if ( commandIndex <= lastCommittedIndex )
        {
            log.warn( format( "Ignored %s because already committed (%d <= %d).", tokenRequest, commandIndex, lastCommittedIndex ) );
            return;
        }

        Collection<StorageCommand> commands = extractCommands( tokenRequest.commandBytes() );
        int newTokenId = extractTokenId( commands );

        Integer existingTokenId = tokenRegistry.getId( tokenRequest.tokenName() );

        if ( existingTokenId == null )
        {
            log.info( format( "Applying %s with newTokenId=%d", tokenRequest, newTokenId ) );
            applyToStore( commands, commandIndex );
            tokenRegistry.addToken( tokenFactory.newToken( tokenRequest.tokenName(), newTokenId ) );
            callback.accept( Result.of( newTokenId ) );
        }
        else
        {
            // This should be rare so a warning is in order.
            log.warn( format( "Ignored %s (newTokenId=%d) since it already exists with existingTokenId=%d", tokenRequest, newTokenId, existingTokenId ) );
            callback.accept( Result.of( existingTokenId ) );
        }
    }

    private void applyToStore( Collection<StorageCommand> commands, long logIndex )
    {
        PhysicalTransactionRepresentation representation = new PhysicalTransactionRepresentation( commands );
        representation.setHeader( encodeLogIndexAsTxHeader( logIndex ), 0, 0, 0, 0L, 0L, 0 );

        try ( LockGroup ignored = new LockGroup() )
        {
            commitProcess.commit( new TransactionToApply( representation, versionContext ), CommitEvent.NULL,
                    TransactionApplicationMode.EXTERNAL );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    private int extractTokenId( Collection<StorageCommand> commands )
    {
        for ( StorageCommand command : commands )
        {
            if ( command instanceof Command.TokenCommand )
            {
                return ((Command.TokenCommand<? extends TokenRecord>) command).getAfter().getIntId();
            }
        }
        throw new IllegalStateException( "Commands did not contain token command" );
    }

    @Override
    public synchronized void flush()
    {
        // already implicitly flushed to the store
    }

    @Override
    public long lastAppliedIndex()
    {
        if ( commitProcess == null )
        {
            /* See {@link #installCommitProcess}. */
            throw new IllegalStateException( "Value has not been installed" );
        }
        return lastCommittedIndex;
    }
}
