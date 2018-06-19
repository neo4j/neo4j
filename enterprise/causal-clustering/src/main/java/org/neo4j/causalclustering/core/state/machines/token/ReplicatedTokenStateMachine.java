/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.token;

import java.util.Collection;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.StateMachine;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.TokenRegistry;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.collection.NoSuchEntryException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static java.lang.String.format;
import static org.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;

public class ReplicatedTokenStateMachine implements StateMachine<ReplicatedTokenRequest>
{
    private TransactionCommitProcess commitProcess;

    private final TokenRegistry tokenRegistry;
    private final VersionContext versionContext;

    private final Log log;
    private long lastCommittedIndex = -1;

    public ReplicatedTokenStateMachine( TokenRegistry tokenRegistry,
            LogProvider logProvider, VersionContextSupplier versionContextSupplier )
    {
        this.tokenRegistry = tokenRegistry;
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
            return;
        }

        Integer tokenId = tokenRegistry.getId( tokenRequest.tokenName() );

        if ( tokenId == null )
        {
            try
            {
                Collection<StorageCommand> commands =
                        ReplicatedTokenRequestSerializer.extractCommands( tokenRequest.commandBytes() );
                tokenId = applyToStore( commands, commandIndex );
            }
            catch ( NoSuchEntryException e )
            {
                throw new IllegalStateException( "Commands did not contain token command" );
            }

            tokenRegistry.put( new NamedToken( tokenRequest.tokenName(), tokenId ) );
        }

        callback.accept( Result.of( tokenId ) );
    }

    private int applyToStore( Collection<StorageCommand> commands, long logIndex ) throws NoSuchEntryException
    {
        int tokenId = extractTokenId( commands );

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

        return tokenId;
    }

    private int extractTokenId( Collection<StorageCommand> commands ) throws NoSuchEntryException
    {
        for ( StorageCommand command : commands )
        {
            if ( command instanceof Command.TokenCommand )
            {
                return ((Command.TokenCommand<? extends TokenRecord>) command).getAfter().getIntId();
            }
        }
        throw new NoSuchEntryException( "Expected command not found" );
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
            /** See {@link #installCommitProcess}. */
            throw new IllegalStateException( "Value has not been installed" );
        }
        return lastCommittedIndex;
    }
}
