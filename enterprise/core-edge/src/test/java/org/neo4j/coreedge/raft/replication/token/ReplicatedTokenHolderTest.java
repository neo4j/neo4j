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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import org.neo4j.coreedge.raft.replication.DirectReplicator;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.coreedge.raft.state.StateMachine;
import org.neo4j.coreedge.raft.state.StateMachines;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequestSerializer.createCommandBytes;
import static org.neo4j.coreedge.raft.replication.token.TokenType.LABEL;
import static org.neo4j.coreedge.raft.replication.tx.LogIndexTxHeaderEncoding.decodeLogIndexFromTxHeader;

public class ReplicatedTokenHolderTest
{
    final int EXPECTED_TOKEN_ID = 1;
    final int INJECTED_TOKEN_ID = 1024;
    Dependencies dependencies = mock( Dependencies.class );

    long TIMEOUT_MILLIS = 1000;

    @Before
    public void setup()
    {
        NeoStores neoStore = mock( NeoStores.class );
        LabelTokenStore labelTokenStore = mock( LabelTokenStore.class );
        when( neoStore.getLabelTokenStore() ).thenReturn( labelTokenStore );
        when( labelTokenStore.allocateNameRecords( Matchers.<byte[]>any() ) ).thenReturn( singletonList( new
                DynamicRecord( 1l ) ) );
        when( dependencies.resolveDependency( NeoStores.class ) ).thenReturn( neoStore );
        when( dependencies.resolveDependency( TransactionRepresentationCommitProcess.class ) ).thenReturn( mock(
                TransactionRepresentationCommitProcess.class ) );
    }

    @Test
    public void shouldCreateTokenId() throws Exception
    {
        // given
        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        IdGenerator idGenerator = mock( IdGenerator.class );
        when( idGenerator.nextId() ).thenReturn( 1L );

        when( idGeneratorFactory.get( any( IdType.class ) ) ).thenReturn( idGenerator );

        StateMachines stateMachines = new StateMachines();
        Replicator replicator = new DirectReplicator( stateMachines );

        when( dependencies.resolveDependency( TransactionRepresentationCommitProcess.class ) )
                .thenReturn( mock( TransactionRepresentationCommitProcess.class ) );
        StorageEngine storageEngine = mockedStorageEngine();
        when( dependencies.resolveDependency( StorageEngine.class ) ).thenReturn( storageEngine );

        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies, TIMEOUT_MILLIS, NullLogProvider.getInstance() );

        stateMachines.add( tokenHolder );

        tokenHolder.setLastCommittedIndex( -1 );
        tokenHolder.start();

        // when
        int tokenId = tokenHolder.getOrCreateId( "Person" );

        // then
        assertEquals( EXPECTED_TOKEN_ID, tokenId );
    }

    @Test
    public void shouldTimeoutIfTokenDoesNotReplicateWithinTimeout() throws Exception
    {
        // given
        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        IdGenerator idGenerator = mock( IdGenerator.class );
        when( idGenerator.nextId() ).thenReturn( 1L );

        when( idGeneratorFactory.get( any( IdType.class ) ) ).thenReturn( idGenerator );

        Replicator replicator = new DropAllTheThingsReplicator();
        when( dependencies.resolveDependency( TransactionRepresentationCommitProcess.class ) )
                .thenReturn( mock( TransactionRepresentationCommitProcess.class ) );
        StorageEngine storageEngine = mockedStorageEngine();
        when( dependencies.resolveDependency( StorageEngine.class ) ).thenReturn( storageEngine );

        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies, 10, NullLogProvider.getInstance() );

        tokenHolder.setLastCommittedIndex( -1 );
        tokenHolder.start();

        // when
        try
        {
            tokenHolder.getOrCreateId( "Person" );
            fail( "Token creation attempt should have timed out" );
        }
        catch ( TransactionFailureException ex )
        {
            // expected
        }
    }

    @Test
    public void shouldStoreRaftLogIndexInTransactionHeader() throws Exception
    {
        // given
        int logIndex = 1;

        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        IdGenerator idGenerator = mock( IdGenerator.class );
        when( idGenerator.nextId() ).thenReturn( 1L );
        when( idGeneratorFactory.get( any( IdType.class ) ) ).thenReturn( idGenerator );

        StubTransactionCommitProcess commitProcess = new StubTransactionCommitProcess( null, null );
        when( dependencies.resolveDependency( TransactionRepresentationCommitProcess.class ) ).thenReturn(
                commitProcess );
        StorageEngine storageEngine = mockedStorageEngine();
        when( dependencies.resolveDependency( StorageEngine.class ) ).thenReturn( storageEngine );

        StateMachines stateMachines = new StateMachines();
        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder(
                new DirectReplicator( stateMachines ), idGeneratorFactory, dependencies, TIMEOUT_MILLIS, NullLogProvider.getInstance() );
        tokenHolder.setLastCommittedIndex( -1 );
        stateMachines.add( tokenHolder );

        // when
        ReplicatedTokenRequest tokenRequest = new ReplicatedTokenRequest( LABEL, "Person",
                createCommandBytes( createCommands( EXPECTED_TOKEN_ID ) ) );
        tokenHolder.applyCommand( tokenRequest, logIndex );

        // then
        List<TransactionRepresentation> transactions = commitProcess.transactionsToApply;
        assertEquals(1, transactions.size());
        assertEquals(logIndex, decodeLogIndexFromTxHeader( transactions.get( 0 ).additionalHeader()) );
    }

    @Test
    public void shouldStoreInitialTokens() throws Exception
    {
        // given
        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder =
                new ReplicatedLabelTokenHolder( null, null, dependencies, TIMEOUT_MILLIS, NullLogProvider.getInstance() );

        // when
        tokenHolder.setInitialTokens( asList( new Token( "name1", 1 ), new Token( "name2", 2 ) ) );

        // then
        assertThat( tokenHolder.getAllTokens(), hasItems( new Token( "name1", 1 ), new Token( "name2", 2 ) ) );
    }

    @Test
    public void shouldThrowExceptionIfLastCommittedIndexNotSet() throws Exception
    {
        // given
        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( null,
                null, dependencies, TIMEOUT_MILLIS, NullLogProvider.getInstance() );

        // when
        try
        {
            tokenHolder.start();
            fail( "Should have thrown exception" );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }

    @Test
    public void shouldGetExistingTokenIdFromAgesAgo() throws Exception
    {
        // given
        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        IdGenerator idGenerator = mock( IdGenerator.class );
        when( idGenerator.nextId() ).thenReturn( 1024L );

        when( idGeneratorFactory.get( any( IdType.class ) ) ).thenReturn( idGenerator );

        StateMachines stateMachines = new StateMachines();
        Replicator replicator = new DirectReplicator( stateMachines );

        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies, TIMEOUT_MILLIS, NullLogProvider.getInstance() );

        stateMachines.add( tokenHolder );

        tokenHolder.setLastCommittedIndex( -1 );
        tokenHolder.start();
        tokenHolder.applyCommand( new ReplicatedTokenRequest( LABEL, "Person", createCommandBytes(
                createCommands( EXPECTED_TOKEN_ID ) ) ), 0 );

        // when
        int tokenId = tokenHolder.getOrCreateId( "Person" );

        // then
        assertEquals( EXPECTED_TOKEN_ID, tokenId );
    }

    @Test
    public void shouldStoreAndReturnASingleTokenForTwoConcurrentRequests() throws Exception
    {
        // given
        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        IdGenerator idGenerator = mock( IdGenerator.class );
        when( idGenerator.nextId() ).thenReturn( 1L );

        when( idGeneratorFactory.get( any( IdType.class ) ) ).thenReturn( idGenerator );

        StorageEngine storageEngine = mockedStorageEngine();
        when( dependencies.resolveDependency( StorageEngine.class ) ).thenReturn( storageEngine );

        StateMachines stateMachines = new StateMachines();
        RaceConditionSimulatingReplicator replicator = new RaceConditionSimulatingReplicator(stateMachines);

        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies, TIMEOUT_MILLIS, NullLogProvider.getInstance() );
        stateMachines.add( tokenHolder );

        tokenHolder.setLastCommittedIndex( -1 );
        tokenHolder.start();
        replicator.injectLabelTokenBeforeOtherOneReplicates( new ReplicatedTokenRequest( LABEL, "Person",
                createCommandBytes( createCommands( INJECTED_TOKEN_ID ) ) ) );

        // when
        int tokenId = tokenHolder.getOrCreateId( "Person" );

        // then
        assertEquals( INJECTED_TOKEN_ID, tokenId );
    }

    @Test
    public void shouldStoreAndReturnASingleTokenForTwoDifferentConcurrentRequests() throws Exception
    {
        // given
        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        IdGenerator idGenerator = mock( IdGenerator.class );
        when( idGenerator.nextId() ).thenReturn( 1L );

        when( idGeneratorFactory.get( any( IdType.class ) ) ).thenReturn( idGenerator );

        StorageEngine storageEngine = mockedStorageEngine();
        when( dependencies.resolveDependency( StorageEngine.class ) ).thenReturn( storageEngine );

        StateMachines stateMachines = new StateMachines();
        RaceConditionSimulatingReplicator replicator = new RaceConditionSimulatingReplicator( stateMachines );

        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies, TIMEOUT_MILLIS, NullLogProvider.getInstance() );
        stateMachines.add( tokenHolder );

        tokenHolder.setLastCommittedIndex( -1 );
        tokenHolder.start();
        replicator.injectLabelTokenBeforeOtherOneReplicates( new ReplicatedTokenRequest( LABEL, "Dog",
                createCommandBytes( createCommands( INJECTED_TOKEN_ID ) ) ) );

        // when
        int tokenId = tokenHolder.getOrCreateId( "Person" );

        // then
        assertEquals( EXPECTED_TOKEN_ID, tokenId );
    }

    private List<StorageCommand> createCommands( int tokenId )
    {
        List<StorageCommand> commands = new ArrayList<>();
        commands.add(
                new Command.LabelTokenCommand( new LabelTokenRecord( tokenId ), new LabelTokenRecord( tokenId ) ) );
        return commands;
    }

    static class RaceConditionSimulatingReplicator implements Replicator
    {
        private final StateMachine stateMachine;
        private ReplicatedTokenRequest otherToken;

        public RaceConditionSimulatingReplicator(StateMachine stateMachine )
        {
            this.stateMachine = stateMachine;
        }

        public void injectLabelTokenBeforeOtherOneReplicates( ReplicatedTokenRequest token )
        {
            this.otherToken = token;
        }

        @Override
        public void replicate( final ReplicatedContent content ) throws ReplicationFailedException
        {
            if ( otherToken != null )
            {
                stateMachine.applyCommand( otherToken, 0 );
            }
            stateMachine.applyCommand( content, 0 );
        }

    }

    static class DropAllTheThingsReplicator implements Replicator
    {
        @Override
        public void replicate( final ReplicatedContent content ) throws ReplicationFailedException
        {
        }
    }

    private class StubTransactionCommitProcess extends TransactionRepresentationCommitProcess
    {
        private final List<TransactionRepresentation> transactionsToApply = new ArrayList<>();

        public StubTransactionCommitProcess( TransactionAppender appender, StorageEngine storageEngine )
        {
            super( appender, storageEngine );
        }

        @Override
        public long commit( TransactionToApply batch, CommitEvent commitEvent, TransactionApplicationMode mode )
                throws TransactionFailureException
        {
            transactionsToApply.add( batch.transactionRepresentation() );
            return -1;
        }
    }

    private StorageEngine mockedStorageEngine() throws Exception
    {
        StorageEngine storageEngine = mock( StorageEngine.class );
        doAnswer( invocation -> {
            Collection<StorageCommand> target = invocation.getArgumentAt( 0, Collection.class );
            ReadableTransactionState txState = invocation.getArgumentAt( 1, ReadableTransactionState.class );
            txState.accept( new TxStateVisitor.Adapter()
            {
                @Override
                public void visitCreatedLabelToken( String name, int id )
                {
                    LabelTokenRecord before = new LabelTokenRecord( id );
                    LabelTokenRecord after = before.clone();
                    after.setInUse( true );
                    target.add( new Command.LabelTokenCommand( before, after ) );
                }
            } );
            return null;
        } ).when( storageEngine ).createCommands( anyCollection(), any( ReadableTransactionState.class ),
                any( ResourceLocker.class ), anyLong() );
        return storageEngine;
    }
}
