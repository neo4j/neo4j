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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.storageengine.StorageEngine;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.Dependencies;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
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

        Replicator replicator = new StubReplicator();
        when( dependencies.resolveDependency( TransactionRepresentationCommitProcess.class ) )
                .thenReturn( mock( TransactionRepresentationCommitProcess.class ) );

        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies );

        tokenHolder.setLastCommittedIndex( -1 );
        tokenHolder.start();

        // when
        int tokenId = tokenHolder.getOrCreateId( "Person" );

        // then
        assertEquals( EXPECTED_TOKEN_ID, tokenId );
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

        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder(
                new StubReplicator(), idGeneratorFactory, dependencies );
        tokenHolder.setLastCommittedIndex( -1 );

        // when
        ReplicatedTokenRequest tokenRequest = new ReplicatedTokenRequest( LABEL, "Person",
                createCommandBytes( createCommands( EXPECTED_TOKEN_ID ) ) );
        tokenHolder.onReplicated( tokenRequest, logIndex );

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
                new ReplicatedLabelTokenHolder( null, null, dependencies );

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
                null, dependencies );

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

        Replicator replicator = new StubReplicator();

        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies );

        tokenHolder.setLastCommittedIndex( -1 );
        tokenHolder.start();
        tokenHolder.onReplicated( new ReplicatedTokenRequest( LABEL, "Person", createCommandBytes(
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

        RaceConditionSimulatingReplicator replicator = new RaceConditionSimulatingReplicator();

        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies );

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

        RaceConditionSimulatingReplicator replicator = new RaceConditionSimulatingReplicator();

        ReplicatedTokenHolder<Token, LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies );

        tokenHolder.setLastCommittedIndex( -1 );
        tokenHolder.start();
        replicator.injectLabelTokenBeforeOtherOneReplicates( new ReplicatedTokenRequest( LABEL, "Dog",
                createCommandBytes( createCommands( INJECTED_TOKEN_ID ) ) ) );

        // when
        int tokenId = tokenHolder.getOrCreateId( "Person" );

        // then
        assertEquals( EXPECTED_TOKEN_ID, tokenId );
    }

    private List<Command> createCommands( int tokenId )
    {
        List<Command> commands = new ArrayList<>();
        commands.add(
                new Command.LabelTokenCommand( new LabelTokenRecord( tokenId ), new LabelTokenRecord( tokenId ) ) );
        return commands;
    }

    static class RaceConditionSimulatingReplicator implements Replicator
    {
        private final Collection<ReplicatedContentListener> listeners = new HashSet<>();
        private ReplicatedTokenRequest otherToken;

        public void injectLabelTokenBeforeOtherOneReplicates( ReplicatedTokenRequest token )
        {
            this.otherToken = token;
        }

        @Override
        public void replicate( final ReplicatedContent content ) throws ReplicationFailedException
        {
            for ( ReplicatedContentListener listener : listeners )
            {
                if ( otherToken != null )
                {
                    listener.onReplicated( otherToken, 0 );
                }
                listener.onReplicated( content, 0 );
            }
        }

        @Override
        public void subscribe( ReplicatedContentListener listener )
        {
            this.listeners.add( listener );
        }

        @Override
        public void unsubscribe( ReplicatedContentListener listener )
        {
            this.listeners.remove( listener );
        }
    }

    static class StubReplicator implements Replicator
    {
        private final Collection<ReplicatedContentListener> listeners = new HashSet<>();

        @Override
        public void replicate( final ReplicatedContent content ) throws ReplicationFailedException
        {
            for ( ReplicatedContentListener listener : listeners )
            {
                listener.onReplicated( content, 0 );
            }
        }

        @Override
        public void subscribe( ReplicatedContentListener listener )
        {
            this.listeners.add( listener );
        }

        @Override
        public void unsubscribe( ReplicatedContentListener listener )
        {
            this.listeners.remove( listener );
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
}
