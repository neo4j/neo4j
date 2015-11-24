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
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import org.neo4j.coreedge.raft.replication.token.ReplicatedLabelTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenHolder;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequest;
import org.neo4j.coreedge.raft.replication.token.TokenType;
import org.neo4j.coreedge.raft.replication.token.ReplicatedTokenRequestSerializer;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.Replicator;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.util.Dependencies;

import static java.util.Collections.singletonList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatedTokenHolderTest
{
    final int EXPECTED_TOKEN_ID = 1;
    final int INJECTED_TOKEN_ID = 1024;

    Dependencies dependencies = mock( Dependencies.class );
    Collection<Command> expectedCommands;
    Collection<Command> injectedCommands;

    @Before
    public void setup()
    {
        {
            expectedCommands = new ArrayList<>();
            Command.LabelTokenCommand labelTokenCommand = new Command.LabelTokenCommand();
            labelTokenCommand.init( new LabelTokenRecord( EXPECTED_TOKEN_ID ) );
            expectedCommands.add( labelTokenCommand );
        }

        {
            injectedCommands = new ArrayList<>();
            Command.LabelTokenCommand labelTokenCommand = new Command.LabelTokenCommand();
            labelTokenCommand.init( new LabelTokenRecord( INJECTED_TOKEN_ID ) );
            injectedCommands.add( labelTokenCommand );
        }

        NeoStores neoStore = mock( NeoStores.class );
        LabelTokenStore labelTokenStore = mock( LabelTokenStore.class );
        when (neoStore.getLabelTokenStore()).thenReturn( labelTokenStore );
        when(labelTokenStore.allocateNameRecords( Matchers.<byte[]>any() )).thenReturn( singletonList( new DynamicRecord( 1l ) ) );
        when( dependencies.resolveDependency( NeoStores.class ) ).thenReturn( neoStore );
        when( dependencies.resolveDependency( TransactionRepresentationCommitProcess.class ) ).thenReturn( mock( TransactionRepresentationCommitProcess.class ) );
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

        ReplicatedTokenHolder<Token,LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies );

        tokenHolder.start();

        // when
        int tokenId = tokenHolder.getOrCreateId( "Person" );

        // then
        assertEquals( EXPECTED_TOKEN_ID, tokenId );
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

        ReplicatedTokenHolder<Token,LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies );

        tokenHolder.start();
        tokenHolder.onReplicated( new ReplicatedTokenRequest( TokenType.LABEL, "Person", ReplicatedTokenRequestSerializer.createCommandBytes( expectedCommands ) ) );

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

        ReplicatedTokenHolder<Token,LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies );

        tokenHolder.start();
        replicator.injectLabelTokenBeforeOtherOneReplicates( new ReplicatedTokenRequest( TokenType.LABEL, "Person",
                ReplicatedTokenRequestSerializer.createCommandBytes( injectedCommands ) ) );

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

        ReplicatedTokenHolder<Token,LabelTokenRecord> tokenHolder = new ReplicatedLabelTokenHolder( replicator,
                idGeneratorFactory, dependencies );

        tokenHolder.start();
        replicator.injectLabelTokenBeforeOtherOneReplicates( new ReplicatedTokenRequest( TokenType.LABEL, "Dog",
                ReplicatedTokenRequestSerializer.createCommandBytes( injectedCommands ) ) );

        // when
        int tokenId = tokenHolder.getOrCreateId( "Person" );

        // then
        assertEquals( EXPECTED_TOKEN_ID, tokenId );

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
                    listener.onReplicated( otherToken );
                }
                listener.onReplicated( content );
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
                listener.onReplicated( content );
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
}
