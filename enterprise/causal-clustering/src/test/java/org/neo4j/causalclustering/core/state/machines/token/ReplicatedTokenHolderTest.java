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
package org.neo4j.causalclustering.core.state.machines.token;

import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatedTokenHolderTest
{
    private Dependencies dependencies = mock( Dependencies.class );

    @Test
    public void shouldStoreInitialTokens() throws Exception
    {
        // given
        TokenRegistry<Token> registry = new TokenRegistry<>( "Label" );
        ReplicatedTokenHolder<Token> tokenHolder = new ReplicatedLabelTokenHolder( registry, null,
                null, dependencies );

        // when
        tokenHolder.setInitialTokens( asList( new Token( "name1", 1 ), new Token( "name2", 2 ) ) );

        // then
        assertThat( tokenHolder.getAllTokens(), hasItems( new Token( "name1", 1 ), new Token( "name2", 2 ) ) );
    }

    @Test
    public void shouldReturnExistingTokenId() throws Exception
    {
        // given
        TokenRegistry<Token> registry = new TokenRegistry<>( "Label" );
        ReplicatedTokenHolder<Token> tokenHolder = new ReplicatedLabelTokenHolder( registry, null,
                null, dependencies );
        tokenHolder.setInitialTokens( asList( new Token( "name1", 1 ), new Token( "name2", 2 ) ) );

        // when
        Integer tokenId = tokenHolder.getOrCreateId( "name1" );

        // then
        assertThat( tokenId, equalTo( 1 ));
    }

    @Test
    public void shouldReplicateTokenRequestForNewToken() throws Exception
    {
        // given
        StorageEngine storageEngine = mockedStorageEngine();
        when( dependencies.resolveDependency( StorageEngine.class ) ).thenReturn( storageEngine );

        IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
        IdGenerator idGenerator = mock( IdGenerator.class );
        when( idGenerator.nextId() ).thenReturn( 1L );

        when( idGeneratorFactory.get( any( IdType.class ) ) ).thenReturn( idGenerator );

        TokenRegistry<Token> registry = new TokenRegistry<>( "Label" );
        int generatedTokenId = 1;
        ReplicatedTokenHolder<Token> tokenHolder = new ReplicatedLabelTokenHolder( registry,
                ( content, trackResult ) -> {
                    CompletableFuture<Object> completeFuture = new CompletableFuture<>();
                    completeFuture.complete( generatedTokenId );
                    return completeFuture;
                },
                idGeneratorFactory, dependencies );

        // when
        Integer tokenId = tokenHolder.getOrCreateId( "name1" );

        // then
        assertThat( tokenId, equalTo( generatedTokenId ));
    }

    @SuppressWarnings( "unchecked" )
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
                any( StorageStatement.class ), any( ResourceLocker.class ), anyLong() );

        StoreReadLayer readLayer = mock( StoreReadLayer.class );
        when( readLayer.newStatement() ).thenReturn( mock( StorageStatement.class ) );
        when( storageEngine.storeReadLayer() ).thenReturn( readLayer );
        return storageEngine;
    }

}
