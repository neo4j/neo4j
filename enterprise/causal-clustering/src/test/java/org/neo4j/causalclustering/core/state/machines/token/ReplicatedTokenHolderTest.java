/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatedTokenHolderTest
{
    private Dependencies dependencies = mock( Dependencies.class );

    @Test
    public void shouldStoreInitialTokens()
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
    public void shouldReturnExistingTokenId()
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
                ( content, trackResult ) ->
                {
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
        doAnswer( invocation ->
        {
            Collection<StorageCommand> target = invocation.getArgument( 0 );
            ReadableTransactionState txState = invocation.getArgument( 1 );
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
        StorageStatement statement = mock( StorageStatement.class );
        when( readLayer.newStatement() ).thenReturn( statement );
        when( storageEngine.storeReadLayer() ).thenReturn( readLayer );
        return storageEngine;
    }

}
