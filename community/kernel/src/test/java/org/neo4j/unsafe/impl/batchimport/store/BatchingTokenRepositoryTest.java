/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.store;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.Token;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static java.lang.Integer.parseInt;

public class BatchingTokenRepositoryTest
{
    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();

    @Test
    public void shouldDedupLabelIds() throws Exception
    {
        // GIVEN
        BatchingLabelTokenRepository repo = new BatchingLabelTokenRepository( mock( TokenStore.class ) );

        // WHEN
        long[] ids = repo.getOrCreateIds( new String[] {"One", "Two", "One"} );

        // THEN
        assertTrue( NodeLabelsField.isSane( ids ) );
    }

    @Test
    public void shouldSortLabelIds() throws Exception
    {
        // GIVEN
        BatchingLabelTokenRepository repo = new BatchingLabelTokenRepository( mock( TokenStore.class ) );
        long[] expected = new long[] {
                repo.getOrCreateId( "One" ),
                repo.getOrCreateId( "Two" ),
                repo.getOrCreateId( "Three" )
        };

        // WHEN
        long[] ids = repo.getOrCreateIds( new String[] {"Two", "One", "Three"} );

        // THEN
        assertArrayEquals( expected, ids );
        assertTrue( NodeLabelsField.isSane( ids ) );
    }

    @Test
    public void shouldRespectExistingTokens() throws Exception
    {
        // given
        TokenStore<RelationshipTypeTokenRecord,RelationshipTypeToken> tokenStore = mock( TokenStore.class );
        int previousHighId = 5;
        when( tokenStore.getHighId() ).thenReturn( (long) previousHighId );
        BatchingRelationshipTypeTokenRepository repo = new BatchingRelationshipTypeTokenRepository( tokenStore );
        verify( tokenStore ).getHighId();

        // when
        int tokenId = repo.getOrCreateId( "NEW_ONE" );

        // then
        assertEquals( previousHighId, tokenId );
    }

    @Test
    public void shouldFlushNewTokens() throws Exception
    {
        // given
        try ( NeoStores stores = new StoreFactory( storage.directory().absolutePath(), Config.defaults(),
                new DefaultIdGeneratorFactory( storage.fileSystem() ), storage.pageCache(), storage.fileSystem(),
                NullLogProvider.getInstance() ).openNeoStores( true, StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME ) )
        {
            TokenStore<PropertyKeyTokenRecord,Token> tokenStore = stores.getPropertyKeyTokenStore();
            int rounds = 3;
            int tokensPerRound = 4;
            try ( BatchingPropertyKeyTokenRepository repo = new BatchingPropertyKeyTokenRepository( tokenStore ) )
            {
                // when first creating some tokens
                int expectedId = 0;
                int tokenNameAsInt = 0;
                for ( int round = 0; round < rounds; round++ )
                {
                    for ( int i = 0; i < tokensPerRound; i++ )
                    {
                        int tokenId = repo.getOrCreateId( String.valueOf( tokenNameAsInt++ ) );
                        assertEquals( expectedId + i, tokenId );
                    }
                    assertEquals( expectedId, tokenStore.getHighId() );
                    repo.flush();
                    assertEquals( expectedId + tokensPerRound, tokenStore.getHighId() );
                    expectedId += tokensPerRound;
                }
            }

            List<Token> tokens = tokenStore.getTokens( 100 );
            assertEquals( tokensPerRound * rounds, tokens.size() );
            for ( Token token : tokens )
            {
                assertEquals( token.id(), parseInt( token.name() ) );
            }
        }
    }
}
