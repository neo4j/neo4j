/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.batchimport.store;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;
import org.neo4j.internal.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.cursor.CursorTypes;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.token.api.NamedToken;

import static java.lang.Integer.parseInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;

@PageCacheExtension
@Neo4jLayoutExtension
class BatchingTokenRepositoryTest
{
    public static final Condition<long[]> ORDERED_IDS_CONDITION = new Condition<>( BatchingTokenRepositoryTest::areOrdered, "Label ids should be ordered" );
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void shouldDedupLabelIds()
    {
        // GIVEN
        BatchingTokenRepository.BatchingLabelTokenRepository repo = new BatchingTokenRepository.BatchingLabelTokenRepository( mock( TokenStore.class ) );

        // WHEN
        long[] ids = repo.getOrCreateIds( new String[] {"One", "Two", "One"} );

        // THEN
        assertThat( ids ).satisfies( ORDERED_IDS_CONDITION );
    }

    @Test
    void shouldSortLabelIds()
    {
        // GIVEN
        BatchingTokenRepository.BatchingLabelTokenRepository repo = new BatchingTokenRepository.BatchingLabelTokenRepository( mock( TokenStore.class ) );
        long[] expected = new long[] {
                repo.getOrCreateId( "One" ),
                repo.getOrCreateId( "Two" ),
                repo.getOrCreateId( "Three" )
        };

        // WHEN
        long[] ids = repo.getOrCreateIds( new String[] {"Two", "One", "Three"} );

        // THEN
        assertArrayEquals( expected, ids );
        assertThat( ids ).satisfies( ORDERED_IDS_CONDITION );
    }

    @Test
    void shouldRespectExistingTokens()
    {
        // given
        TokenStore<RelationshipTypeTokenRecord> tokenStore = mock( TokenStore.class );
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
    void shouldFlushNewTokens()
    {
        // given
        try ( NeoStores stores = newNeoStores( StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME );
              var storeCursors = new CachedStoreCursors( stores, NULL ) )
        {
            TokenStore<PropertyKeyTokenRecord> tokenStore = stores.getPropertyKeyTokenStore();
            int rounds = 3;
            int tokensPerRound = 4;
            BatchingPropertyKeyTokenRepository repo = new BatchingPropertyKeyTokenRepository( tokenStore );
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
                try ( PageCursor pageCursor = storeCursors.writeCursor( CursorTypes.PROPERTY_KEY_TOKEN_CURSOR ) )
                {
                    repo.flush( NULL, pageCursor, storeCursors );
                }
                assertEquals( expectedId + tokensPerRound, tokenStore.getHighId() );
                expectedId += tokensPerRound;
            }
            try ( PageCursor pageCursor = storeCursors.writeCursor( CursorTypes.PROPERTY_KEY_TOKEN_CURSOR ) )
            {
                repo.flush( NULL, pageCursor, storeCursors );
            }

            List<NamedToken> tokens = tokenStore.getTokens( storeCursors );
            assertEquals( tokensPerRound * rounds, tokens.size() );
            for ( NamedToken token : tokens )
            {
                assertEquals( token.id(), parseInt( token.name() ) );
            }
        }
    }

    @Test
    void shouldCheckTokenNamesForValidity()
    {
        try ( NeoStores neoStores = newNeoStores( StoreType.LABEL_TOKEN, StoreType.LABEL_TOKEN_NAME ) )
        {
            BatchingTokenRepository.BatchingLabelTokenRepository repository =
                    new BatchingTokenRepository.BatchingLabelTokenRepository( neoStores.getLabelTokenStore() );
            assertThrows( IllegalArgumentException.class, () -> repository.getOrCreateId( null ) );
            assertThrows( IllegalArgumentException.class, () -> repository.getOrCreateId( "" ) );
            assertThrows( IllegalArgumentException.class, () -> repository.getOrCreateId( (Object) "" ) ); // the string-or-integer method
            assertThrows( IllegalArgumentException.class, () -> repository.getOrCreateIds( new String[]{"abc", "", null} ) );
        }
    }

    private NeoStores newNeoStores( StoreType... storeTypes )
    {
        return new StoreFactory( databaseLayout, Config.defaults(), new DefaultIdGeneratorFactory( fileSystem, immediate(), databaseLayout.getDatabaseName() ),
                pageCache, fileSystem, NullLogProvider.getInstance(), PageCacheTracer.NULL, writable() ).openNeoStores( true, storeTypes );
    }

    public static boolean areOrdered( long[] labelIds )
    {
        long prev = -1;
        for ( long labelId : labelIds )
        {
            if ( labelId <= prev )
            {
                return false;
            }
            prev = labelId;
        }
        return true;
    }
}
