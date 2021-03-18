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
package org.neo4j.kernel.impl.index.schema;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forAllEntityTokens;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.kernel.impl.index.schema.TokenIndexUtility.TOKENS;
import static org.neo4j.kernel.impl.index.schema.TokenIndexUtility.generateRandomTokens;
import static org.neo4j.kernel.impl.index.schema.TokenIndexUtility.generateSomeRandomUpdates;
import static org.neo4j.kernel.impl.index.schema.TokenIndexUtility.verifyUpdates;

public class TokenIndexAccessorTest extends IndexAccessorTests<TokenScanKey,TokenScanValue,TokenScanLayout>
{
    @Override
    IndexAccessor createAccessor( PageCache pageCache )
    {
        RecoveryCleanupWorkCollector cleanup = RecoveryCleanupWorkCollector.immediate();
        DatabaseIndexContext context = DatabaseIndexContext.builder( pageCache, fs ).withReadOnly( false ).withDatabaseName( DEFAULT_DATABASE_NAME ).build();
        return new TokenIndexAccessor( context, DatabaseLayout.ofFlat( directory.homePath() ), indexFiles, Config.defaults(), indexDescriptor, cleanup );
    }

    @Override
    IndexFiles createIndexFiles( FileSystemAbstraction fs, TestDirectory directory, IndexDescriptor indexDescriptor )
    {
        return new IndexFiles.SingleFile( fs, directory.homePath().resolve( DatabaseFile.LABEL_SCAN_STORE.getName() ) );
    }

    @Override
    IndexDescriptor indexDescriptor()
    {
        return forSchema( forAllEntityTokens( EntityType.NODE ), TokenIndexProvider.DESCRIPTOR ).withName( "index" ).materialise( 0 );
    }

    @Override
    TokenScanLayout createLayout()
    {
        return new TokenScanLayout();
    }

    @Test
    void processMustThrowAfterClose() throws Exception
    {
        // given
        IndexUpdater updater = accessor.newUpdater( ONLINE, NULL );
        updater.close();

        assertThrows( IllegalStateException.class, () -> updater.process( simpleUpdate() ) );
    }

    @Test
    void shouldAddWithUpdater() throws IndexEntryConflictException, IOException
    {
        // Give
        MutableLongObjectMap<long[]> entityTokens = LongObjectMaps.mutable.empty();
        List<TokenIndexEntryUpdate<?>> updates = generateSomeRandomUpdates( entityTokens, random );

        // When
        try ( IndexUpdater updater = accessor.newUpdater( ONLINE, NULL ) )
        {
            for ( TokenIndexEntryUpdate<?> update : updates )
            {
                updater.process( update );
            }
        }

        // Then
        forceAndCloseAccessor();
        verifyUpdates( entityTokens, layout, this::getTree );
    }

    @Test
    void updaterShouldHandleRandomizedUpdates() throws Throwable
    {
        Executable additionalOperation = () -> {
        };
        updaterShouldHandleRandomizedUpdates( additionalOperation );
    }

    @Test
    void updaterShouldHandleRandomizedUpdatesWithRestart() throws Throwable
    {
        Executable additionalOperation = this::maybeRestartAccessor;
        updaterShouldHandleRandomizedUpdates( additionalOperation );
    }

    @Test
    void updaterShouldHandleRandomizedUpdatesWithCheckpoint() throws Throwable
    {
        Executable additionalOperation = this::maybeCheckpoint;
        updaterShouldHandleRandomizedUpdates( additionalOperation );
    }

    private void updaterShouldHandleRandomizedUpdates( Executable additionalOperation ) throws Throwable
    {
        MutableLongObjectMap<long[]> entityTokens = LongObjectMaps.mutable.empty();
        doRandomizedUpdatesWithAdditionalOperation( additionalOperation, entityTokens );
        forceAndCloseAccessor();
        verifyUpdates( entityTokens, layout, this::getTree );
    }

    @Test
    void newValueReaderShouldThrow()
    {
        assertThatThrownBy( accessor::newValueReader ).isInstanceOf( UnsupportedOperationException.class );
    }

    @Test
    void readerShouldFindNothingOnEmptyIndex() throws Exception
    {
        assertReaderFindsExpected( 1, LongLists.immutable.empty() );
    }

    @Test
    void readerShouldFindNothingBecauseNoMatchingToken() throws Exception
    {
        // Given
        int tokenId = 1;
        int otherTokenId = 2;
        addToIndex( otherTokenId, 1 );

        // When
        assertReaderFindsExpected( tokenId, LongLists.immutable.empty() );
    }

    @Test
    void readerShouldFindSingleWithNoOtherTokens() throws Exception
    {
        // Given
        int tokenId = 1;
        long entityId = 1;
        addToIndex( tokenId, entityId );
        LongList expectedIds = LongLists.immutable.of( entityId );

        // When
        assertReaderFindsExpected( tokenId, expectedIds );
    }

    @Test
    void readerShouldFindSingleWithOtherTokens() throws Exception
    {
        // Given
        int tokenId = 1;
        long entityId = 1;
        addToIndex( tokenId, entityId );
        addToIndex( 2, 2 );
        LongList expectedIds = LongLists.immutable.of( entityId );

        // When
        assertReaderFindsExpected( tokenId, expectedIds );
    }

    @ParameterizedTest
    @EnumSource( IndexOrder.class )
    void readerShouldFindManyWithNoOtherTokens( IndexOrder indexOrder ) throws Exception
    {
        // Given
        int tokenId = 1;
        long[] entityIds = new long[]{1, 2, 3, 64, 65, 1000, 2000};
        addToIndex( tokenId, entityIds );
        LongList expectedIds = LongLists.immutable.of( entityIds );

        // When
        assertReaderFindsExpected( indexOrder, tokenId, expectedIds );
    }

    @ParameterizedTest
    @EnumSource( IndexOrder.class )
    void readerShouldFindManyWithOtherTokens( IndexOrder indexOrder ) throws Exception
    {
        // Given
        int tokenId = 1;
        long[] entityIds =      new long[]{1, 2, 3,   64, 65,    1000,       2001};
        long[] otherEntityIds = new long[]{1, 2,   4, 64,    66, 1000, 2000      };
        addToIndex( tokenId, entityIds );
        addToIndex( 2, otherEntityIds );
        LongList expectedIds = LongLists.immutable.of( entityIds );

        // When
        assertReaderFindsExpected( indexOrder, tokenId, expectedIds );
    }

    @ParameterizedTest
    @MethodSource( "orderCombinations" )
    void readerShouldHandleNestedQueries( IndexOrder outerOrder, IndexOrder innerOrder ) throws Exception
    {
        int outerTokenId = 1;
        int innerTokenId = 2;
        long[] outerIds = new long[]{1, 2, 3,   64, 65,    1000,       2001};
        long[] innerIds = new long[]{1, 2,   4, 64,    66, 1000, 2000      };
        LongList outerExpectedIds = LongLists.immutable.of( outerIds );
        LongList innerExpectedIds = LongLists.immutable.of( innerIds );
        addToIndex( outerTokenId, outerIds );
        addToIndex( innerTokenId, innerIds );

        try ( var reader = accessor.newTokenReader() )
        {
            assertReaderFindsExpected( reader, outerOrder, outerTokenId, outerExpectedIds,
                    indexReader -> assertReaderFindsExpected( indexReader, innerOrder, innerTokenId, innerExpectedIds, ThrowingConsumer.noop() ) );
        }
    }

    @Test
    void readerShouldFindRandomizedUpdates() throws Throwable
    {
        Executable additionalOperation = () -> {
        };
        readerShouldFindRandomizedUpdates( additionalOperation );
    }

    @Test
    void readerShouldFindRandomizedUpdatesWithCheckpoint() throws Throwable
    {
        Executable additionalOperation = this::maybeCheckpoint;
        readerShouldFindRandomizedUpdates( additionalOperation );
    }

    @Test
    void readerShouldFindRandomizedUpdatesWithRestart() throws Throwable
    {
        Executable additionalOperation = this::maybeRestartAccessor;
        readerShouldFindRandomizedUpdates( additionalOperation );
    }

    @Test
    void readingAfterDropShouldThrow()
    {
        // given
        accessor.drop();

        assertThrows( IllegalStateException.class, () -> accessor.newTokenReader() );
    }

    @Test
    void readingAfterCloseShouldThrow()
    {
        // given
        accessor.close();

        assertThrows( IllegalStateException.class, () -> accessor.newTokenReader() );
    }

    private void readerShouldFindRandomizedUpdates( Executable additionalOperation ) throws Throwable
    {
        MutableLongObjectMap<long[]> entityTokens = LongObjectMaps.mutable.empty();
        doRandomizedUpdatesWithAdditionalOperation( additionalOperation, entityTokens );
        verifyReaderSeesAllUpdates( convertToExpectedEntitiesPerToken( entityTokens ) );
    }

    private void doRandomizedUpdatesWithAdditionalOperation( Executable additionalOperation, MutableLongObjectMap<long[]> trackingStructure ) throws Throwable
    {
        int numberOfEntities = 1_000;
        long currentMaxEntityId = 0;

        while ( currentMaxEntityId < numberOfEntities )
        {
            try ( IndexUpdater updater = accessor.newUpdater( ONLINE, NULL ) )
            {
                // Simply add random token ids to a new batch of 100 entities
                for ( int i = 0; i < 100; i++ )
                {
                    long[] afterTokens = generateRandomTokens( random );
                    if ( afterTokens.length != 0 )
                    {
                        trackingStructure.put( currentMaxEntityId, Arrays.copyOf( afterTokens, afterTokens.length ) );
                        updater.process( IndexEntryUpdate.change( currentMaxEntityId, null, EMPTY_LONG_ARRAY, afterTokens ) );
                    }
                    currentMaxEntityId++;
                }
            }
            additionalOperation.execute();
            // Interleave updates in id range lower than currentMaxEntityId
            try ( IndexUpdater updater = accessor.newUpdater( ONLINE, NULL ) )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    long entityId = random.nextLong( currentMaxEntityId );
                    // Current tokens for the entity in the tree
                    long[] beforeTokens = trackingStructure.get( entityId );
                    if ( beforeTokens == null )
                    {
                        beforeTokens = EMPTY_LONG_ARRAY;
                    }
                    long[] afterTokens = generateRandomTokens( random );
                    trackingStructure.put( entityId, Arrays.copyOf( afterTokens, afterTokens.length ) );
                    updater.process( IndexEntryUpdate.change( entityId, null, beforeTokens, afterTokens ) );
                }
            }
            additionalOperation.execute();
        }
    }

    private MutableLongObjectMap<MutableLongList> convertToExpectedEntitiesPerToken( MutableLongObjectMap<long[]> trackingStructure )
    {
        MutableLongObjectMap<MutableLongList> entitiesPerToken = LongObjectMaps.mutable.empty();
        trackingStructure.forEachKeyValue( ( entityId, tokens ) -> {
            for ( long token : tokens )
            {
                MutableLongList entities = entitiesPerToken.getIfAbsentPut( token, LongLists.mutable.empty() );
                entities.add( entityId );
            }
        } );
        return entitiesPerToken;
    }

    private void addToIndex( int tokenId, long... entityIds ) throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( ONLINE, NULL ) )
        {
            for ( long entityId : entityIds )
            {
                updater.process( IndexEntryUpdate.change( entityId, indexDescriptor, EMPTY_LONG_ARRAY, new long[]{tokenId} ) );
            }
        }
    }

    private void verifyReaderSeesAllUpdates( MutableLongObjectMap<MutableLongList> entitiesPerToken ) throws Exception
    {
        for ( long token : TOKENS )
        {
            MutableLongList expectedEntities = entitiesPerToken.getIfAbsent( token, LongLists.mutable::empty );
            assertReaderFindsExpected( token, expectedEntities );
        }
    }

    private void assertReaderFindsExpected( long tokenId, LongList expectedIds ) throws Exception
    {
        assertReaderFindsExpected( IndexOrder.NONE, tokenId, expectedIds );
    }

    private void assertReaderFindsExpected( IndexOrder indexOrder, long tokenId, LongList expectedIds ) throws Exception
    {
        try ( var indexReader = accessor.newTokenReader() )
        {
            assertReaderFindsExpected( indexReader, indexOrder, tokenId, expectedIds, ThrowingConsumer.noop() );
        }
    }

    private void assertReaderFindsExpected( TokenIndexReader reader, IndexOrder indexOrder, long tokenId, LongList expectedIds,
                                            ThrowingConsumer<TokenIndexReader,Exception> innerCalling )
            throws Exception
    {
        if ( indexOrder.equals( IndexOrder.DESCENDING ) )
        {
            expectedIds = expectedIds.toReversed();
        }
        try ( CollectingEntityTokenClient collectingEntityTokenClient = new CollectingEntityTokenClient( tokenId ) )
        {
            IndexQueryConstraints constraint = IndexQueryConstraints.constrained( indexOrder, false );
            TokenPredicate query = new TokenPredicate( (int) tokenId );
            reader.query( NULL_CONTEXT, collectingEntityTokenClient, constraint, query );

            // Then
            int count = 0;
            while ( collectingEntityTokenClient.next() )
            {
                innerCalling.accept( reader );
                count++;
            }
            assertThat( count ).isEqualTo( expectedIds.size() );
            assertThat( collectingEntityTokenClient.actualIds ).isEqualTo( expectedIds );
        }
    }

    private void maybeRestartAccessor() throws IOException
    {
        if ( random.nextDouble() < 0.1 )
        {
            forceAndCloseAccessor();
            setupAccessor();
        }
    }

    private void maybeCheckpoint()
    {
        if ( random.nextBoolean() )
        {
            accessor.force( IOLimiter.UNLIMITED, NULL );
        }
    }

    private void forceAndCloseAccessor()
    {
        accessor.force( IOLimiter.UNLIMITED, NULL );
        accessor.close();
    }

    private TokenIndexEntryUpdate<IndexDescriptor> simpleUpdate()
    {
        return TokenIndexEntryUpdate.change( 0, indexDescriptor, EMPTY_LONG_ARRAY, new long[]{0} );
    }

    private static Stream<Arguments> orderCombinations()
    {
        List<Arguments> arguments = new ArrayList<>();
        for ( IndexOrder outer : IndexOrder.values() )
        {
            for ( IndexOrder inner : IndexOrder.values() )
            {
                arguments.add( Arguments.of( outer, inner ) );
            }
        }
        return arguments.stream();
    }

    private static class CollectingEntityTokenClient implements IndexProgressor.EntityTokenClient, Closeable
    {
        private final long expectedToken;
        private final MutableLongList actualIds = LongLists.mutable.empty();
        private IndexProgressor progressor;

        private CollectingEntityTokenClient()
        {
            this( -1 );
        }

        private CollectingEntityTokenClient( long expectedToken )
        {
            this.expectedToken = expectedToken;
        }

        @Override
        public void initialize( IndexProgressor progressor, int token, IndexOrder order )
        {
            assertThat( token ).isEqualTo( expectedToken );
            this.progressor = progressor;
        }

        @Override
        public void initialize( IndexProgressor progressor, int token, LongIterator added, LongSet removed )
        {
            throw new UnsupportedOperationException( "Did not expect to use this method" );
        }

        @Override
        public boolean acceptEntity( long reference, TokenSet tokens )
        {
            actualIds.add( reference );
            return true;
        }

        boolean next()
        {
            return progressor.next();
        }

        @Override
        public void close()
        {
            if ( progressor != null )
            {
                progressor.close();
            }
        }
    }
}
