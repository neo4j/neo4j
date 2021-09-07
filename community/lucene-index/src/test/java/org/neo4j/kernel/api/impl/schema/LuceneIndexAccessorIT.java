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
package org.neo4j.kernel.api.impl.schema;

import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Random;
import java.util.Set;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.PERSISTENT;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.values.storable.Values.stringValue;

@ExtendWith( RandomExtension.class )
@TestDirectoryExtension
public class LuceneIndexAccessorIT
{
    @Inject
    private RandomRule random;
    @Inject
    private TestDirectory directory;

    private final LifeSupport life = new LifeSupport();
    private LuceneIndexProvider indexProvider;
    private IndexSamplingConfig samplingConfig;
    private DatabaseReadOnlyChecker.Default readOnlyChecker;
    private Config config;

    @BeforeEach
    void setUp()
    {
        Path path = directory.directory( "db" );
        config = Config.defaults();
        readOnlyChecker = new DatabaseReadOnlyChecker.Default( config, DEFAULT_DATABASE_NAME );
        indexProvider = new LuceneIndexProvider( directory.getFileSystem(), PERSISTENT, directoriesByProvider( path ), new Monitors(), config,
                readOnlyChecker );
        life.add( indexProvider );
        life.start();
        samplingConfig = new IndexSamplingConfig( config );
    }

    @AfterEach
    void close()
    {
        life.shutdown();
    }

    @Test
    void shouldIterateAllDocumentsEvenWhenContainingDeletions() throws Exception
    {
        // given
        int nodes = 100;
        MutableLongSet expectedNodes = LongSets.mutable.withInitialCapacity( nodes );
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 2 ) ).withName( "TestIndex" ).materialise( 99 );
        populateWithInitialNodes( indexDescriptor, nodes, expectedNodes );
        try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( indexDescriptor, samplingConfig, mock( TokenNameLookup.class ) ) )
        {
            // when
            removeSomeNodes( indexDescriptor, nodes / 2, accessor, expectedNodes );

            // then
            try ( BoundedIterable<Long> reader = accessor.newAllEntriesValueReader( NULL ) )
            {
                MutableLongSet readIds = LongSets.mutable.empty();
                reader.forEach( readIds::add );
                assertThat( readIds ).isEqualTo( expectedNodes );
            }
        }
    }

    @Test
    void failToAcquireIndexWriterWhileReadOnly() throws Exception
    {
        int nodes = 100;
        MutableLongSet expectedNodes = LongSets.mutable.withInitialCapacity( nodes );
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 2 ) ).withName( "TestIndex" ).materialise( 99 );
        populateWithInitialNodes( indexDescriptor, nodes, expectedNodes );
        config.set( GraphDatabaseSettings.read_only_databases, Set.of( DEFAULT_DATABASE_NAME ) );
        try ( IndexAccessor onlineAccessor = indexProvider.getOnlineAccessor( indexDescriptor, samplingConfig, mock( TokenNameLookup.class ) ) )
        {
            assertThrows( UnsupportedOperationException.class, () -> onlineAccessor.newUpdater( IndexUpdateMode.ONLINE, NULL ) );
        }
    }

    @Test
    void shouldIterateAllDocumentsEvenWhenContainingDeletionsInOnlySomeLeaves() throws Exception
    {
        // given
        int nodes = 300_000;
        MutableLongSet expectedNodes = LongSets.mutable.empty();
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 2, 3, 4, 5 ) ).withName( "TestIndex" ).materialise( 99 );
        populateWithInitialNodes( indexDescriptor, nodes, expectedNodes );
        try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( indexDescriptor, samplingConfig, mock( TokenNameLookup.class ) ) )
        {
            // when
            removeSomeNodes( indexDescriptor, 2, accessor, expectedNodes );

            // then
            try ( BoundedIterable<Long> reader = accessor.newAllEntriesValueReader( NULL ) )
            {
                MutableLongSet readIds = LongSets.mutable.empty();
                reader.forEach( readIds::add );
                assertThat( readIds ).isEqualTo( expectedNodes );
            }
        }
    }

    @Test
    void shouldReadAllDocumentsInSchemaIndexAfterRandomAdditionsAndDeletions() throws Exception
    {
        // given
        IndexDescriptor descriptor = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 0, 1 ) ).withName( "test" ).materialise( 1 );
        TokenNameLookup tokenNameLookup = mock( TokenNameLookup.class );
        populateWithInitialNodes( descriptor, 0, new LongHashSet() );
        try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( descriptor, samplingConfig, tokenNameLookup ) )
        {
            // when
            BitSet expectedEntities = writeRandomThings( accessor, descriptor );
            int expectedCount = expectedEntities.cardinality();

            // then
            int count = 0;
            try ( BoundedIterable<Long> reader = accessor.newAllEntriesValueReader( NULL ) )
            {
                for ( Long entityId : reader )
                {
                    count++;
                    assertThat( expectedEntities.get( toIntExact( entityId ) ) ).isTrue();
                }
            }
            assertThat( count ).isEqualTo( expectedCount );
        }
    }

    @Test
    void shouldPartitionAndReadAllDocuments() throws Exception
    {
        // given
        IndexDescriptor descriptor = IndexPrototype.forSchema( SchemaDescriptor.forLabel( 0, 1 ) ).withName( "test" ).materialise( 1 );
        MutableLongSet expectedNodes = new LongHashSet();
        populateWithInitialNodes( descriptor, random.nextInt( 1_000, 10_000 ), expectedNodes );

        try ( IndexAccessor accessor = indexProvider.getOnlineAccessor( descriptor, samplingConfig, mock( TokenNameLookup.class ) ) )
        {
            // when
            MutableLongSet readNodes = new LongHashSet();
            IndexEntriesReader[] partitionReaders = accessor.newAllEntriesValueReader( random.nextInt( 2, 16 ), NULL );
            for ( IndexEntriesReader partitionReader : partitionReaders )
            {
                while ( partitionReader.hasNext() )
                {
                    boolean added = readNodes.add( partitionReader.next() );
                    assertThat( added ).isTrue();
                }
                partitionReader.close();
            }
            assertThat( readNodes ).isEqualTo( expectedNodes );
        }
    }

    private static void removeSomeNodes( IndexDescriptor indexDescriptor, int nodes, IndexAccessor accessor, MutableLongSet expectedNodes )
            throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
        {
            for ( long id = 0; id < nodes; id++ )
            {
                updater.process( IndexEntryUpdate.remove( id, indexDescriptor, values( indexDescriptor, id ) ) );
                expectedNodes.remove( id );
            }
        }
    }

    private void populateWithInitialNodes( IndexDescriptor indexDescriptor, int nodes, MutableLongSet expectedNodes )
            throws IndexEntryConflictException, IOException
    {
        IndexPopulator populator =
                indexProvider.getPopulator( indexDescriptor, samplingConfig, ByteBufferFactory.heapBufferFactory( (int) kibiBytes( 100 ) ), INSTANCE,
                        mock( TokenNameLookup.class ) );
        Collection<IndexEntryUpdate<IndexDescriptor>> initialData = new ArrayList<>();
        populator.create();
        for ( long id = 0; id < nodes; id++ )
        {
            Value[] values = values( indexDescriptor, id );
            initialData.add( add( id, indexDescriptor, values ) );
            expectedNodes.add( id );
            if ( initialData.size() >= 100 )
            {
                populator.add( initialData, NULL );
                initialData.clear();
            }
        }
        if ( !initialData.isEmpty() )
        {
            populator.add( initialData, NULL );
        }
        populator.scanCompleted( nullInstance, mock( IndexPopulator.PopulationWorkScheduler.class ), NULL );
        populator.close( true, NULL );
    }

    private static Value[] values( IndexDescriptor properties, long id )
    {
        int numProperties = properties.schema().getPropertyIds().length;
        Value[] values = new Value[numProperties];
        for ( int i = 0; i < numProperties; i++ )
        {
            values[i] = value( id * numProperties + i );
        }
        return values;
    }

    private static TextValue value( long id )
    {
        return stringValue( "string_" + id );
    }

    private BitSet writeRandomThings( IndexAccessor index, IndexDescriptor descriptor ) throws IndexEntryConflictException
    {
        int rounds = 200;
        int updatesPerRound = 200;
        BitSet liveEntityIds = new BitSet( rounds * updatesPerRound );
        MutableLong highEntityId = new MutableLong();
        for ( int i = 0; i < rounds; i++ )
        {
            try ( IndexUpdater updater = index.newUpdater( IndexUpdateMode.RECOVERY, NULL ) )
            {
                for ( int j = 0; j < updatesPerRound; j++ )
                {
                    IndexEntryUpdate<?> update = randomUpdate( highEntityId, liveEntityIds, descriptor, random.random() );
                    updater.process( update );
                }
            }
            if ( random.nextInt( 100 ) == 0 )
            {
                index.force( NULL );
            }
        }
        index.force( NULL );
        return liveEntityIds;
    }

    private static IndexEntryUpdate<?> randomUpdate( MutableLong highEntityId, BitSet liveEntityIds, IndexDescriptor descriptor, Random random )
    {
        if ( highEntityId.longValue() > 0 && random.nextInt( 10 ) == 0 )
        {
            long entityId = -1;
            for ( int i = 0; i < 10; i++ )
            {
                long tentativeEntityId = random.nextInt( highEntityId.intValue() );
                if ( liveEntityIds.get( toIntExact( tentativeEntityId ) ) )
                {
                    entityId = tentativeEntityId;
                    break;
                }
            }
            if ( entityId != -1 )
            {
                liveEntityIds.clear( toIntExact( entityId ) );
                return IndexEntryUpdate.remove( entityId, descriptor, stringValue( String.valueOf( entityId ) ) );
            }
        }

        long entityId = highEntityId.getAndIncrement();
        liveEntityIds.set( toIntExact( entityId ) );
        return IndexEntryUpdate.add( entityId, descriptor, stringValue( String.valueOf( entityId ) ) );
    }
}
