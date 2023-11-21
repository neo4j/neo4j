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
package org.neo4j.kernel.api.impl.index;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.LuceneIndexAccessor;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.api.impl.schema.AbstractLuceneIndexProvider.UPDATE_IGNORE_STRATEGY;
import static org.neo4j.kernel.api.impl.schema.LuceneTestTokenNameLookup.SIMPLE_TOKEN_LOOKUP;

@TestDirectoryExtension
class LuceneSchemaIndexPopulationIT
{
    private final IndexDescriptor descriptor = IndexPrototype.uniqueForSchema( SchemaDescriptors.forLabel( 0, 0 ) ).withName( "a" ).materialise( 1 );

    @Inject
    private TestDirectory testDir;
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @BeforeEach
    void before()
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", "10" );
    }

    @AfterEach
    void after()
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", "" );
    }

    @ParameterizedTest
    @ValueSource( ints = {7, 11, 14, 20, 35, 58} )
    void partitionedIndexPopulation( int affectedNodes ) throws Exception
    {
        Path rootFolder = testDir.directory( "partitionIndex" + affectedNodes ).resolve( "uniqueIndex" + affectedNodes );
        try ( SchemaIndex uniqueIndex = LuceneSchemaIndexBuilder.create( descriptor, writable(), Config.defaults() )
                .withFileSystem( fileSystem )
                .withIndexRootFolder( rootFolder ).build() )
        {
            uniqueIndex.open();

            // index is empty and not yet exist
            assertEquals( 0, uniqueIndex.allDocumentsReader().maxCount() );
            assertFalse( uniqueIndex.exists() );

            try ( LuceneIndexAccessor indexAccessor = new LuceneIndexAccessor( uniqueIndex, descriptor, SIMPLE_TOKEN_LOOKUP, UPDATE_IGNORE_STRATEGY ) )
            {
                generateUpdates( indexAccessor, affectedNodes );
                indexAccessor.force( NULL );

                // now index is online and should contain updates data
                assertTrue( uniqueIndex.isOnline() );

                try ( var indexReader = indexAccessor.newValueReader();
                      NodeValueIterator results = new NodeValueIterator();
                      IndexSampler indexSampler = indexReader.createSampler() )
                {
                    indexReader.query( results, NULL_CONTEXT, AccessMode.Static.READ, unconstrained(), PropertyIndexQuery.exists( 1 ) );
                    long[] nodes = PrimitiveLongCollections.asArray( results );
                    assertEquals( affectedNodes, nodes.length );

                    IndexSample sample = indexSampler.sampleIndex( NULL, new AtomicBoolean() );
                    assertEquals( affectedNodes, sample.indexSize() );
                    assertEquals( affectedNodes, sample.uniqueValues() );
                    assertEquals( affectedNodes, sample.sampleSize() );
                }
            }
        }
    }

    private void generateUpdates( LuceneIndexAccessor indexAccessor, int nodesToUpdate ) throws IndexEntryConflictException
    {
        try ( IndexUpdater updater = indexAccessor.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
        {
            for ( int nodeId = 0; nodeId < nodesToUpdate; nodeId++ )
            {
                updater.process( add( nodeId, "node " + nodeId ) );
            }
        }
    }

    private IndexEntryUpdate<?> add( long nodeId, Object value )
    {
        return IndexEntryUpdate.add( nodeId, descriptor, Values.of( value ) );
    }
}
