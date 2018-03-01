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
package org.neo4j.kernel.api.impl.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneIndexAccessor;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.logging.NullLog;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class LuceneSchemaIndexPopulationIT
{
    public final TestDirectory testDir = TestDirectory.testDirectory();
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain rules = RuleChain
            .outerRule( testDir )
            .around( fileSystemRule )
            .around( pageCacheRule );

    private final int affectedNodes;
    private final IndexDescriptor descriptor = IndexDescriptorFactory.uniqueForLabel( 0, 0 );

    @Before
    public void before()
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", "10" );
    }

    @After
    public void after()
    {
        System.setProperty( "luceneSchemaIndex.maxPartitionSize", "" );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static List<Integer> affectedNodes()
    {
        return Arrays.asList( 7, 11, 14, 20, 35, 58 );
    }

    public LuceneSchemaIndexPopulationIT( int affectedNodes )
    {
        this.affectedNodes = affectedNodes;
    }

    @Test
    public void partitionedIndexPopulation() throws Exception
    {
        Config config = Config.defaults();
        DirectoryFactory dirFactory =
                DirectoryFactory.newDirectoryFactory( pageCacheRule.getPageCache( fileSystemRule ), config,
                        NullLog.getInstance() );
        try ( SchemaIndex uniqueIndex = LuceneSchemaIndexBuilder.create( descriptor, config,
                dirFactory )
                .withFileSystem( fileSystemRule.get() )
                .withIndexRootFolder( new File( testDir.directory( "partitionIndex" + affectedNodes ), "uniqueIndex" + affectedNodes ) )
                .build() )
        {
            uniqueIndex.open();

            // index is empty and not yet exist
            assertEquals( 0, uniqueIndex.allDocumentsReader().maxCount() );
            assertFalse( uniqueIndex.exists() );

            try ( LuceneIndexAccessor indexAccessor = new LuceneIndexAccessor( uniqueIndex, descriptor ) )
            {
                generateUpdates( indexAccessor, affectedNodes );
                indexAccessor.force();

                // now index is online and should contain updates data
                assertTrue( uniqueIndex.isOnline() );

                try ( IndexReader indexReader = indexAccessor.newReader() )
                {
                    long[] nodes = PrimitiveLongCollections.asArray( indexReader.query( IndexQuery.exists( 1 )) );
                    assertEquals( affectedNodes, nodes.length );

                    IndexSampler indexSampler = indexReader.createSampler();
                    IndexSample sample = indexSampler.sampleIndex();
                    assertEquals( affectedNodes, sample.indexSize() );
                    assertEquals( affectedNodes, sample.uniqueValues() );
                    assertEquals( affectedNodes, sample.sampleSize() );
                }
            }
        }
    }

    private void generateUpdates( LuceneIndexAccessor indexAccessor, int nodesToUpdate )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = indexAccessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( int nodeId = 0; nodeId < nodesToUpdate; nodeId++ )
            {
                updater.process( add( nodeId, nodeId ) );
            }
        }
    }

    private IndexEntryUpdate<?> add( long nodeId, Object value )
    {
        return IndexEntryUpdate.add( nodeId, descriptor.schema(), Values.of( value ) );
    }
}
