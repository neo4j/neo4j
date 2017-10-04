/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema.populator;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexBuilder;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.api.index.IndexQueryHelper.add;

public class NonUniqueDatabaseIndexPopulatorTest
{
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    private final DirectoryFactory dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();

    private SchemaIndex index;
    private NonUniqueLuceneIndexPopulator populator;
    private final LabelSchemaDescriptor labelSchemaDescriptor = SchemaDescriptorFactory.forLabel( 0, 0 );

    @Before
    public void setUp() throws Exception
    {
        File folder = testDir.directory( "folder" );
        PartitionedIndexStorage indexStorage = new PartitionedIndexStorage( dirFactory, fileSystemRule.get(), folder, false );

        index = LuceneSchemaIndexBuilder.create( IndexDescriptorFactory.forSchema( labelSchemaDescriptor ) )
                .withIndexStorage( indexStorage )
                .build();
    }

    @After
    public void tearDown() throws Exception
    {
        if ( populator != null )
        {
            populator.close( false );
        }
        IOUtils.closeAll( index, dirFactory );
    }

    @Test
    public void sampleEmptyIndex() throws IOException
    {
        populator = newPopulator();

        IndexSample sample = populator.sampleResult();

        assertEquals( new IndexSample(), sample );
    }

    @Test
    public void sampleIncludedUpdates() throws Exception
    {
        populator = newPopulator();

        List<IndexEntryUpdate<?>> updates = Arrays.asList(
                add( 1, labelSchemaDescriptor, "aaa" ),
                add( 2, labelSchemaDescriptor, "bbb" ),
                add( 3, labelSchemaDescriptor, "ccc" ) );

        updates.forEach( populator::includeSample );

        IndexSample sample = populator.sampleResult();

        assertEquals( new IndexSample( 3, 3, 3 ), sample );
    }

    @Test
    public void sampleIncludedUpdatesWithDuplicates() throws Exception
    {
        populator = newPopulator();

        List<IndexEntryUpdate<?>> updates = Arrays.asList(
                add( 1, labelSchemaDescriptor, "foo" ),
                add( 2, labelSchemaDescriptor, "bar" ),
                add( 3, labelSchemaDescriptor, "foo" ) );

        updates.forEach( populator::includeSample );

        IndexSample sample = populator.sampleResult();

        assertEquals( new IndexSample( 3, 2, 3 ), sample );
    }

    @Test
    public void addUpdates() throws Exception
    {
        populator = newPopulator();

        List<IndexEntryUpdate<?>> updates = Arrays.asList(
                add( 1, labelSchemaDescriptor, "foo" ),
                add( 2, labelSchemaDescriptor, "bar" ),
                add( 42, labelSchemaDescriptor, "bar" ) );

        populator.add( updates );

        index.maybeRefreshBlocking();
        try ( IndexReader reader = index.getIndexReader() )
        {
            int propertyKeyId = labelSchemaDescriptor.getPropertyId();
            PrimitiveLongIterator allEntities = reader.query( IndexQuery.exists( propertyKeyId ) );
            assertArrayEquals( new long[]{1, 2, 42}, PrimitiveLongCollections.asArray( allEntities ) );
        }
    }

    private NonUniqueLuceneIndexPopulator newPopulator() throws IOException
    {
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
        NonUniqueLuceneIndexPopulator populator = new NonUniqueLuceneIndexPopulator( index, samplingConfig );
        populator.create();
        return populator;
    }
}
