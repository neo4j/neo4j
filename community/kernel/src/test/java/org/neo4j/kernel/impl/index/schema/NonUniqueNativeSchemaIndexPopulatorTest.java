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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.IMMEDIATE;
import static org.neo4j.kernel.impl.index.schema.FullScanNonUniqueIndexSamplerTest.countUniqueValues;

public class NonUniqueNativeSchemaIndexPopulatorTest
        extends NativeSchemaIndexPopulatorTest<NonUniqueSchemaNumberKey,NonUniqueSchemaNumberValue>
{
    @Override
    Layout<NonUniqueSchemaNumberKey,NonUniqueSchemaNumberValue> createLayout()
    {
        return new NonUniqueSchemaNumberIndexLayout();
    }

    @Override
    NativeSchemaIndexPopulator<NonUniqueSchemaNumberKey,NonUniqueSchemaNumberValue> createPopulator( PageCache pageCache, File indexFile,
            Layout<NonUniqueSchemaNumberKey,NonUniqueSchemaNumberValue> layout, IndexSamplingConfig samplingConfig )
    {
        return new NonUniqueNativeSchemaIndexPopulator<>( pageCache, indexFile, layout, IMMEDIATE, samplingConfig );
    }

    @Override
    protected int compareValue( NonUniqueSchemaNumberValue value1, NonUniqueSchemaNumberValue value2 )
    {
        return compareIndexedPropertyValue( value1, value2 );
    }

    @Test
    public void addShouldApplyDuplicateValues() throws Exception
    {
        // given
        populator.create();
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someDuplicateIndexEntryUpdates();

        // when
        populator.add( Arrays.asList( updates ) );

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void updaterShouldApplyDuplicateValues() throws Exception
    {
        // given
        populator.create();
        IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor );
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someDuplicateIndexEntryUpdates();

        // when
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            updater.process( update );
        }

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void shouldApplyLargeAmountOfInterleavedRandomUpdatesWithDuplicates() throws Exception
    {
        // given
        populator.create();
        random.reset();
        Random updaterRandom = new Random( random.seed() );
        Iterator<IndexEntryUpdate<IndexDescriptor>> updates = randomUniqueUpdateGenerator( random, 0.1f );

        // when
        int count = interleaveLargeAmountOfUpdates( updaterRandom, updates );

        // then
        populator.close( true );
        random.reset();
        verifyUpdates( randomUniqueUpdateGenerator( random, 0.1f ), count );
    }

    @Test
    public void shouldFailOnSampleBeforeConfiguredSampling() throws Exception
    {
        // GIVEN
        populator.create();

        // WHEN
        try
        {
            populator.sampleResult();
            fail();
        }
        catch ( IllegalStateException e )
        {
            // THEN good
        }
        populator.close( true );
    }

    @Test
    public void shouldSampleWholeIndexIfConfiguredForPopulatingSampling() throws Exception
    {
        // GIVEN
        populator.create();
        populator.configureSampling( false );
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someIndexEntryUpdates();
        populator.add( Arrays.asList( updates ) );

        // WHEN
        IndexSample sample = populator.sampleResult();

        // THEN
        assertEquals( updates.length, sample.sampleSize() );
        assertEquals( updates.length, sample.uniqueValues() );
        assertEquals( updates.length, sample.indexSize() );
        populator.close( true );
    }

    @Test
    public void shouldSampleUpdatesIfConfiguredForOnlineSampling() throws Exception
    {
        // GIVEN
        populator.create();
        populator.configureSampling( true );
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] scanUpdates = someIndexEntryUpdates();
        populator.add( Arrays.asList( scanUpdates ) );
        Number[] updates = array( 101, 102, 102, 103, 103 );
        try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor ) )
        {
            long nodeId = 1000;
            for ( Number number : updates )
            {
                IndexEntryUpdate<IndexDescriptor> update = add( nodeId++, number );
                updater.process( update );
                populator.includeSample( update );
            }
        }

        // WHEN
        IndexSample sample = populator.sampleResult();

        // THEN
        assertEquals( updates.length, sample.sampleSize() );
        assertEquals( countUniqueValues( asList( updates ) ), sample.uniqueValues() );
        assertEquals( updates.length, sample.indexSize() );
        populator.close( true );
    }

    @Override
    protected void copyValue( NonUniqueSchemaNumberValue value, NonUniqueSchemaNumberValue intoValue )
    {
        intoValue.type = value.type;
        intoValue.rawValueBits = value.rawValueBits;
    }
}
