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
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.IMMEDIATE;

public class NativeUniqueSchemaNumberIndexPopulatorTest extends NativeSchemaIndexPopulatorTest<NumberKey,NumberValue>
{
    @Override
    NativeSchemaNumberIndexPopulator<NumberKey,NumberValue> createPopulator(
            PageCache pageCache, File indexFile,
            Layout<NumberKey,NumberValue> layout, IndexSamplingConfig samplingConfig )
    {
        return new NativeUniqueSchemaNumberIndexPopulator<>( pageCache, indexFile, layout, IMMEDIATE );
    }

    @Override
    protected LayoutTestUtil<NumberKey,NumberValue> createLayoutTestUtil()
    {
        return new UniqueLayoutTestUtil();
    }

    @Override
    protected int compareValue( NumberValue value1, NumberValue value2 )
    {
        return layoutUtil.compareIndexedPropertyValue( value1, value2 );
    }

    @Test
    public void addShouldThrowOnDuplicateValues() throws Exception
    {
        // given
        populator.create();
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someDuplicateIndexEntryUpdates();

        // when
        try
        {
            populator.add( Arrays.asList( updates ) );
            fail( "Updates should have conflicted" );
        }
        catch ( IndexEntryConflictException e )
        {
            // then good
        }

        populator.close( true );
    }

    @Test
    public void updaterShouldThrowOnDuplicateValues() throws Exception
    {
        // given
        populator.create();
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someDuplicateIndexEntryUpdates();
        try ( IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor ) )
        {
            // when
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                updater.process( update );
            }
            fail( "Updates should have conflicted" );
        }
        catch ( IndexEntryConflictException e )
        {
            // then good
        }

        populator.close( true );
    }

    @Test
    public void shouldThrowOnLargeAmountOfInterleavedRandomUpdatesWithDuplicates() throws Exception
    {
        // given
        populator.create();
        random.reset();
        Random updaterRandom = new Random( random.seed() );
        Iterator<IndexEntryUpdate<IndexDescriptor>> updates = randomUniqueUpdateGenerator( random, 0.01f );
        Number failSafeDuplicateValue = 12345.6789D;

        // when
        try
        {
            populator.add( add( 1_000_000_000, failSafeDuplicateValue ) );
            for ( int i = 0; i < LARGE_AMOUNT_OF_UPDATES; i++ )
            {
                if ( updaterRandom.nextFloat() < 0.1 )
                {
                    try ( IndexUpdater indexUpdater = populator.newPopulatingUpdater( null_property_accessor ) )
                    {
                        int numberOfUpdaterUpdates = updaterRandom.nextInt( 100 );
                        for ( int j = 0; j < numberOfUpdaterUpdates; j++ )
                        {
                            indexUpdater.process( updates.next() );
                        }
                    }
                }
                populator.add( updates.next() );
            }
            populator.add( add( 1_000_000_001, failSafeDuplicateValue ) );
            fail( "Should have bumped into and detected duplicate" );
        }
        catch ( IndexEntryConflictException e )
        {
            // then good
        }

        populator.close( true );
    }

    @Test
    public void shouldSampleUpdates() throws Exception
    {
        // GIVEN
        populator.create();
        populator.configureSampling( true ); // has no effect, really
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someIndexEntryUpdates();

        // WHEN
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            populator.add( update );
            populator.includeSample( update );
        }
        IndexSample sample = populator.sampleResult();

        // THEN
        assertEquals( updates.length, sample.sampleSize() );
        assertEquals( updates.length, sample.uniqueValues() );
        assertEquals( updates.length, sample.indexSize() );
        populator.close( true );
    }
}
