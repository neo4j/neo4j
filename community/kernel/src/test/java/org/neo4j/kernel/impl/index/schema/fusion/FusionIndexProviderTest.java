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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.index.schema.NumberIndexProvider;
import org.neo4j.kernel.impl.index.schema.StringIndexProvider;
import org.neo4j.kernel.impl.index.schema.TemporalIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.NONE;
import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory.forLabel;

public class FusionIndexProviderTest
{
    private static final IndexProvider.Descriptor DESCRIPTOR = new IndexProvider.Descriptor( "test-fusion", "1" );

    private StringIndexProvider stringProvider;
    private NumberIndexProvider numberProvider;
    private SpatialFusionIndexProvider spatialProvider;
    private TemporalIndexProvider temporalProvider;
    private IndexProvider luceneProvider;
    private IndexProvider[] providers;

    @Before
    public void setup()
    {
        stringProvider = mock( StringIndexProvider.class );
        numberProvider = mock( NumberIndexProvider.class );
        spatialProvider = mock( SpatialFusionIndexProvider.class );
        temporalProvider = mock( TemporalIndexProvider.class );
        luceneProvider = mock( IndexProvider.class );
        when( stringProvider.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( "string", "1" ) );
        when( numberProvider.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( "number", "1" ) );
        when( spatialProvider.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( "spatial", "1" ) );
        when( temporalProvider.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( "temporal", "1" ) );
        when( luceneProvider.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( "lucene", "1" ) );
        providers = array( stringProvider, numberProvider, spatialProvider, temporalProvider, luceneProvider );
    }

    @Rule
    public RandomRule random = new RandomRule();

    @Test
    public void mustSelectCorrectTargetForAllGivenValueCombinations()
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();
        Selector selector = new FusionSelector();

        for ( int i = 0; i < values.length; i++ )
        {
            Value[] group = values[i];
            for ( Value value : group )
            {
                // when
                IndexProvider selected = selector.select( providers, value );

                // then
                assertSame( providers[i], selected );
            }
        }

        // All composite values should go to lucene
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                // when
                IndexProvider selected = selector.select( providers, firstValue, secondValue );

                // then
                assertSame( luceneProvider, selected );
            }
        }
    }

    @Test
    public void mustCombineSamples()
    {
        // given
        int sumIndexSize = 0;
        int sumUniqueValues = 0;
        int sumSampleSize = 0;
        IndexSample[] samples = new IndexSample[providers.length];
        for ( int i = 0; i < samples.length; i++ )
        {
            int indexSize = random.nextInt( 0, 1_000_000 );
            int uniqueValues = random.nextInt( 0, 1_000_000 );
            int sampleSize = random.nextInt( 0, 1_000_000 );
            samples[i] = new IndexSample( indexSize, uniqueValues, sampleSize );
            sumIndexSize += indexSize;
            sumUniqueValues += uniqueValues;
            sumSampleSize += sampleSize;
        }

        // when
        IndexSample fusionSample = FusionIndexSampler.combineSamples( samples );

        // then
        assertEquals( sumIndexSize, fusionSample.indexSize() );
        assertEquals( sumUniqueValues, fusionSample.uniqueValues() );
        assertEquals( sumSampleSize, fusionSample.sampleSize() );
    }

    @Test
    public void getPopulationFailureMustThrowIfNoFailure()
    {
        // given
        FusionIndexProvider fusionIndexProvider = fusionProvider();

        // when
        // ... no failure
        IllegalStateException failure = new IllegalStateException( "not failed" );
        for ( IndexProvider provider : providers )
        {
            when( provider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( failure );
        }

        // then
        try
        {
            fusionIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {   // good
        }
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenAnyFailed()
    {
        for ( int i = 0; i < providers.length; i++ )
        {
            FusionIndexProvider fusionSchemaIndexProvider = fusionProvider();

            // when
            String failure = "failure";
            IllegalStateException exception = new IllegalStateException( "not failed" );
            for ( int j = 0; j < providers.length; j++ )
            {
                if ( j == i )
                {
                    when( providers[j].getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( failure );
                }
                else
                {
                    when( providers[j].getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( exception );
                }
            }

            // then
            assertThat( fusionSchemaIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) ), containsString( failure ) );
        }
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenMultipleFail()
    {
        FusionIndexProvider fusionSchemaIndexProvider = fusionProvider();

        // when
        String[] failures = new String[providers.length];
        for ( int i = 0; i < providers.length; i++ )
        {
            failures[i] = "FAILURE[" + i + "]";
            when( providers[i].getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( failures[i] );
        }

        // then
        String populationFailure = fusionSchemaIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) );
        for ( String failure : failures )
        {
            assertThat( populationFailure, containsString( failure ) );
        }
    }

    @Test
    public void shouldReportFailedIfAnyIsFailed()
    {
        // given
        IndexProvider provider = fusionProvider();
        SchemaIndexDescriptor schemaIndexDescriptor = forLabel( 1, 1 );

        for ( InternalIndexState state : InternalIndexState.values() )
        {
            for ( int i = 0; i < providers.length; i++ )
            {
                // when
                for ( int j = 0; j < providers.length; j++ )
                {
                    setInitialState( providers[j], i == j ? InternalIndexState.FAILED : state );
                }
                InternalIndexState initialState = provider.getInitialState( 0, schemaIndexDescriptor );

                // then
                assertEquals( InternalIndexState.FAILED, initialState );
            }
        }
    }

    @Test
    public void shouldReportPopulatingIfAnyIsPopulating()
    {
        // given
        IndexProvider provider = fusionProvider();
        SchemaIndexDescriptor schemaIndexDescriptor = forLabel( 1, 1 );

        for ( InternalIndexState state : array( InternalIndexState.ONLINE, InternalIndexState.POPULATING ) )
        {
            for ( int i = 0; i < providers.length; i++ )
            {
                // when
                for ( int j = 0; j < providers.length; j++ )
                {
                    setInitialState( providers[j], i == j ? InternalIndexState.POPULATING : state );
                }
                InternalIndexState initialState = provider.getInitialState( 0, schemaIndexDescriptor );

                // then
                assertEquals( InternalIndexState.POPULATING, initialState );
            }
        }
    }

    private FusionIndexProvider fusionProvider()
    {
        return new FusionIndexProvider( stringProvider, numberProvider, spatialProvider, temporalProvider, luceneProvider, new FusionSelector(),
                DESCRIPTOR, 10, NONE, mock( FileSystemAbstraction.class ) );
    }

    private void setInitialState( IndexProvider mockedProvider, InternalIndexState state )
    {
        when( mockedProvider.getInitialState( anyLong(), any( SchemaIndexDescriptor.class ) ) ).thenReturn( state );
    }
}
