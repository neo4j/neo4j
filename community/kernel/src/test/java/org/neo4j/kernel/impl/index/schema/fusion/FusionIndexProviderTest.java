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
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
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

    private IndexProvider nativeProvider;
    private IndexProvider spatialProvider;
    private IndexProvider temporalProvider;
    private IndexProvider luceneProvider;

    @Before
    public void setup()
    {
        nativeProvider = mock( IndexProvider.class );
        spatialProvider = mock( IndexProvider.class );
        temporalProvider = mock( IndexProvider.class );
        luceneProvider = mock( IndexProvider.class );
        when( nativeProvider.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( "native", "1" ) );
        when( spatialProvider.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( "spatial", "1" ) );
        when( temporalProvider.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( "temporal", "1" ) );
        when( luceneProvider.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( "lucene", "1" ) );
    }

    @Rule
    public RandomRule random = new RandomRule();

    @Test
    public void mustSelectCorrectTargetForAllGivenValueCombinations()
    {
        // given
        Value[] numberValues = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] spatialValues = FusionIndexTestHelp.valuesSupportedBySpatial();
        Value[] temporalValues = FusionIndexTestHelp.valuesSupportedByTemporal();
        Value[] otherValues = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();
        Value[] allValues = FusionIndexTestHelp.allValues();

        // Number values should go to native provider
        Selector selector = new FusionSelector();
        for ( Value numberValue : numberValues )
        {
            // when
            IndexProvider selected = selector.select( nativeProvider, spatialProvider, temporalProvider, luceneProvider, numberValue );

            // then
            assertSame( nativeProvider, selected );
        }

        // Geometric values should go to spatial provider
        for ( Value spatialValue : spatialValues )
        {
            // when
            IndexProvider selected = selector.select( nativeProvider, spatialProvider, temporalProvider, luceneProvider, spatialValue );

            // then
            assertSame( spatialProvider, selected );
        }

        // Temporal values should go to temporal provider
        for ( Value temporalValue : temporalValues )
        {
            // when
            IndexProvider selected = selector.select( nativeProvider, spatialProvider, temporalProvider, luceneProvider, temporalValue );

            // then
            assertSame( temporalProvider, selected );
        }

        // Other values should go to lucene provider
        for ( Value otherValue : otherValues )
        {
            // when
            IndexProvider selected = selector.select( nativeProvider, spatialProvider, temporalProvider, luceneProvider, otherValue );

            // then
            assertSame( luceneProvider, selected );
        }

        // All composite values should go to lucene
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                // when
                IndexProvider selected = selector.select( nativeProvider, spatialProvider, temporalProvider, luceneProvider, firstValue, secondValue );

                // then
                assertSame( luceneProvider, selected );
            }
        }
    }

    @Test
    public void mustCombineSamples()
    {
        // given
        int nativeIndexSize = random.nextInt( 0, 1_000_000 );
        int nativeUniqueValues = random.nextInt( 0, 1_000_000 );
        int nativeSampleSize = random.nextInt( 0, 1_000_000 );
        IndexSample nativeSample = new IndexSample( nativeIndexSize, nativeUniqueValues, nativeSampleSize );

        int spatialIndexSize = random.nextInt( 0, 1_000_000 );
        int spatialUniqueValues = random.nextInt( 0, 1_000_000 );
        int spatialSampleSize = random.nextInt( 0, 1_000_000 );
        IndexSample spatialSample = new IndexSample( spatialIndexSize, spatialUniqueValues, spatialSampleSize );

        int temporalIndexSize = random.nextInt( 0, 1_000_000 );
        int temporalUniqueValues = random.nextInt( 0, 1_000_000 );
        int temporalSampleSize = random.nextInt( 0, 1_000_000 );
        IndexSample temporalSample = new IndexSample( temporalIndexSize, temporalUniqueValues, temporalSampleSize );

        int luceneIndexSize = random.nextInt( 0, 1_000_000 );
        int luceneUniqueValues = random.nextInt( 0, 1_000_000 );
        int luceneSampleSize = random.nextInt( 0, 1_000_000 );
        IndexSample luceneSample = new IndexSample( luceneIndexSize, luceneUniqueValues, luceneSampleSize );

        // when
        IndexSample fusionSample =
                FusionIndexProvider.combineSamples( nativeSample, spatialSample, temporalSample, luceneSample );

        // then
        assertEquals( nativeIndexSize + spatialIndexSize + temporalIndexSize + luceneIndexSize, fusionSample.indexSize() );
        assertEquals( nativeUniqueValues + spatialUniqueValues + temporalUniqueValues + luceneUniqueValues, fusionSample.uniqueValues() );
        assertEquals( nativeSampleSize + spatialSampleSize + temporalSampleSize + luceneSampleSize, fusionSample.sampleSize() );
    }

    @Test
    public void getPopulationFailureMustThrowIfNoFailure()
    {
        // given
        FusionIndexProvider fusionIndexProvider = fusionProvider();

        // when
        // ... no failure
        IllegalStateException nativeThrow = new IllegalStateException( "no native failure" );
        IllegalStateException spatialThrow = new IllegalStateException( "no spatial failure" );
        IllegalStateException temporalThrow = new IllegalStateException( "no temporal failure" );
        IllegalStateException luceneThrow = new IllegalStateException( "no lucene failure" );
        when( nativeProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( nativeThrow );
        when( spatialProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( spatialThrow );
        when( temporalProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( temporalThrow );
        when( luceneProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( luceneThrow );
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
    public void getPopulationFailureMustReportFailureWhenNativeFailure()
    {
        FusionIndexProvider fusionIndexProvider = fusionProvider();

        // when
        // ... native failure
        String nativeFailure = "native failure";
        IllegalStateException spatialThrow = new IllegalStateException( "no spatial failure" );
        IllegalStateException temporalThrow = new IllegalStateException( "no temporal failure" );
        IllegalStateException luceneThrow = new IllegalStateException( "no lucene failure" );
        when( nativeProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( nativeFailure );
        when( spatialProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( spatialThrow );
        when( temporalProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( temporalThrow );
        when( luceneProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( luceneThrow );
        // then
        assertThat( fusionIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) ), containsString( nativeFailure ) );
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenSpatialFailure() throws Exception
    {
        FusionIndexProvider fusionIndexProvider = fusionProvider();

        // when
        // ... spatial failure
        IllegalStateException nativeThrow = new IllegalStateException( "no native failure" );
        String spatialFailure = "spatial failure";
        IllegalStateException temporalThrow = new IllegalStateException( "no temporal failure" );
        IllegalStateException luceneThrow = new IllegalStateException( "no lucene failure" );
        when( nativeProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( nativeThrow );
        when( spatialProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( spatialFailure );
        when( temporalProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( temporalThrow );
        when( luceneProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( luceneThrow );
        // then
        assertThat( fusionIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) ), containsString( spatialFailure ) );
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenTemporalFailure() throws Exception
    {
        FusionIndexProvider fusionIndexProvider = fusionProvider();

        // when
        // ... spatial failure
        IllegalStateException nativeThrow = new IllegalStateException( "no native failure" );
        IllegalStateException spatialThrow = new IllegalStateException( "no spatial failure" );
        String temporalFailure = "temporal failure";
        IllegalStateException luceneThrow = new IllegalStateException( "no lucene failure" );
        when( nativeProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( nativeThrow );
        when( spatialProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( spatialThrow );
        when( temporalProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( temporalFailure );
        when( luceneProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( luceneThrow );
        // then
        assertThat( fusionIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) ), containsString( temporalFailure ) );
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenLuceneFailure()
    {
        FusionIndexProvider fusionIndexProvider = fusionProvider();

        // when
        // ... lucene failure
        IllegalStateException nativeThrow = new IllegalStateException( "no native failure" );
        IllegalStateException spatialThrow = new IllegalStateException( "no spatial failure" );
        IllegalStateException temporalThrow = new IllegalStateException( "no temporal failure" );
        String luceneFailure = "lucene failure";
        when( nativeProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( nativeThrow );
        when( spatialProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( spatialThrow );
        when( temporalProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( temporalThrow );
        when( luceneProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( luceneFailure );
        // then
        assertThat( fusionIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) ), containsString( luceneFailure ) );
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenBothNativeAndLuceneFail()
    {
        FusionIndexProvider fusionIndexProvider = fusionProvider();

        // when
        // ... native and lucene failure
        String nativeFailure = "native failure";
        IllegalStateException spatialThrow = new IllegalStateException( "no spatial failure" );
        IllegalStateException temporalThrow = new IllegalStateException( "no temporal failure" );
        String luceneFailure = "lucene failure";
        when( nativeProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( nativeFailure );
        when( spatialProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( spatialThrow );
        when( temporalProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenThrow( temporalThrow );
        when( luceneProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( luceneFailure );
        // then
        String populationFailure = fusionIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) );
        assertThat( populationFailure, containsString( nativeFailure ) );
        assertThat( populationFailure, containsString( luceneFailure ) );
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenAllFail() throws Exception
    {
        FusionIndexProvider fusionIndexProvider = fusionProvider();

        // when
        // ... native and lucene failure
        String nativeFailure = "native failure";
        String spatialFailure = "spatial failure";
        String temporalFailure = "temporal failure";
        String luceneFailure = "lucene failure";
        when( nativeProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( nativeFailure );
        when( spatialProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( spatialFailure );
        when( temporalProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( temporalFailure );
        when( luceneProvider.getPopulationFailure( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( luceneFailure );
        // then
        String populationFailure = fusionIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) );
        assertThat( populationFailure, containsString( nativeFailure ) );
        assertThat( populationFailure, containsString( spatialFailure ) );
        assertThat( populationFailure, containsString( temporalFailure ) );
        assertThat( populationFailure, containsString( luceneFailure ) );
    }

    @Test
    public void shouldReportFailedIfAnyIsFailed()
    {
        // given
        IndexProvider provider = fusionProvider();
        SchemaIndexDescriptor schemaIndexDescriptor = forLabel( 1, 1 );

        for ( InternalIndexState state : InternalIndexState.values() )
        {
            // when
            setInitialState( nativeProvider, InternalIndexState.FAILED );
            setInitialState( spatialProvider, state );
            setInitialState( temporalProvider, state );
            setInitialState( luceneProvider, state );
            InternalIndexState failed1 = provider.getInitialState( 0, schemaIndexDescriptor );

            setInitialState( nativeProvider, state );
            setInitialState( spatialProvider, InternalIndexState.FAILED );
            setInitialState( temporalProvider, state );
            setInitialState( luceneProvider, state );
            InternalIndexState failed2 = provider.getInitialState( 0, schemaIndexDescriptor );

            setInitialState( nativeProvider, state );
            setInitialState( spatialProvider, state );
            setInitialState( temporalProvider, InternalIndexState.FAILED );
            setInitialState( luceneProvider, state );
            InternalIndexState failed3 = provider.getInitialState( 0, schemaIndexDescriptor );

            setInitialState( nativeProvider, state );
            setInitialState( spatialProvider, state );
            setInitialState( temporalProvider, state );
            setInitialState( luceneProvider, InternalIndexState.FAILED );
            InternalIndexState failed4 = provider.getInitialState( 0, schemaIndexDescriptor );

            // then
            assertEquals( InternalIndexState.FAILED, failed1 );
            assertEquals( InternalIndexState.FAILED, failed2 );
            assertEquals( InternalIndexState.FAILED, failed3 );
            assertEquals( InternalIndexState.FAILED, failed4 );
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
            // when
            setInitialState( nativeProvider, InternalIndexState.POPULATING );
            setInitialState( spatialProvider, state );
            setInitialState( temporalProvider, state );
            setInitialState( luceneProvider, state );
            InternalIndexState failed1 = provider.getInitialState( 0, schemaIndexDescriptor );

            setInitialState( nativeProvider, state );
            setInitialState( spatialProvider, InternalIndexState.POPULATING );
            setInitialState( temporalProvider, state );
            setInitialState( luceneProvider, state );
            InternalIndexState failed2 = provider.getInitialState( 0, schemaIndexDescriptor );

            setInitialState( nativeProvider, state );
            setInitialState( spatialProvider, state );
            setInitialState( temporalProvider, InternalIndexState.POPULATING );
            setInitialState( luceneProvider, state );
            InternalIndexState failed3 = provider.getInitialState( 0, schemaIndexDescriptor );

            setInitialState( nativeProvider, state );
            setInitialState( spatialProvider, state );
            setInitialState( temporalProvider, state );
            setInitialState( luceneProvider, InternalIndexState.POPULATING );
            InternalIndexState failed4 = provider.getInitialState( 0, schemaIndexDescriptor );

            // then
            assertEquals( InternalIndexState.POPULATING, failed1 );
            assertEquals( InternalIndexState.POPULATING, failed2 );
            assertEquals( InternalIndexState.POPULATING, failed3 );
            assertEquals( InternalIndexState.POPULATING, failed4 );
        }
    }

    private FusionIndexProvider fusionProvider()
    {
        return new FusionIndexProvider( nativeProvider, spatialProvider, temporalProvider, luceneProvider, new FusionSelector(), DESCRIPTOR, 10, NONE,
                mock( FileSystemAbstraction.class ) );
    }

    private void setInitialState( IndexProvider mockedProvider, InternalIndexState state )
    {
        when( mockedProvider.getInitialState( anyLong(), any( SchemaIndexDescriptor.class ) ) ).thenReturn( state );
    }
}
