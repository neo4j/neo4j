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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.NativeSelector;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.Selector;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.NONE;

public class FusionSchemaIndexProviderTest
{
    private static final SchemaIndexProvider.Descriptor DESCRIPTOR = new SchemaIndexProvider.Descriptor( "test-fusion", "1" );

    private SchemaIndexProvider nativeProvider;
    private SchemaIndexProvider luceneProvider;

    @Before
    public void setup()
    {
        nativeProvider = mock( SchemaIndexProvider.class );
        luceneProvider = mock( SchemaIndexProvider.class );
        when( nativeProvider.getProviderDescriptor() ).thenReturn( new SchemaIndexProvider.Descriptor( "native", "1" ) );
        when( luceneProvider.getProviderDescriptor() ).thenReturn( new SchemaIndexProvider.Descriptor( "lucene", "1" ) );
    }

    @Rule
    public RandomRule random = new RandomRule();

    @Test
    public void mustSelectCorrectTargetForAllGivenValueCombinations() throws Exception
    {
        // given
        Value[] numberValues = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] otherValues = FusionIndexTestHelp.valuesNotSupportedByNative();
        Value[] allValues = FusionIndexTestHelp.allValues();

        // Number values should go to native provider
        Selector selector = new NativeSelector();
        for ( Value numberValue : numberValues )
        {
            // when
            SchemaIndexProvider selected = selector.select( nativeProvider, luceneProvider, numberValue );

            // then
            assertSame( nativeProvider, selected );
        }

        // Other values should go to lucene provider
        for ( Value otherValue : otherValues )
        {
            // when
            SchemaIndexProvider selected = selector.select( nativeProvider, luceneProvider, otherValue );

            // then
            assertSame( luceneProvider, selected );
        }

        // All composite values should go to lucene
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                // when
                SchemaIndexProvider selected = selector.select( nativeProvider, luceneProvider, firstValue, secondValue );

                // then
                assertSame( luceneProvider, selected );
            }
        }
    }

    @Test
    public void mustCombineSamples() throws Exception
    {
        // given
        int nativeIndexSize = random.nextInt( 0, 1_000_000 );
        int nativeUniqueValues = random.nextInt( 0, 1_000_000 );
        int nativeSampleSize = random.nextInt( 0, 1_000_000 );
        IndexSample nativeSample = new IndexSample( nativeIndexSize, nativeUniqueValues, nativeSampleSize );

        int luceneIndexSize = random.nextInt( 0, 1_000_000 );
        int luceneUniqueValues = random.nextInt( 0, 1_000_000 );
        int luceneSampleSize = random.nextInt( 0, 1_000_000 );
        IndexSample luceneSample = new IndexSample( luceneIndexSize, luceneUniqueValues, luceneSampleSize );

        // when
        IndexSample fusionSample = FusionSchemaIndexProvider.combineSamples( nativeSample, luceneSample );

        // then
        assertEquals( nativeIndexSize + luceneIndexSize, fusionSample.indexSize() );
        assertEquals( nativeUniqueValues + luceneUniqueValues, fusionSample.uniqueValues() );
        assertEquals( nativeSampleSize + luceneSampleSize, fusionSample.sampleSize() );
    }

    @Test
    public void getPopulationFailureMustThrowIfNoFailure() throws Exception
    {
        // given
        FusionSchemaIndexProvider fusionSchemaIndexProvider = fusionProvider();

        // when
        // ... no failure
        IllegalStateException nativeThrow = new IllegalStateException( "no native failure" );
        IllegalStateException luceneThrow = new IllegalStateException( "no lucene failure" );
        when( nativeProvider.getPopulationFailure( anyLong() ) ).thenThrow( nativeThrow );
        when( luceneProvider.getPopulationFailure( anyLong() ) ).thenThrow( luceneThrow );
        // then
        try
        {
            fusionSchemaIndexProvider.getPopulationFailure( 0 );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {   // good
        }
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenNativeFailure() throws Exception
    {
        FusionSchemaIndexProvider fusionSchemaIndexProvider = fusionProvider();

        // when
        // ... native failure
        String nativeFailure = "native failure";
        IllegalStateException luceneThrow = new IllegalStateException( "no lucene failure" );
        when( nativeProvider.getPopulationFailure( anyLong() ) ).thenReturn( nativeFailure );
        when( luceneProvider.getPopulationFailure( anyLong() ) ).thenThrow( luceneThrow );
        // then
        assertThat( fusionSchemaIndexProvider.getPopulationFailure( 0 ), containsString( nativeFailure ) );
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenLuceneFailure() throws Exception
    {
        FusionSchemaIndexProvider fusionSchemaIndexProvider = fusionProvider();

        // when
        // ... lucene failure
        String luceneFailure = "lucene failure";
        IllegalStateException nativeThrow = new IllegalStateException( "no native failure" );
        when( nativeProvider.getPopulationFailure( anyLong() ) ).thenThrow( nativeThrow );
        when( luceneProvider.getPopulationFailure( anyLong() ) ).thenReturn( luceneFailure );
        // then
        assertThat( fusionSchemaIndexProvider.getPopulationFailure( 0 ), containsString( luceneFailure ) );
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenBothFailure() throws Exception
    {
        FusionSchemaIndexProvider fusionSchemaIndexProvider = fusionProvider();

        // when
        // ... native and lucene failure
        String luceneFailure = "lucene failure";
        String nativeFailure = "native failure";
        when( nativeProvider.getPopulationFailure( anyLong() ) ).thenReturn( nativeFailure );
        when( luceneProvider.getPopulationFailure( anyLong() ) ).thenReturn( luceneFailure );
        // then
        String populationFailure = fusionSchemaIndexProvider.getPopulationFailure( 0 );
        assertThat( populationFailure, containsString( nativeFailure ) );
        assertThat( populationFailure, containsString( luceneFailure ) );
    }

    @Test
    public void shouldReportFailedIfAnyIsFailed() throws Exception
    {
        // given
        SchemaIndexProvider provider = fusionProvider();
        IndexDescriptor indexDescriptor = IndexDescriptorFactory.forLabel( 1, 1 );

        for ( InternalIndexState state : InternalIndexState.values() )
        {
            // when
            setInitialState( nativeProvider, InternalIndexState.FAILED );
            setInitialState( luceneProvider, state );
            InternalIndexState failed1 = provider.getInitialState( 0, indexDescriptor );

            setInitialState( nativeProvider, state );
            setInitialState( luceneProvider, InternalIndexState.FAILED );
            InternalIndexState failed2 = provider.getInitialState( 0, indexDescriptor );

            // then
            assertEquals( InternalIndexState.FAILED, failed1 );
            assertEquals( InternalIndexState.FAILED, failed2 );
        }
    }

    @Test
    public void shouldReportPopulatingIfAnyIsPopulating() throws Exception
    {
        // given
        SchemaIndexProvider provider = fusionProvider();
        IndexDescriptor indexDescriptor = IndexDescriptorFactory.forLabel( 1, 1 );

        for ( InternalIndexState state : array( InternalIndexState.ONLINE, InternalIndexState.POPULATING ) )
        {
            // when
            setInitialState( nativeProvider, InternalIndexState.POPULATING );
            setInitialState( luceneProvider, state );
            InternalIndexState failed1 = provider.getInitialState( 0, indexDescriptor );

            setInitialState( nativeProvider, state );
            setInitialState( luceneProvider, InternalIndexState.POPULATING );
            InternalIndexState failed2 = provider.getInitialState( 0, indexDescriptor );

            // then
            assertEquals( InternalIndexState.POPULATING, failed1 );
            assertEquals( InternalIndexState.POPULATING, failed2 );
        }
    }

    private FusionSchemaIndexProvider fusionProvider()
    {
        return new FusionSchemaIndexProvider( nativeProvider, luceneProvider, new NativeSelector(), DESCRIPTOR, 10, NONE,
                mock( FileSystemAbstraction.class ) );
    }

    private void setInitialState( SchemaIndexProvider mockedProvider, InternalIndexState state )
    {
        when( mockedProvider.getInitialState( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( state );
    }
}
