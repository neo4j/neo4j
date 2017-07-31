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

import org.junit.Rule;
import org.junit.Test;

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FusionSchemaIndexProviderTest
{
    private static final SchemaIndexProvider.Descriptor DESCRIPTOR = new SchemaIndexProvider.Descriptor( "test-fusion", "1" );

    @Rule
    public RandomRule random = new RandomRule();

    @Test
    public void mustThrowForMixedAndReportCorrectForMatchingInitialState() throws Exception
    {
        // given
        SchemaIndexProvider nativeProvider = mock( SchemaIndexProvider.class );
        SchemaIndexProvider luceneProvider = mock( SchemaIndexProvider.class );
        when( nativeProvider.getProviderDescriptor() ).thenReturn( new SchemaIndexProvider.Descriptor( "native", "1" ) );
        when( luceneProvider.getProviderDescriptor() ).thenReturn( new SchemaIndexProvider.Descriptor( "lucene", "1" ) );
        FusionSchemaIndexProvider fusionSchemaIndexProvider =
                new FusionSchemaIndexProvider( nativeProvider, luceneProvider, new NativeSelector(), DESCRIPTOR, 10 );
        IndexDescriptor anyIndexDescriptor = IndexDescriptorFactory.forLabel( 0, 0 );

        for ( InternalIndexState nativeState : InternalIndexState.values() )
        {
            setInitialState( nativeProvider, nativeState );
            for ( InternalIndexState luceneState : InternalIndexState.values() )
            {
                setInitialState( luceneProvider, luceneState );

                // when
                if ( nativeState != luceneState )
                {
                    // then
                    try
                    {
                        fusionSchemaIndexProvider.getInitialState( 0, anyIndexDescriptor );
                        fail( "Should have failed" );
                    }
                    catch ( IllegalStateException e )
                    {
                        // good
                    }
                }
                else
                {
                    // or then
                    assertSame( nativeState, fusionSchemaIndexProvider.getInitialState( 0, anyIndexDescriptor ) );
                }
            }
        }
    }

    private void setInitialState( SchemaIndexProvider mockedProvider, InternalIndexState state )
    {
        when( mockedProvider.getInitialState( anyLong(), any( IndexDescriptor.class ) ) ).thenReturn( state );
    }

    @Test
    public void mustSelectCorrectTargetForAllGivenValueCombinations() throws Exception
    {
        // given
        SchemaIndexProvider nativeProvider = mock( SchemaIndexProvider.class );
        SchemaIndexProvider luceneProvider = mock( SchemaIndexProvider.class );

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
    public void mustReportPopulationFailure() throws Exception
    {
        // given
        SchemaIndexProvider nativeProvider = mock( SchemaIndexProvider.class );
        SchemaIndexProvider luceneProvider = mock( SchemaIndexProvider.class );
        when( nativeProvider.getProviderDescriptor() ).thenReturn( new SchemaIndexProvider.Descriptor( "native", "1" ) );
        when( luceneProvider.getProviderDescriptor() ).thenReturn( new SchemaIndexProvider.Descriptor( "lucene", "1" ) );
        FusionSchemaIndexProvider fusionSchemaIndexProvider =
                new FusionSchemaIndexProvider( nativeProvider, luceneProvider, new NativeSelector(), DESCRIPTOR, 10 );

        // when
        // ... no failure
        when( nativeProvider.getPopulationFailure( anyLong() ) ).thenReturn( null );
        when( luceneProvider.getPopulationFailure( anyLong() ) ).thenReturn( null );
        // then
        assertNull( fusionSchemaIndexProvider.getPopulationFailure( 0 ) );

        // when
        // ... native failure
        String nativeFailure = "native failure";
        when( nativeProvider.getPopulationFailure( anyLong() ) ).thenReturn( nativeFailure );
        when( luceneProvider.getPopulationFailure( anyLong() ) ).thenReturn( null );
        // then
        assertEquals( nativeFailure, fusionSchemaIndexProvider.getPopulationFailure( 0 ) );

        // when
        // ... lucene failure
        String luceneFailure = "lucene failure";
        when( nativeProvider.getPopulationFailure( anyLong() ) ).thenReturn( null );
        when( luceneProvider.getPopulationFailure( anyLong() ) ).thenReturn( luceneFailure );
        // then
        assertEquals( luceneFailure, fusionSchemaIndexProvider.getPopulationFailure( 0 ) );

        // when
        // ... native and lucene failure
        when( nativeProvider.getPopulationFailure( anyLong() ) ).thenReturn( nativeFailure );
        when( luceneProvider.getPopulationFailure( anyLong() ) ).thenReturn( luceneFailure );
        // then
        String populationFailure = fusionSchemaIndexProvider.getPopulationFailure( 0 );
        assertThat( populationFailure, containsString( nativeFailure ) );
        assertThat( populationFailure, containsString( luceneFailure ) );
    }
}
