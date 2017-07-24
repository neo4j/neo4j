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
package org.neo4j.kernel.impl.index.schema.combined;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
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
import static org.neo4j.kernel.impl.index.schema.combined.CombinedSchemaIndexProvider.select;

public class CombinedSchemaIndexProviderTest
{
    @Rule
    public RandomRule random = new RandomRule();

    @Test
    public void mustThrowForMixedAndReportCorrectForMatchingInitialState() throws Exception
    {
        // given
        SchemaIndexProvider boostProvider = mock( SchemaIndexProvider.class );
        SchemaIndexProvider fallbackProvider = mock( SchemaIndexProvider.class );
        CombinedSchemaIndexProvider combinedSchemaIndexProvider = new CombinedSchemaIndexProvider( boostProvider, fallbackProvider );
        IndexDescriptor anyIndexDescriptor = IndexDescriptorFactory.forLabel( 0, 0 );

        for ( InternalIndexState boostState : InternalIndexState.values() )
        {
            setInitialState( boostProvider, boostState );
            for ( InternalIndexState fallbackState : InternalIndexState.values() )
            {
                setInitialState( fallbackProvider, fallbackState );

                // when
                if ( boostState != fallbackState )
                {
                    // then
                    try
                    {
                        combinedSchemaIndexProvider.getInitialState( 0, anyIndexDescriptor );
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
                    assertSame( boostState, combinedSchemaIndexProvider.getInitialState( 0, anyIndexDescriptor ) );
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
        SchemaIndexProvider boostProvider = mock( SchemaIndexProvider.class );
        SchemaIndexProvider fallbackProvider = mock( SchemaIndexProvider.class );

        Value[] numberValues = CombinedIndexTestHelp.valuesSupportedByBoost();
        Value[] otherValues = CombinedIndexTestHelp.valuesNotSupportedByBoost();
        Value[] allValues = CombinedIndexTestHelp.allValues();

        // Number values should go to boost provider
        for ( Value numberValue : numberValues )
        {
            // when
            SchemaIndexProvider selected = select( boostProvider, fallbackProvider, numberValue );

            // then
            assertSame( boostProvider, selected );
        }

        // Other values should go to fallback provider
        for ( Value otherValue : otherValues )
        {
            // when
            SchemaIndexProvider selected = select( boostProvider, fallbackProvider, otherValue );

            // then
            assertSame( fallbackProvider, selected );
        }

        // All composite values should go to fallback
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                // when
                SchemaIndexProvider selected = select( boostProvider, fallbackProvider, firstValue, secondValue );

                // then
                assertSame( fallbackProvider, selected );
            }
        }
    }

    @Test
    public void mustCombineSamples() throws Exception
    {
        // given
        int boostIndexSize = random.nextInt();
        int boostUniqueValues = random.nextInt();
        int boostSampleSize = random.nextInt();
        IndexSample boostSample = new IndexSample( boostIndexSize, boostUniqueValues, boostSampleSize );

        int fallbackIndexSize = random.nextInt();
        int fallbackUniqueValues = random.nextInt();
        int fallbackSampleSize = random.nextInt();
        IndexSample fallbackSample = new IndexSample( fallbackIndexSize, fallbackUniqueValues, fallbackSampleSize );

        // when
        IndexSample combinedSample = CombinedSchemaIndexProvider.combineSamples( boostSample, fallbackSample );

        // then
        assertEquals( boostIndexSize + fallbackIndexSize, combinedSample.indexSize() );
        assertEquals( boostUniqueValues + fallbackUniqueValues, combinedSample.uniqueValues() );
        assertEquals( boostSampleSize + fallbackSampleSize, combinedSample.sampleSize() );
    }

    @Test
    public void mustReportPopulationFailure() throws Exception
    {
        // given
        SchemaIndexProvider boostProvider = mock( SchemaIndexProvider.class );
        SchemaIndexProvider fallbackProvider = mock( SchemaIndexProvider.class );
        CombinedSchemaIndexProvider combinedSchemaIndexProvider = new CombinedSchemaIndexProvider( boostProvider, fallbackProvider );

        // when
        // ... no failure
        when( boostProvider.getPopulationFailure( anyLong() ) ).thenReturn( null );
        when( fallbackProvider.getPopulationFailure( anyLong() ) ).thenReturn( null );
        // then
        assertNull( combinedSchemaIndexProvider.getPopulationFailure( 0 ) );

        // when
        // ... boost failure
        String boostFailure = "boost failure";
        when( boostProvider.getPopulationFailure( anyLong() ) ).thenReturn( boostFailure );
        when( fallbackProvider.getPopulationFailure( anyLong() ) ).thenReturn( null );
        // then
        assertEquals( boostFailure, combinedSchemaIndexProvider.getPopulationFailure( 0 ) );

        // when
        // ... fallback failure
        String fallbackFailure = "fallback failure";
        when( boostProvider.getPopulationFailure( anyLong() ) ).thenReturn( null );
        when( fallbackProvider.getPopulationFailure( anyLong() ) ).thenReturn( fallbackFailure );
        // then
        assertEquals( fallbackFailure, combinedSchemaIndexProvider.getPopulationFailure( 0 ) );

        // when
        // ... boost and fallback failure
        when( boostProvider.getPopulationFailure( anyLong() ) ).thenReturn( boostFailure );
        when( fallbackProvider.getPopulationFailure( anyLong() ) ).thenReturn( fallbackFailure );
        // then
        String populationFailure = combinedSchemaIndexProvider.getPopulationFailure( 0 );
        assertThat( populationFailure, containsString( boostFailure ) );
        assertThat( populationFailure, containsString( fallbackFailure ) );
    }
}
