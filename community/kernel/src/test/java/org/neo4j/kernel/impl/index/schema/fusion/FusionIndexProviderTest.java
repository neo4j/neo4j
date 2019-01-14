/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.NumberIndexProvider;
import org.neo4j.kernel.impl.index.schema.SpatialIndexProvider;
import org.neo4j.kernel.impl.index.schema.StringIndexProvider;
import org.neo4j.kernel.impl.index.schema.TemporalIndexProvider;
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
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.GROUP_OF;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v00;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v10;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v20;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.INSTANCE_COUNT;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.TEMPORAL;

@RunWith( Parameterized.class )
public class FusionIndexProviderTest
{
    private static final IndexProvider.Descriptor DESCRIPTOR = new IndexProvider.Descriptor( "test-fusion", "1" );

    private IndexProvider[] providers;
    private IndexProvider[] aliveProviders;
    private IndexProvider fusionIndexProvider;
    private SlotSelector slotSelector;
    private InstanceSelector<IndexProvider> instanceSelector;

    @Parameterized.Parameters( name = "{0}" )
    public static FusionVersion[] versions()
    {
        return new FusionVersion[]
                {
                        v00, v10, v20
                };
    }

    @Parameterized.Parameter
    public static FusionVersion fusionVersion;

    @Before
    public void setup()
    {
        slotSelector = fusionVersion.slotSelector();
        setupMocks();
    }

    @Rule
    public RandomRule random = new RandomRule();

    @Test
    public void mustSelectCorrectTargetForAllGivenValueCombinations()
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( int i = 0; i < values.length; i++ )
        {
            Value[] group = values[i];
            for ( Value value : group )
            {
                // when
                IndexProvider selected = instanceSelector.select( slotSelector.selectSlot( array( value ), GROUP_OF ) );

                // then
                assertSame( orLucene( providers[i] ), selected );
            }
        }

        // All composite values should go to lucene
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                // when
                IndexProvider selected = instanceSelector.select( slotSelector.selectSlot( array( firstValue, secondValue ), GROUP_OF ) );

                // then
                assertSame( providers[LUCENE], selected );
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
        // when
        // ... no failure
        IllegalStateException failure = new IllegalStateException( "not failed" );
        for ( IndexProvider provider : aliveProviders )
        {
            when( provider.getPopulationFailure( anyLong(), any( SchemaIndexDescriptor.class ) ) ).thenThrow( failure );
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
        for ( IndexProvider failingProvider : aliveProviders )
        {
            // when
            String failure = "failure";
            IllegalStateException exception = new IllegalStateException( "not failed" );
            for ( IndexProvider provider : aliveProviders )
            {
                if ( provider == failingProvider )
                {
                    when( provider.getPopulationFailure( anyLong(), any( SchemaIndexDescriptor.class ) ) ).thenReturn( failure );
                }
                else
                {
                    when( provider.getPopulationFailure( anyLong(), any( SchemaIndexDescriptor.class ) ) ).thenThrow( exception );
                }
            }

            // then
            assertThat( fusionIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) ), containsString( failure ) );
        }
    }

    @Test
    public void getPopulationFailureMustReportFailureWhenMultipleFail()
    {
        // when
        List<String> failureMessages = new ArrayList<>();
        for ( IndexProvider aliveProvider : aliveProviders )
        {
            String failureMessage = "FAILURE[" + aliveProvider + "]";
            failureMessages.add( failureMessage );
            when( aliveProvider.getPopulationFailure( anyLong(), any( SchemaIndexDescriptor.class ) ) ).thenReturn( failureMessage );
        }

        // then
        String populationFailure = fusionIndexProvider.getPopulationFailure( 0, forLabel( 0, 0 ) );
        for ( String failureMessage : failureMessages )
        {
            assertThat( populationFailure, containsString( failureMessage ) );
        }
    }

    @Test
    public void shouldReportFailedIfAnyIsFailed()
    {
        // given
        IndexProvider provider = fusionIndexProvider;
        SchemaIndexDescriptor schemaIndexDescriptor = SchemaIndexDescriptorFactory.forLabel( 1, 1 );

        for ( InternalIndexState state : InternalIndexState.values() )
        {
            for ( IndexProvider failedProvider : aliveProviders )
            {
                // when
                for ( IndexProvider aliveProvider : aliveProviders )
                {
                    setInitialState( aliveProvider, failedProvider == aliveProvider ? InternalIndexState.FAILED : state );
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
        SchemaIndexDescriptor schemaIndexDescriptor = SchemaIndexDescriptorFactory.forLabel( 1, 1 );

        for ( InternalIndexState state : array( InternalIndexState.ONLINE, InternalIndexState.POPULATING ) )
        {
            for ( IndexProvider populatingProvider : aliveProviders )
            {
                // when
                for ( IndexProvider aliveProvider : aliveProviders )
                {
                    setInitialState( aliveProvider, populatingProvider == aliveProvider ? InternalIndexState.POPULATING : state );
                }
                InternalIndexState initialState = fusionIndexProvider.getInitialState( 0, schemaIndexDescriptor );

                // then
                assertEquals( InternalIndexState.POPULATING, initialState );
            }
        }
    }

    private void setupMocks()
    {
        int[] aliveSlots = fusionVersion.aliveSlots();
        aliveProviders = new IndexProvider[aliveSlots.length];
        providers = new IndexProvider[INSTANCE_COUNT];
        Arrays.fill( providers, IndexProvider.EMPTY );
        for ( int i = 0; i < aliveSlots.length; i++ )
        {
            switch ( aliveSlots[i] )
            {
            case STRING:
                IndexProvider string = mockProvider( StringIndexProvider.class, "string" );
                providers[STRING] = string;
                aliveProviders[i] = string;
                break;
            case NUMBER:
                IndexProvider number = mockProvider( NumberIndexProvider.class, "number" );
                providers[NUMBER] = number;
                aliveProviders[i] = number;
                break;
            case SPATIAL:
                IndexProvider spatial = mockProvider( SpatialIndexProvider.class, "spatial" );
                providers[SPATIAL] = spatial;
                aliveProviders[i] = spatial;
                break;
            case TEMPORAL:
                IndexProvider temporal = mockProvider( TemporalIndexProvider.class, "temporal" );
                providers[TEMPORAL] = temporal;
                aliveProviders[i] = temporal;
                break;
            case LUCENE:
                IndexProvider lucene = mockProvider( IndexProvider.class, "lucene" );
                providers[LUCENE] = lucene;
                aliveProviders[i] = lucene;
                break;
            default:
                throw new RuntimeException();
            }
        }
        fusionIndexProvider = new FusionIndexProvider(
                providers[STRING],
                providers[NUMBER],
                providers[SPATIAL],
                providers[TEMPORAL],
                providers[LUCENE],
                fusionVersion.slotSelector(), DESCRIPTOR, 10, NONE, mock( FileSystemAbstraction.class ), false );
        instanceSelector = new InstanceSelector<>( providers );
    }

    private IndexProvider mockProvider( Class<? extends IndexProvider> providerClass, String name )
    {
        IndexProvider mock = mock( providerClass );
        when( mock.getProviderDescriptor() ).thenReturn( new IndexProvider.Descriptor( name, "1" ) );
        return mock;
    }

    private void setInitialState( IndexProvider mockedProvider, InternalIndexState state )
    {
        when( mockedProvider.getInitialState( anyLong(), any( SchemaIndexDescriptor.class ) ) ).thenReturn( state );
    }

    private IndexProvider orLucene( IndexProvider provider )
    {
        return provider != IndexProvider.EMPTY ? provider : providers[LUCENE];
    }
}
