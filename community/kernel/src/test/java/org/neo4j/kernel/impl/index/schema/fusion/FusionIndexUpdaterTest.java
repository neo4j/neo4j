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

import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntFunction;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.updater.SwallowingIndexUpdater;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.helpers.ArrayUtil.without;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.change;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.remove;
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
public class FusionIndexUpdaterTest
{
    private IndexUpdater[] aliveUpdaters;
    private IndexUpdater[] updaters;
    private FusionIndexUpdater fusionIndexUpdater;

    @Rule
    public RandomRule random = new RandomRule();
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
        initiateMocks();
    }

    private void initiateMocks()
    {
        int[] activeSlots = fusionVersion.aliveSlots();
        updaters = new IndexUpdater[INSTANCE_COUNT];
        Arrays.fill( updaters, SwallowingIndexUpdater.INSTANCE );
        aliveUpdaters = new IndexUpdater[activeSlots.length];
        for ( int i = 0; i < activeSlots.length; i++ )
        {
            IndexUpdater mock = mock( IndexUpdater.class );
            aliveUpdaters[i] = mock;
            switch ( activeSlots[i] )
            {
            case STRING:
                updaters[STRING] = mock;
                break;
            case NUMBER:
                updaters[NUMBER] = mock;
                break;
            case SPATIAL:
                updaters[SPATIAL] = mock;
                break;
            case TEMPORAL:
                updaters[TEMPORAL] = mock;
                break;
            case LUCENE:
                updaters[LUCENE] = mock;
                break;
            default:
                throw new RuntimeException();
            }
        }
        fusionIndexUpdater = new FusionIndexUpdater( fusionVersion.slotSelector(), new LazyInstanceSelector<>( updaters, throwingFactory() ) );
    }

    private IntFunction<IndexUpdater> throwingFactory()
    {
        return i ->
        {
            throw new IllegalStateException( "All updaters should exist already" );
        };
    }

    private void resetMocks()
    {
        for ( IndexUpdater updater : aliveUpdaters )
        {
            reset( updater );
        }
    }

    /* process */

    @Test
    public void processMustSelectCorrectForAdd() throws Exception
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( int i = 0; i < updaters.length; i++ )
        {
            for ( Value value : values[i] )
            {
                // then
                verifyAddWithCorrectUpdater( orLucene( updaters[i] ), value );
            }
        }

        // when value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectUpdater( updaters[LUCENE], firstValue, secondValue );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForRemove() throws Exception
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( int i = 0; i < updaters.length; i++ )
        {
            for ( Value value : values[i] )
            {
                // then
                verifyRemoveWithCorrectUpdater( orLucene( updaters[i] ), value );
            }
        }

        // when value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyRemoveWithCorrectUpdater( updaters[LUCENE], firstValue, secondValue );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForChange() throws Exception
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();

        // when
        for ( int i = 0; i < updaters.length; i++ )
        {
            for ( Value before : values[i] )
            {
                for ( Value after : values[i] )
                {
                    verifyChangeWithCorrectUpdaterNotMixed( orLucene( updaters[i] ), before, after );
                }
            }
        }
    }

    @Test
    public void processMustSelectCorrectForChangeFromOneGroupToAnother() throws Exception
    {
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        for ( int f = 0; f < values.length; f++ )
        {
            // given
            for ( int t = 0; t < values.length; t++ )
            {
                if ( f != t )
                {
                    // when
                    verifyChangeWithCorrectUpdaterMixed( orLucene( updaters[f] ), orLucene( updaters[t] ), values[f], values[t] );
                }
                else
                {
                    verifyChangeWithCorrectUpdaterNotMixed( orLucene( updaters[f] ), values[f] );
                }
                resetMocks();
            }
        }
    }

    private IndexUpdater orLucene( IndexUpdater updater )
    {
        return updater != SwallowingIndexUpdater.INSTANCE ? updater : updaters[LUCENE];
    }

    private void verifyAddWithCorrectUpdater( IndexUpdater correctPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = add( numberValues );
        fusionIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        for ( IndexUpdater populator : aliveUpdaters )
        {
            if ( populator != correctPopulator )
            {
                verify( populator, never() ).process( update );
            }
        }
    }

    private void verifyRemoveWithCorrectUpdater( IndexUpdater correctPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = FusionIndexTestHelp.remove( numberValues );
        fusionIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        for ( IndexUpdater populator : aliveUpdaters )
        {
            if ( populator != correctPopulator )
            {
                verify( populator, never() ).process( update );
            }
        }
    }

    private void verifyChangeWithCorrectUpdaterNotMixed( IndexUpdater correctPopulator, Value before,
            Value after ) throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = FusionIndexTestHelp.change( before, after );
        fusionIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        for ( IndexUpdater populator : aliveUpdaters )
        {
            if ( populator != correctPopulator )
            {
                verify( populator, never() ).process( update );
            }
        }
    }

    private void verifyChangeWithCorrectUpdaterNotMixed( IndexUpdater updater, Value[] supportedValues ) throws IndexEntryConflictException, IOException
    {
        for ( Value before : supportedValues )
        {
            for ( Value after : supportedValues )
            {
                verifyChangeWithCorrectUpdaterNotMixed( updater, before, after );
            }
        }
    }

    private void verifyChangeWithCorrectUpdaterMixed( IndexUpdater expectRemoveFrom, IndexUpdater expectAddTo, Value[] beforeValues,
            Value[] afterValues ) throws IOException, IndexEntryConflictException
    {
        for ( int beforeIndex = 0; beforeIndex < beforeValues.length; beforeIndex++ )
        {
            Value before = beforeValues[beforeIndex];
            for ( int afterIndex = 0; afterIndex < afterValues.length; afterIndex++ )
            {
                Value after = afterValues[afterIndex];

                IndexEntryUpdate<LabelSchemaDescriptor> change = change( before, after );
                fusionIndexUpdater.process( change );

                if ( expectRemoveFrom != expectAddTo )
                {
                    verify( expectRemoveFrom, times( afterIndex + 1 ) ).process( remove( before ) );
                    verify( expectAddTo, times( beforeIndex + 1 ) ).process( add( after ) );
                }
                else
                {
                    verify( expectRemoveFrom, times( 1 ) ).process( change( before, after ) );
                }
            }
        }
    }

    /* close */

    @Test
    public void closeMustCloseAll() throws Exception
    {
        // when
        fusionIndexUpdater.close();

        // then
        for ( IndexUpdater updater : aliveUpdaters )
        {
            verify( updater, times( 1 ) ).close();
        }
    }

    @Test
    public void closeMustThrowIfAnyThrow() throws Exception
    {
        for ( int i = 0; i < aliveUpdaters.length; i++ )
        {
            IndexUpdater updater = aliveUpdaters[i];
            FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updater, fusionIndexUpdater );
            initiateMocks();
        }
    }

    @Test
    public void closeMustCloseOthersIfAnyThrow() throws Exception
    {
        for ( int i = 0; i < aliveUpdaters.length; i++ )
        {
            IndexUpdater updater = aliveUpdaters[i];
            FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( updater, fusionIndexUpdater, without( aliveUpdaters, updater ) );
            initiateMocks();
        }
    }

    @Test
    public void closeMustThrowIfAllThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow( fusionIndexUpdater, aliveUpdaters );
    }

    @Test
    public void shouldInstantiatePartLazilyForSpecificValueGroupUpdates() throws IOException, IndexEntryConflictException
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        for ( int i = 0; i < updaters.length; i++ )
        {
            if ( updaters[i] != SwallowingIndexUpdater.INSTANCE )
            {
                // when
                Value value = values[i][0];
                fusionIndexUpdater.process( add( value ) );
                for ( int j = 0; j < updaters.length; j++ )
                {
                    // then
                    if ( updaters[j] != SwallowingIndexUpdater.INSTANCE )
                    {
                        if ( i == j )
                        {
                            verify( updaters[i] ).process( any( IndexEntryUpdate.class ) );
                        }
                        else
                        {
                            verifyNoMoreInteractions( updaters[j] );
                        }
                    }
                }
            }

            initiateMocks();
        }
    }
}
