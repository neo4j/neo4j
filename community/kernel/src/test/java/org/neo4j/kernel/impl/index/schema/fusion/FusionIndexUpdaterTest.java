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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.EnumMap;
import java.util.function.Function;

import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.SwallowingIndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.Value;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.internal.helpers.ArrayUtil.without;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.change;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.fill;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.remove;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.GENERIC;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.LUCENE;

abstract class FusionIndexUpdaterTest
{
    private final FusionVersion fusionVersion;
    private IndexUpdater[] aliveUpdaters;
    private EnumMap<IndexSlot,IndexUpdater> updaters;
    private FusionIndexUpdater fusionIndexUpdater;

    FusionIndexUpdaterTest( FusionVersion fusionVersion )
    {
        this.fusionVersion = fusionVersion;
    }

    @BeforeEach
    void setup()
    {
        initiateMocks();
    }

    private void initiateMocks()
    {
        IndexSlot[] activeSlots = fusionVersion.aliveSlots();
        updaters = new EnumMap<>( IndexSlot.class );
        fill( updaters, SwallowingIndexUpdater.INSTANCE );
        aliveUpdaters = new IndexUpdater[activeSlots.length];
        for ( int i = 0; i < activeSlots.length; i++ )
        {
            IndexUpdater mock = mock( IndexUpdater.class );
            aliveUpdaters[i] = mock;
            switch ( activeSlots[i] )
            {
            case GENERIC:
                updaters.put( GENERIC, mock );
                break;
            case LUCENE:
                updaters.put( LUCENE, mock );
                break;
            default:
                throw new RuntimeException();
            }
        }
        fusionIndexUpdater = new FusionIndexUpdater( fusionVersion.slotSelector(), new LazyInstanceSelector<>( updaters, throwingFactory() ) );
    }

    private Function<IndexSlot,IndexUpdater> throwingFactory()
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
    void processMustSelectCorrectForAdd() throws Exception
    {
        // given
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( IndexSlot slot : IndexSlot.values() )
        {
            for ( Value value : values.get( slot ) )
            {
                // then
                verifyAddWithCorrectUpdater( orLucene( updaters.get( slot ) ), value );
            }
        }

        // when value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectUpdater( updaters.get( GENERIC ), firstValue, secondValue );
            }
        }
    }

    @Test
    void processMustSelectCorrectForRemove() throws Exception
    {
        // given
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( IndexSlot slot : IndexSlot.values() )
        {
            for ( Value value : values.get( slot ) )
            {
                // then
                verifyRemoveWithCorrectUpdater( orLucene( updaters.get( slot ) ), value );
            }
        }

        // when value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyRemoveWithCorrectUpdater( updaters.get( GENERIC ), firstValue, secondValue );
            }
        }
    }

    @Test
    void processMustSelectCorrectForChange() throws Exception
    {
        // given
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();

        // when
        for ( IndexSlot slot : IndexSlot.values() )
        {
            for ( Value before : values.get( slot ) )
            {
                for ( Value after : values.get( slot ) )
                {
                    verifyChangeWithCorrectUpdaterNotMixed( orLucene( updaters.get( slot ) ), before, after );
                }
            }
        }
    }

    @Test
    void processMustSelectCorrectForChangeFromOneGroupToAnother() throws Exception
    {
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();
        for ( IndexSlot from : IndexSlot.values() )
        {
            // given
            for ( IndexSlot to : IndexSlot.values() )
            {
                if ( from != to )
                {
                    // when
                    verifyChangeWithCorrectUpdaterMixed(
                            orLucene( updaters.get( from ) ), orLucene( updaters.get( to ) ), values.get( from ), values.get( to ) );
                }
                else
                {
                    verifyChangeWithCorrectUpdaterNotMixed( orLucene( updaters.get( from ) ), values.get( from ) );
                }
                resetMocks();
            }
        }
    }

    private IndexUpdater orLucene( IndexUpdater updater )
    {
        return updater != SwallowingIndexUpdater.INSTANCE ? updater : updaters.get( LUCENE );
    }

    private void verifyAddWithCorrectUpdater( IndexUpdater correctPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = add( numberValues );
        fusionIndexUpdater.process( update );
        verify( correctPopulator ).process( update );
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
        verify( correctPopulator ).process( update );
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
        verify( correctPopulator ).process( update );
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
                    verify( expectRemoveFrom ).process( change( before, after ) );
                }
            }
        }
    }

    /* close */

    @Test
    void closeMustCloseAll() throws Exception
    {
        // when
        fusionIndexUpdater.close();

        // then
        for ( IndexUpdater updater : aliveUpdaters )
        {
            verify( updater ).close();
        }
    }

    @Test
    void closeMustThrowIfAnyThrow() throws Exception
    {
        for ( IndexSlot indexSlot : fusionVersion.aliveSlots() )
        {
            FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updaters.get( indexSlot ), fusionIndexUpdater );
            initiateMocks();
        }
    }

    @Test
    void closeMustCloseOthersIfAnyThrow() throws Exception
    {
        for ( IndexSlot indexSlot : fusionVersion.aliveSlots() )
        {
            IndexUpdater failingUpdater = updaters.get( indexSlot );
            FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( failingUpdater, fusionIndexUpdater, without( aliveUpdaters, failingUpdater ) );
            initiateMocks();
        }
    }

    @Test
    void closeMustThrowIfAllThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow( fusionIndexUpdater, aliveUpdaters );
    }

    @Test
    void shouldInstantiatePartLazilyForSpecificValueGroupUpdates() throws IOException, IndexEntryConflictException
    {
        // given
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();
        for ( IndexSlot i : IndexSlot.values() )
        {
            if ( updaters.get( i ) != SwallowingIndexUpdater.INSTANCE )
            {
                // when
                Value value = values.get( i )[0];
                fusionIndexUpdater.process( add( value ) );
                for ( IndexSlot j : IndexSlot.values() )
                {
                    // then
                    if ( updaters.get( j ) != SwallowingIndexUpdater.INSTANCE )
                    {
                        if ( i == j )
                        {
                            verify( updaters.get( i ) ).process( any( IndexEntryUpdate.class ) );
                        }
                        else
                        {
                            verifyNoMoreInteractions( updaters.get( j ) );
                        }
                    }
                }
            }

            initiateMocks();
        }
    }
}
