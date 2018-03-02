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

import java.io.IOException;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.ArrayUtil.without;
import static org.neo4j.helpers.collection.Iterators.array;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.change;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.remove;

public class FusionIndexUpdaterTest
{
    private IndexUpdater luceneUpdater;
    private IndexUpdater[] allUpdaters;
    private FusionIndexUpdater fusionIndexUpdater;

    @Rule
    public RandomRule random = new RandomRule();

    @Before
    public void mockComponents()
    {
        IndexUpdater stringUpdater = mock( IndexUpdater.class );
        IndexUpdater numberUpdater = mock( IndexUpdater.class );
        IndexUpdater spatialUpdater = mock( IndexUpdater.class );
        IndexUpdater temporalUpdater = mock( IndexUpdater.class );
        luceneUpdater = mock( IndexUpdater.class );
        allUpdaters = array( stringUpdater, numberUpdater, spatialUpdater, temporalUpdater, luceneUpdater );
        fusionIndexUpdater = new FusionIndexUpdater( allUpdaters, new FusionSelector() );
    }

    /* process */

    @Test
    public void processMustSelectCorrectForAdd() throws Exception
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( int i = 0; i < allUpdaters.length; i++ )
        {
            for ( Value value : values[i] )
            {
                // then
                verifyAddWithCorrectUpdater( allUpdaters[i], value );
            }
        }

        // when value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectUpdater( luceneUpdater, firstValue, secondValue );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForRemove() throws Exception
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( int i = 0; i < allUpdaters.length; i++ )
        {
            for ( Value value : values[i] )
            {
                // then
                verifyRemoveWithCorrectUpdater( allUpdaters[i], value );
            }
        }

        // when value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyRemoveWithCorrectUpdater( luceneUpdater, firstValue, secondValue );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForChange() throws Exception
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();

        // when
        for ( int i = 0; i < allUpdaters.length; i++ )
        {
            for ( Value before : values[i] )
            {
                for ( Value after : values[i] )
                {
                    verifyChangeWithCorrectUpdaterNotMixed( allUpdaters[i], before, after );
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
                    verifyChangeWithCorrectUpdaterMixed( allUpdaters[f], allUpdaters[t], values[f], values[t] );
                }
                else
                {
                    verifyChangeWithCorrectUpdaterNotMixed( allUpdaters[f], values[f] );
                }
                mockComponents();
            }
        }
    }

    private void verifyAddWithCorrectUpdater( IndexUpdater correctPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = add( numberValues );
        fusionIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        for ( IndexUpdater populator : allUpdaters )
        {
            if ( populator != correctPopulator )
            {
                verify( populator, times( 0 ) ).process( update );
            }
        }
    }

    private void verifyRemoveWithCorrectUpdater( IndexUpdater correctPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = FusionIndexTestHelp.remove( numberValues );
        fusionIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        for ( IndexUpdater populator : allUpdaters )
        {
            if ( populator != correctPopulator )
            {
                verify( populator, times( 0 ) ).process( update );
            }
        }
    }

    private void verifyChangeWithCorrectUpdaterNotMixed( IndexUpdater correctPopulator, Value before,
            Value after ) throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = FusionIndexTestHelp.change( before, after );
        fusionIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        for ( IndexUpdater populator : allUpdaters )
        {
            if ( populator != correctPopulator )
            {
                verify( populator, times( 0 ) ).process( update );
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

                verify( expectRemoveFrom, times( afterIndex + 1 ) ).process( remove( before ) );
                verify( expectAddTo, times( beforeIndex + 1 ) ).process( add( after ) );
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
        for ( IndexUpdater updater : allUpdaters )
        {
            verify( updater, times( 1 ) ).close();
        }
    }

    @Test
    public void closeMustThrowIfAnyThrow() throws Exception
    {
        for ( int i = 0; i < allUpdaters.length; i++ )
        {
            IndexUpdater updater = allUpdaters[i];
            FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updater, fusionIndexUpdater );
            mockComponents();
        }
    }

    @Test
    public void closeMustCloseOthersIfAnyThrow() throws Exception
    {
        for ( int i = 0; i < allUpdaters.length; i++ )
        {
            IndexUpdater updater = allUpdaters[i];
            FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( updater, fusionIndexUpdater, without( allUpdaters, updater ) );
            mockComponents();
        }
    }

    @Test
    public void closeMustThrowIfAllThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow( fusionIndexUpdater, allUpdaters );
    }
}
