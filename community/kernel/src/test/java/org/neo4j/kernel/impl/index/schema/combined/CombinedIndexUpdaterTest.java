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

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.values.storable.Value;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.index.schema.combined.CombinedIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.combined.CombinedIndexTestHelp.change;
import static org.neo4j.kernel.impl.index.schema.combined.CombinedIndexTestHelp.remove;

public class CombinedIndexUpdaterTest
{
    private IndexUpdater boostUpdater;
    private IndexUpdater fallbackUpdater;
    private CombinedIndexUpdater combinedIndexUpdater;

    @Before
    public void setup()
    {
        boostUpdater = mock( IndexUpdater.class );
        fallbackUpdater = mock( IndexUpdater.class );
        combinedIndexUpdater = new CombinedIndexUpdater( boostUpdater, fallbackUpdater );
    }

    /* remove */

    @Test
    public void removeMustRemoveFromBothBoostAndFallback() throws Exception
    {
        // when
        PrimitiveLongSet nodeIds = asPrimitiveSet( 1, 2, 3 );
        combinedIndexUpdater.remove( nodeIds );

        // then
        verify( boostUpdater, times( 1 ) ).remove( nodeIds );
        verify( fallbackUpdater, times( 1 ) ).remove( nodeIds );
    }

    private PrimitiveLongSet asPrimitiveSet( long... nodeIds )
    {
        PrimitiveLongSet set = Primitive.longSet();
        for ( long nodeId : nodeIds )
        {
            set.add( nodeId );
        }
        return set;
    }

    /* process */

    @Test
    public void processMustSelectCorrectForAdd() throws Exception
    {
        // given
        Value[] supportedByBoost = CombinedIndexTestHelp.valuesSupportedByBoost();
        Value[] notSupportedByBoost = CombinedIndexTestHelp.valuesNotSupportedByBoost();
        Value[] allValues = CombinedIndexTestHelp.allValues();

        // when
        // ... value supported by boost
        for ( Value value : supportedByBoost )
        {
            //then
            verifyAddWithCorrectUpdater( boostUpdater, fallbackUpdater, value );
        }

        // when
        // ... value not supported by boost
        for ( Value value : notSupportedByBoost )
        {
            verifyAddWithCorrectUpdater( fallbackUpdater, boostUpdater, value );
        }

        // when
        // ... value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectUpdater( fallbackUpdater, boostUpdater, firstValue, secondValue );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForRemove() throws Exception
    {
        // given
        Value[] supportedByBoost = CombinedIndexTestHelp.valuesSupportedByBoost();
        Value[] notSupportedByBoost = CombinedIndexTestHelp.valuesNotSupportedByBoost();
        Value[] allValues = CombinedIndexTestHelp.allValues();

        // when
        // ... value supported by boost
        for ( Value value : supportedByBoost )
        {
            //then
            verifyRemoveWithCorrectUpdater( boostUpdater, fallbackUpdater, value );
        }

        // when
        // ... value not supported by boost
        for ( Value value : notSupportedByBoost )
        {
            verifyRemoveWithCorrectUpdater( fallbackUpdater, boostUpdater, value );
        }

        // when
        // ... value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyRemoveWithCorrectUpdater( fallbackUpdater, boostUpdater, firstValue, secondValue );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForChangeSupportedByBoost() throws Exception
    {
        // given
        Value[] supportedByBoost = CombinedIndexTestHelp.valuesSupportedByBoost();

        // when
        // ... before - supported
        // ... after - supported
        for ( Value before : supportedByBoost )
        {
            for ( Value after : supportedByBoost )
            {
                verifyChangeWithCorrectUpdaterNotMixed( boostUpdater, fallbackUpdater, before, after );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForChangeNotSupportedByBoost() throws Exception
    {
        // given
        Value[] notSupportedByBoost = CombinedIndexTestHelp.valuesNotSupportedByBoost();

        // when
        // ... before - not supported
        // ... after - not supported
        for ( Value before : notSupportedByBoost )
        {
            for ( Value after : notSupportedByBoost )
            {
                verifyChangeWithCorrectUpdaterNotMixed( fallbackUpdater, boostUpdater, before, after );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForChangeFromBoostToFallback() throws Exception
    {
        // given
        Value[] supportedByBoost = CombinedIndexTestHelp.valuesSupportedByBoost();
        Value[] notSupportedByBoost = CombinedIndexTestHelp.valuesNotSupportedByBoost();

        // when
        // ... before - supported
        // ... after - not supported
        verifyChangeWithCorrectUpdaterMixed( boostUpdater, fallbackUpdater, supportedByBoost, notSupportedByBoost );
    }

    @Test
    public void processMustSelectCorrectForChangeFromFallbackToBoost() throws Exception
    {
        // given
        Value[] supportedByBoost = CombinedIndexTestHelp.valuesSupportedByBoost();
        Value[] notSupportedByBoost = CombinedIndexTestHelp.valuesNotSupportedByBoost();

        // when
        // ... before - not supported
        // ... after - supported
        verifyChangeWithCorrectUpdaterMixed( fallbackUpdater, boostUpdater, notSupportedByBoost, supportedByBoost );
    }

    private void verifyAddWithCorrectUpdater( IndexUpdater correctPopulator, IndexUpdater wrongPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = add( numberValues );
        combinedIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        verify( wrongPopulator, times( 0 ) ).process( update );
    }

    private void verifyRemoveWithCorrectUpdater( IndexUpdater correctPopulator, IndexUpdater wrongPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = CombinedIndexTestHelp.remove( numberValues );
        combinedIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        verify( wrongPopulator, times( 0 ) ).process( update );
    }

    private void verifyChangeWithCorrectUpdaterNotMixed( IndexUpdater correctPopulator, IndexUpdater wrongPopulator, Value before,
            Value after ) throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = CombinedIndexTestHelp.change( before, after );
        combinedIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        verify( wrongPopulator, times( 0 ) ).process( update );
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
                IndexEntryUpdate<LabelSchemaDescriptor> remove = remove( before );
                IndexEntryUpdate<LabelSchemaDescriptor> add = add( after );
                combinedIndexUpdater.process( change );
                verify( expectRemoveFrom, times( afterIndex + 1 ) ).process( remove );
                verify( expectAddTo, times( beforeIndex + 1 ) ).process( add );
            }
        }
    }

    /* close */

    @Test
    public void closeMustCloseBothBoostAndFallback() throws Exception
    {
        // when
        combinedIndexUpdater.close();

        // then
        verify( boostUpdater, times( 1 ) ).close();
        verify( fallbackUpdater, times( 1 ) ).close();
    }

    @Test
    public void closeMustThrowIfFallbackThrow() throws Exception
    {
        CombinedIndexTestHelp.verifyCombinedCloseThrowOnSingleCloseThrow( fallbackUpdater, combinedIndexUpdater );
    }

    @Test
    public void closeMustThrowIfBoostThrow() throws Exception
    {
        CombinedIndexTestHelp.verifyCombinedCloseThrowOnSingleCloseThrow( boostUpdater, combinedIndexUpdater );
    }

    @Test
    public void closeMustCloseBoostIfFallbackThrow() throws Exception
    {
        CombinedIndexTestHelp.verifyOtherIsClosedOnSingleThrow( fallbackUpdater, boostUpdater, combinedIndexUpdater );
    }

    @Test
    public void closeMustCloseFallbackIfBoostThrow() throws Exception
    {
        CombinedIndexTestHelp.verifyOtherIsClosedOnSingleThrow( boostUpdater, fallbackUpdater, combinedIndexUpdater );
    }

    @Test
    public void closeMustThrowIfBothThrow() throws Exception
    {
        CombinedIndexTestHelp.verifyCombinedCloseThrowIfBothThrow( boostUpdater, fallbackUpdater, combinedIndexUpdater );
    }
}
