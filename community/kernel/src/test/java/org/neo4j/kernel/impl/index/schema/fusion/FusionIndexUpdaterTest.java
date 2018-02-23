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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.index.schema.NativeSelector;
import org.neo4j.values.storable.Value;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.change;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.remove;

class FusionIndexUpdaterTest
{
    private IndexUpdater nativeUpdater;
    private IndexUpdater spatialUpdater;
    private IndexUpdater luceneUpdater;
    private IndexUpdater[] allUpdaters;
    private FusionIndexUpdater fusionIndexUpdater;

    @BeforeEach
    void setup()
    {
        nativeUpdater = mock( IndexUpdater.class );
        spatialUpdater = mock( IndexUpdater.class );
        luceneUpdater = mock( IndexUpdater.class );
        allUpdaters = new IndexUpdater[]{nativeUpdater, spatialUpdater, luceneUpdater};
        fusionIndexUpdater = new FusionIndexUpdater( nativeUpdater, spatialUpdater, luceneUpdater, new NativeSelector() );
    }

    /* process */

    @Test
    void processMustSelectCorrectForAdd() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] supportedBySpatial = FusionIndexTestHelp.valuesSupportedBySpatial();
        Value[] notSupportedByNativeOrSpatial = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();
        Value[] allValues = FusionIndexTestHelp.allValues();

        // when
        // ... value supported by native
        for ( Value value : supportedByNative )
        {
            //then
            verifyAddWithCorrectUpdater( nativeUpdater, value );
        }

        // when
        // ... value supported by spatial
        for ( Value value : supportedBySpatial )
        {
            //then
            verifyAddWithCorrectUpdater( spatialUpdater, value );
        }

        // when
        // ... value not supported by native
        for ( Value value : notSupportedByNativeOrSpatial )
        {
            verifyAddWithCorrectUpdater( luceneUpdater, value );
        }

        // when
        // ... value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectUpdater( luceneUpdater, firstValue, secondValue );
            }
        }
    }

    @Test
    void processMustSelectCorrectForRemove() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] supportedBySpatial = FusionIndexTestHelp.valuesSupportedBySpatial();
        Value[] notSupportedByNativeOrSpatial = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();
        Value[] allValues = FusionIndexTestHelp.allValues();

        // when
        // ... value supported by native
        for ( Value value : supportedByNative )
        {
            //then
            verifyRemoveWithCorrectUpdater( nativeUpdater, value );
        }

        // when
        // ... value supported by spatial
        for ( Value value : supportedBySpatial )
        {
            //then
            verifyRemoveWithCorrectUpdater( spatialUpdater, value );
        }

        // when
        // ... value not supported by native
        for ( Value value : notSupportedByNativeOrSpatial )
        {
            verifyRemoveWithCorrectUpdater( luceneUpdater, value );
        }

        // when
        // ... value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyRemoveWithCorrectUpdater( luceneUpdater, firstValue, secondValue );
            }
        }
    }

    @Test
    void processMustSelectCorrectForChangeSupportedByNative() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();

        // when
        // ... before - supported
        // ... after - supported
        for ( Value before : supportedByNative )
        {
            for ( Value after : supportedByNative )
            {
                verifyChangeWithCorrectUpdaterNotMixed( nativeUpdater, before, after );
            }
        }
    }

    @Test
    void processMustSelectCorrectForChangeSupportedBySpatial() throws Exception
    {
        // given
        Value[] supportedBySpatial = FusionIndexTestHelp.valuesSupportedBySpatial();

        // when
        // ... before - supported
        // ... after - supported
        for ( Value before : supportedBySpatial )
        {
            for ( Value after : supportedBySpatial )
            {
                verifyChangeWithCorrectUpdaterNotMixed( spatialUpdater, before, after );
            }
        }
    }

    @Test
    void processMustSelectCorrectForChangeNotSupportedByNative() throws Exception
    {
        // given
        Value[] notSupportedByNativeOrSpatial = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();

        // when
        // ... before - not supported
        // ... after - not supported
        for ( Value before : notSupportedByNativeOrSpatial )
        {
            for ( Value after : notSupportedByNativeOrSpatial )
            {
                verifyChangeWithCorrectUpdaterNotMixed( luceneUpdater, before, after );
            }
        }
    }

    @Test
    void processMustSelectCorrectForChangeFromNativeToLucene() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] notSupportedByNativeOrSpatial = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();

        // when
        // ... before - supported
        // ... after - not supported
        verifyChangeWithCorrectUpdaterMixed( nativeUpdater, luceneUpdater, supportedByNative, notSupportedByNativeOrSpatial );
    }

    @Test
    void processMustSelectCorrectForChangeFromLuceneToNative() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] notSupportedByNativeOrSpatial = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();

        // when
        // ... before - not supported
        // ... after - supported
        verifyChangeWithCorrectUpdaterMixed( luceneUpdater, nativeUpdater, notSupportedByNativeOrSpatial, supportedByNative );
    }

    @Test
    void processMustSelectCorrectForChangeFromNativeToSpatialOrBack() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] supportedBySpatial = FusionIndexTestHelp.valuesSupportedBySpatial();

        // when
        // ... before - supported
        // ... after - not supported
        verifyChangeWithCorrectUpdaterMixed( nativeUpdater, spatialUpdater, supportedByNative, supportedBySpatial );
        verifyChangeWithCorrectUpdaterMixed( spatialUpdater, nativeUpdater, supportedBySpatial, supportedByNative );
    }

    @Test
    void processMustSelectCorrectForChangeFromLuceneToSpatialOrBack() throws Exception
    {
        // given
        Value[] supportedByLucene = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();
        Value[] supportedBySpatial = FusionIndexTestHelp.valuesSupportedBySpatial();

        // when
        // ... before - supported
        // ... after - not supported
        verifyChangeWithCorrectUpdaterMixed( luceneUpdater, spatialUpdater, supportedByLucene, supportedBySpatial );
        reset( luceneUpdater, spatialUpdater );
        verifyChangeWithCorrectUpdaterMixed( spatialUpdater, luceneUpdater, supportedBySpatial, supportedByLucene );
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
                fusionIndexUpdater.process( change );
                verify( expectRemoveFrom, times( afterIndex + 1 ) ).process( remove );
                verify( expectAddTo, times( beforeIndex + 1 ) ).process( add );
            }
        }
    }

    /* close */

    @Test
    void closeMustCloseBothNativeAndLucene() throws Exception
    {
        // when
        fusionIndexUpdater.close();

        // then
        verify( nativeUpdater, times( 1 ) ).close();
        verify( spatialUpdater, times( 1 ) ).close();
        verify( luceneUpdater, times( 1 ) ).close();
    }

    @Test
    void closeMustThrowIfLuceneThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( luceneUpdater, fusionIndexUpdater );
    }

    @Test
    void closeMustThrowIfNativeThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( nativeUpdater, fusionIndexUpdater );
    }

    @Test
    void closeMustThrowIfSpatialThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( spatialUpdater, fusionIndexUpdater );
    }

    @Test
    void closeMustCloseNativeIfLuceneThrow() throws Exception
    {
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( luceneUpdater, fusionIndexUpdater, nativeUpdater, spatialUpdater );
    }

    @Test
    void closeMustCloseLuceneIfSpatialThrow() throws Exception
    {
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( spatialUpdater, fusionIndexUpdater, luceneUpdater, nativeUpdater );
    }

    @Test
    void closeMustCloseLuceneIfNativeThrow() throws Exception
    {
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( nativeUpdater, fusionIndexUpdater, luceneUpdater, spatialUpdater );
    }

    @Test
    void closeMustThrowIfBothThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow( fusionIndexUpdater, nativeUpdater, luceneUpdater );
    }
}
