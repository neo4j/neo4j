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
import org.neo4j.values.storable.Value;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.change;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.remove;

public class FusionIndexUpdaterTest
{
    private IndexUpdater nativeUpdater;
    private IndexUpdater spatialUpdater;
    private IndexUpdater temporalUpdater;
    private IndexUpdater luceneUpdater;
    private IndexUpdater[] allUpdaters;
    private FusionIndexUpdater fusionIndexUpdater;

    @BeforeEach
    public void setup()
    {
        nativeUpdater = mock( IndexUpdater.class );
        spatialUpdater = mock( IndexUpdater.class );
        temporalUpdater = mock( IndexUpdater.class );
        luceneUpdater = mock( IndexUpdater.class );
        allUpdaters = new IndexUpdater[]{nativeUpdater, spatialUpdater, temporalUpdater, luceneUpdater};
        fusionIndexUpdater = new FusionIndexUpdater( nativeUpdater, spatialUpdater, temporalUpdater, luceneUpdater, new FusionSelector() );
    }

    /* process */

    @Test
    public void processMustSelectCorrectForAdd() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] supportedBySpatial = FusionIndexTestHelp.valuesSupportedBySpatial();
        Value[] supportedByTemporal = FusionIndexTestHelp.valuesSupportedByTemporal();
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
        // ... value supported by temporal
        for ( Value value : supportedByTemporal )
        {
            //then
            verifyAddWithCorrectUpdater( temporalUpdater, value );
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
    public void processMustSelectCorrectForRemove() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] supportedBySpatial = FusionIndexTestHelp.valuesSupportedBySpatial();
        Value[] supportedByTemporal = FusionIndexTestHelp.valuesSupportedByTemporal();
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
        // ... value supported by temporal
        for ( Value value : supportedByTemporal )
        {
            //then
            verifyRemoveWithCorrectUpdater( temporalUpdater, value );
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
    public void processMustSelectCorrectForChangeSupportedByNative() throws Exception
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
    public void processMustSelectCorrectForChangeSupportedBySpatial() throws Exception
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
    public void processMustSelectCorrectForChangeSupportedByTemporal() throws Exception
    {
        // given
        Value[] supportedByTemporal = FusionIndexTestHelp.valuesSupportedByTemporal();

        // when
        // ... before - supported
        // ... after - supported
        for ( Value before : supportedByTemporal )
        {
            for ( Value after : supportedByTemporal )
            {
                verifyChangeWithCorrectUpdaterNotMixed( temporalUpdater, before, after );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForChangeNotSupportedByNative() throws Exception
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
    public void processMustSelectCorrectForChangeFromNativeToLucene() throws Exception
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
    public void processMustSelectCorrectForChangeFromLuceneToNative() throws Exception
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
    public void processMustSelectCorrectForChangeFromNativeToSpatialOrBack() throws Exception
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
    public void processMustSelectCorrectForChangeFromNativeToTemporalOrBack() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] supportedByTemporal = FusionIndexTestHelp.valuesSupportedByTemporal();

        // when
        // ... before - supported
        // ... after - not supported
        verifyChangeWithCorrectUpdaterMixed( nativeUpdater, temporalUpdater, supportedByNative, supportedByTemporal );
        verifyChangeWithCorrectUpdaterMixed( temporalUpdater, nativeUpdater, supportedByTemporal, supportedByNative );
    }

    @Test
    public void processMustSelectCorrectForChangeFromSpatialToTemporalOrBack() throws Exception
    {
        // given
        Value[] supportedBySpatial = FusionIndexTestHelp.valuesSupportedBySpatial();
        Value[] supportedByTemporal = FusionIndexTestHelp.valuesSupportedByTemporal();

        // when
        // ... before - supported
        // ... after - not supported
        verifyChangeWithCorrectUpdaterMixed( spatialUpdater, temporalUpdater, supportedBySpatial, supportedByTemporal );
        verifyChangeWithCorrectUpdaterMixed( temporalUpdater, spatialUpdater, supportedByTemporal, supportedBySpatial );
    }

    @Test
    public void processMustSelectCorrectForChangeFromLuceneToSpatialOrBack() throws Exception
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

    @Test
    public void processMustSelectCorrectForChangeFromLuceneToTemporalOrBack() throws Exception
    {
        // given
        Value[] supportedByLucene = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();
        Value[] supportedByTemporal = FusionIndexTestHelp.valuesSupportedByTemporal();

        // when
        // ... before - supported
        // ... after - not supported
        verifyChangeWithCorrectUpdaterMixed( luceneUpdater, temporalUpdater, supportedByLucene, supportedByTemporal );
        reset( luceneUpdater, temporalUpdater );
        verifyChangeWithCorrectUpdaterMixed( temporalUpdater, luceneUpdater, supportedByTemporal, supportedByLucene );
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
    public void closeMustThrowIfLuceneThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( luceneUpdater, fusionIndexUpdater );
    }

    @Test
    public void closeMustThrowIfNativeThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( nativeUpdater, fusionIndexUpdater );
    }

    @Test
    public void closeMustThrowIfSpatialThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( spatialUpdater, fusionIndexUpdater );
    }

    @Test
    public void closeMustThrowIfTemporalThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( temporalUpdater, fusionIndexUpdater );
    }

    @Test
    public void closeMustCloseOthersIfLuceneThrow() throws Exception
    {
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( luceneUpdater, fusionIndexUpdater, nativeUpdater, spatialUpdater, temporalUpdater );
    }

    @Test
    public void closeMustCloseOthersIfSpatialThrow() throws Exception
    {
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( spatialUpdater, fusionIndexUpdater, nativeUpdater, temporalUpdater, luceneUpdater );
    }

    @Test
    public void closeMustCloseOthersIfTemporalThrow() throws Exception
    {
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( temporalUpdater, fusionIndexUpdater, nativeUpdater, spatialUpdater, luceneUpdater );
    }

    @Test
    public void closeMustCloseOthersIfNativeThrow() throws Exception
    {
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( nativeUpdater, fusionIndexUpdater, spatialUpdater, temporalUpdater, luceneUpdater );
    }

    @Test
    public void closeMustThrowIfAllThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow( fusionIndexUpdater, nativeUpdater, spatialUpdater, temporalUpdater, luceneUpdater );
    }
}
