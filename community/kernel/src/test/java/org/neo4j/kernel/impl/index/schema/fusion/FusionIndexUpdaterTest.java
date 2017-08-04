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
import org.junit.Test;

import java.io.IOException;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.impl.index.schema.NativeSelector;
import org.neo4j.values.storable.Value;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.change;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.remove;

public class FusionIndexUpdaterTest
{
    private IndexUpdater nativeUpdater;
    private IndexUpdater luceneUpdater;
    private FusionIndexUpdater fusionIndexUpdater;

    @Before
    public void setup()
    {
        nativeUpdater = mock( IndexUpdater.class );
        luceneUpdater = mock( IndexUpdater.class );
        fusionIndexUpdater = new FusionIndexUpdater( nativeUpdater, luceneUpdater, new NativeSelector() );
    }

    /* remove */

    @Test
    public void removeMustRemoveFromBothNativeAndLucene() throws Exception
    {
        // when
        PrimitiveLongSet nodeIds = asPrimitiveSet( 1, 2, 3 );
        fusionIndexUpdater.remove( nodeIds );

        // then
        verify( nativeUpdater, times( 1 ) ).remove( nodeIds );
        verify( luceneUpdater, times( 1 ) ).remove( nodeIds );
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
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] notSupportedByNative = FusionIndexTestHelp.valuesNotSupportedByNative();
        Value[] allValues = FusionIndexTestHelp.allValues();

        // when
        // ... value supported by native
        for ( Value value : supportedByNative )
        {
            //then
            verifyAddWithCorrectUpdater( nativeUpdater, luceneUpdater, value );
        }

        // when
        // ... value not supported by native
        for ( Value value : notSupportedByNative )
        {
            verifyAddWithCorrectUpdater( luceneUpdater, nativeUpdater, value );
        }

        // when
        // ... value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectUpdater( luceneUpdater, nativeUpdater, firstValue, secondValue );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForRemove() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] notSupportedByNative = FusionIndexTestHelp.valuesNotSupportedByNative();
        Value[] allValues = FusionIndexTestHelp.allValues();

        // when
        // ... value supported by native
        for ( Value value : supportedByNative )
        {
            //then
            verifyRemoveWithCorrectUpdater( nativeUpdater, luceneUpdater, value );
        }

        // when
        // ... value not supported by native
        for ( Value value : notSupportedByNative )
        {
            verifyRemoveWithCorrectUpdater( luceneUpdater, nativeUpdater, value );
        }

        // when
        // ... value is composite
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyRemoveWithCorrectUpdater( luceneUpdater, nativeUpdater, firstValue, secondValue );
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
                verifyChangeWithCorrectUpdaterNotMixed( nativeUpdater, luceneUpdater, before, after );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForChangeNotSupportedByNative() throws Exception
    {
        // given
        Value[] notSupportedByNative = FusionIndexTestHelp.valuesNotSupportedByNative();

        // when
        // ... before - not supported
        // ... after - not supported
        for ( Value before : notSupportedByNative )
        {
            for ( Value after : notSupportedByNative )
            {
                verifyChangeWithCorrectUpdaterNotMixed( luceneUpdater, nativeUpdater, before, after );
            }
        }
    }

    @Test
    public void processMustSelectCorrectForChangeFromNativeToLucene() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] notSupportedByNative = FusionIndexTestHelp.valuesNotSupportedByNative();

        // when
        // ... before - supported
        // ... after - not supported
        verifyChangeWithCorrectUpdaterMixed( nativeUpdater, luceneUpdater, supportedByNative, notSupportedByNative );
    }

    @Test
    public void processMustSelectCorrectForChangeFromLuceneToNative() throws Exception
    {
        // given
        Value[] supportedByNative = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] notSupportedByNative = FusionIndexTestHelp.valuesNotSupportedByNative();

        // when
        // ... before - not supported
        // ... after - supported
        verifyChangeWithCorrectUpdaterMixed( luceneUpdater, nativeUpdater, notSupportedByNative, supportedByNative );
    }

    private void verifyAddWithCorrectUpdater( IndexUpdater correctPopulator, IndexUpdater wrongPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = add( numberValues );
        fusionIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        verify( wrongPopulator, times( 0 ) ).process( update );
    }

    private void verifyRemoveWithCorrectUpdater( IndexUpdater correctPopulator, IndexUpdater wrongPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = FusionIndexTestHelp.remove( numberValues );
        fusionIndexUpdater.process( update );
        verify( correctPopulator, times( 1 ) ).process( update );
        verify( wrongPopulator, times( 0 ) ).process( update );
    }

    private void verifyChangeWithCorrectUpdaterNotMixed( IndexUpdater correctPopulator, IndexUpdater wrongPopulator, Value before,
            Value after ) throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = FusionIndexTestHelp.change( before, after );
        fusionIndexUpdater.process( update );
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
                fusionIndexUpdater.process( change );
                verify( expectRemoveFrom, times( afterIndex + 1 ) ).process( remove );
                verify( expectAddTo, times( beforeIndex + 1 ) ).process( add );
            }
        }
    }

    /* close */

    @Test
    public void closeMustCloseBothNativeAndLucene() throws Exception
    {
        // when
        fusionIndexUpdater.close();

        // then
        verify( nativeUpdater, times( 1 ) ).close();
        verify( luceneUpdater, times( 1 ) ).close();
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
    public void closeMustCloseNativeIfLuceneThrow() throws Exception
    {
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( luceneUpdater, nativeUpdater, fusionIndexUpdater );
    }

    @Test
    public void closeMustCloseLuceneIfNativeThrow() throws Exception
    {
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( nativeUpdater, luceneUpdater, fusionIndexUpdater );
    }

    @Test
    public void closeMustThrowIfBothThrow() throws Exception
    {
        FusionIndexTestHelp.verifyFusionCloseThrowIfBothThrow( nativeUpdater, luceneUpdater, fusionIndexUpdater );
    }
}
