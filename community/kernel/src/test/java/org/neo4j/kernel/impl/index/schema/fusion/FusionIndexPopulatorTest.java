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
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.index.schema.NativeSelector;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.DropAction;
import org.neo4j.values.storable.Value;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyCallFail;

public class FusionIndexPopulatorTest
{
    private IndexPopulator nativePopulator;
    private IndexPopulator spatialPopulator;
    private IndexPopulator lucenePopulator;
    private IndexPopulator[] allPopulators;
    private FusionIndexPopulator fusionIndexPopulator;
    private final long indexId = 8;
    private final DropAction dropAction = mock( DropAction.class );

    @Before
    public void mockComponents()
    {
        nativePopulator = mock( IndexPopulator.class );
        spatialPopulator = mock( IndexPopulator.class );
        lucenePopulator = mock( IndexPopulator.class );
        allPopulators = new IndexPopulator[]{nativePopulator, spatialPopulator, lucenePopulator};
        fusionIndexPopulator = new FusionIndexPopulator( nativePopulator, spatialPopulator, lucenePopulator, new NativeSelector(), indexId, dropAction );
    }

    /* create */

    @Test
    public void createMustCreateBothNativeAndLucene() throws Exception
    {
        // when
        fusionIndexPopulator.create();

        // then
        verify( nativePopulator, times( 1 ) ).create();
        verify( spatialPopulator, times( 1 ) ).create();
        verify( lucenePopulator, times( 1 ) ).create();
    }

    @Test
    public void createMustThrowIfCreateNativeThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( nativePopulator ).create();

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.create();
            return null;
        } );
    }

    @Test
    public void createMustThrowIfCreateSpatialThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( spatialPopulator ).create();

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.create();
            return null;
        } );
    }

    @Test
    public void createMustThrowIfCreateLuceneThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( lucenePopulator ).create();

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.create();
            return null;
        } );
    }

    /* drop */

    @Test
    public void dropMustDropBothNativeAndLucene() throws Exception
    {
        // when
        fusionIndexPopulator.drop();

        // then
        verify( nativePopulator, times( 1 ) ).drop();
        verify( spatialPopulator, times( 1 ) ).drop();
        verify( lucenePopulator, times( 1 ) ).drop();
        verify( dropAction ).drop( indexId );
    }

    @Test
    public void dropMustThrowIfDropNativeThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( nativePopulator ).drop();

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.drop();
            return null;
        } );
    }

    @Test
    public void dropMustThrowIfDropSpatialThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( spatialPopulator ).drop();

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.drop();
            return null;
        } );
    }

    @Test
    public void dropMustThrowIfDropLuceneThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( lucenePopulator ).drop();

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.drop();
            return null;
        } );
    }

    /* add */

    @Test
    public void addMustSelectCorrectPopulator() throws Exception
    {
        // given
        Value[] numberValues = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] spatialValues = FusionIndexTestHelp.valuesSupportedBySpatial();
        Value[] otherValues = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();
        Value[] allValues = FusionIndexTestHelp.allValues();

        // Add with native for number values
        for ( Value numberValue : numberValues )
        {
            verifyAddWithCorrectPopulator( nativePopulator, numberValue );
        }

        // Add with spatial for geometric values
        for ( Value spatialValue : spatialValues )
        {
            verifyAddWithCorrectPopulator( spatialPopulator, spatialValue );
        }

        // Add with lucene for other values
        for ( Value otherValue : otherValues )
        {
            verifyAddWithCorrectPopulator( lucenePopulator, otherValue );
        }

        // All composite values should go to lucene
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectPopulator( lucenePopulator, firstValue, secondValue );
            }
        }
    }

    private void verifyAddWithCorrectPopulator( IndexPopulator correctPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        Collection<IndexEntryUpdate<LabelSchemaDescriptor>> update = asList( add( numberValues ) );
        fusionIndexPopulator.add( update );
        verify( correctPopulator, times( 1 ) ).add( update );
        for ( IndexPopulator populator : allPopulators )
        {
            if ( populator != correctPopulator )
            {
                verify( populator, times( 0 ) ).add( update );
            }
        }
    }

    /* verifyDeferredConstraints */

    @Test
    public void verifyDeferredConstraintsMustThrowIfNativeThrow() throws Exception
    {
        // given
        IndexEntryConflictException failure = mock( IndexEntryConflictException.class );
        doThrow( failure ).when( nativePopulator ).verifyDeferredConstraints( any() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.verifyDeferredConstraints( null );
            return null;
        } );
    }

    @Test
    public void verifyDeferredConstraintsMustThrowIfSpatialThrow() throws Exception
    {
        // given
        IndexEntryConflictException failure = mock( IndexEntryConflictException.class );
        doThrow( failure ).when( spatialPopulator ).verifyDeferredConstraints( any() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.verifyDeferredConstraints( null );
            return null;
        } );
    }

    @Test
    public void verifyDeferredConstraintsMustThrowIfLuceneThrow() throws Exception
    {
        // given
        IndexEntryConflictException failure = mock( IndexEntryConflictException.class );
        doThrow( failure ).when( lucenePopulator ).verifyDeferredConstraints( any() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.verifyDeferredConstraints( null );
            return null;
        } );
    }

    /* close */

    @Test
    public void successfulCloseMustCloseBothNativeAndLucene() throws Exception
    {
        // when
        closeAndVerifyPropagation( true );
    }

    @Test
    public void unsuccessfulCloseMustCloseBothNativeAndLucene() throws Exception
    {
        // when
        closeAndVerifyPropagation( false );
    }

    private void closeAndVerifyPropagation( boolean populationCompletedSuccessfully ) throws IOException
    {
        fusionIndexPopulator.close( populationCompletedSuccessfully );

        // then
        verify( nativePopulator, times( 1 ) ).close( populationCompletedSuccessfully );
        verify( spatialPopulator, times( 1 ) ).close( populationCompletedSuccessfully );
        verify( lucenePopulator, times( 1 ) ).close( populationCompletedSuccessfully );
    }

    @Test
    public void closeMustThrowIfCloseNativeThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( nativePopulator ).close( anyBoolean() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.close( anyBoolean() );
            return null;
        } );
    }

    @Test
    public void closeMustThrowIfCloseSpatialThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( spatialPopulator ).close( anyBoolean() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.close( anyBoolean() );
            return null;
        } );
    }

    @Test
    public void closeMustThrowIfCloseLuceneThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( lucenePopulator ).close( anyBoolean() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.close( anyBoolean() );
            return null;
        } );
    }

    @Test
    public void closeMustCloseOthersIfLuceneThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( lucenePopulator ).close( anyBoolean() );

        // when
        try
        {
            fusionIndexPopulator.close( true );
            fail( "Should have failed" );
        }
        catch ( IOException ignore )
        {
        }

        // then
        verify( nativePopulator, times( 1 ) ).close( true );
        verify( spatialPopulator, times( 1 ) ).close( true );
    }

    @Test
    public void closeMustCloseOthersIfSpatialThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( spatialPopulator ).close( anyBoolean() );

        // when
        try
        {
            fusionIndexPopulator.close( true );
            fail( "Should have failed" );
        }
        catch ( IOException ignore )
        {
        }

        // then
        verify( lucenePopulator, times( 1 ) ).close( true );
        verify( nativePopulator, times( 1 ) ).close( true );
    }

    @Test
    public void closeMustCloseOthersIfNativeThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( nativePopulator ).close( anyBoolean() );

        // when
        try
        {
            fusionIndexPopulator.close( true );
            fail( "Should have failed" );
        }
        catch ( IOException ignore )
        {
        }

        // then
        verify( lucenePopulator, times( 1 ) ).close( true );
        verify( spatialPopulator, times( 1 ) ).close( true );
    }

    @Test
    public void closeMustThrowIfAllThrow() throws Exception
    {
        // given
        IOException nativeFailure = new IOException( "native" );
        IOException spatialFailure = new IOException( "spatial" );
        IOException luceneFailure = new IOException( "lucene" );
        doThrow( nativeFailure ).when( nativePopulator ).close( anyBoolean() );
        doThrow( spatialFailure ).when( spatialPopulator ).close( anyBoolean() );
        doThrow( luceneFailure ).when( lucenePopulator).close( anyBoolean() );

        try
        {
            // when
            fusionIndexPopulator.close( anyBoolean() );
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            // then
            assertThat( e, anyOf( sameInstance( nativeFailure ), sameInstance( spatialFailure ), sameInstance( luceneFailure ) ) );
        }
    }

    /* markAsFailed */

    @Test
    public void markAsFailedMustMarkAll() throws Exception
    {
        // when
        String failureMessage = "failure";
        fusionIndexPopulator.markAsFailed( failureMessage );

        // then
        verify( nativePopulator, times( 1 ) ).markAsFailed( failureMessage );
        verify( spatialPopulator, times( 1 ) ).markAsFailed( failureMessage );
        verify( lucenePopulator, times( 1 ) ).markAsFailed( failureMessage );
    }

    @Test
    public void markAsFailedMustThrowIfNativeThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( nativePopulator ).markAsFailed( anyString() );

        // then
        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.markAsFailed( anyString() );
            return null;
        } );
    }

    @Test
    public void markAsFailedMustThrowIfSpatialThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( spatialPopulator ).markAsFailed( anyString() );

        // then
        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.markAsFailed( anyString() );
            return null;
        } );
    }

    @Test
    public void markAsFailedMustThrowIfLuceneThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( lucenePopulator ).markAsFailed( anyString() );

        // then
        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.markAsFailed( anyString() );
            return null;
        } );
    }

    @Test
    public void shouldIncludeSampleOnCorrectPopulator()
    {
        // given
        Value[] numberValues = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] spatialValues = FusionIndexTestHelp.valuesSupportedBySpatial();
        Value[] otherValues = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();

        for ( Value value : numberValues )
        {
            // when
            IndexEntryUpdate<LabelSchemaDescriptor> update = add( value );
            fusionIndexPopulator.includeSample( update );

            // then
            verify( nativePopulator ).includeSample( update );
            reset( nativePopulator );
        }

        for ( Value value : spatialValues )
        {
            // when
            IndexEntryUpdate<LabelSchemaDescriptor> update = add( value );
            fusionIndexPopulator.includeSample( update );

            // then
            verify( spatialPopulator ).includeSample( update );
            reset( spatialPopulator );
        }

        for ( Value value : otherValues )
        {
            // when
            IndexEntryUpdate<LabelSchemaDescriptor> update = add( value );
            fusionIndexPopulator.includeSample( update );

            // then
            verify( lucenePopulator ).includeSample( update );
            reset( lucenePopulator );
        }
    }
}
