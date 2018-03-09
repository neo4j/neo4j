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
import java.util.Collections;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.DropAction;
import org.neo4j.values.storable.Value;

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
    private IndexPopulator numberPopulator;
    private IndexPopulator spatialPopulator;
    private IndexPopulator temporalPopulator;
    private IndexPopulator lucenePopulator;
    private IndexPopulator[] allPopulators;
    private FusionIndexPopulator fusionIndexPopulator;
    private final long indexId = 8;
    private final DropAction dropAction = mock( DropAction.class );

    @Before
    public void mockComponents()
    {
        numberPopulator = mock( IndexPopulator.class );
        spatialPopulator = mock( IndexPopulator.class );
        temporalPopulator = mock( IndexPopulator.class );
        lucenePopulator = mock( IndexPopulator.class );
        allPopulators = new IndexPopulator[]{numberPopulator, spatialPopulator, temporalPopulator, lucenePopulator};
        fusionIndexPopulator =
                new FusionIndexPopulator( numberPopulator, spatialPopulator, temporalPopulator, lucenePopulator, new FusionSelector(), indexId, dropAction );
    }

    /* create */

    @Test
    public void createMustCreateBothNativeAndLucene() throws Exception
    {
        // when
        fusionIndexPopulator.create();

        // then
        for ( IndexPopulator populator : allPopulators )
        {
            verify( populator, times( 1 ) ).create();
        }
    }

    @Test
    public void createMustThrowIfCreateNativeThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( numberPopulator ).create();

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
    public void createMustThrowIfCreateTemporalThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( temporalPopulator ).create();

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
    public void dropMustDropAll() throws Exception
    {
        // when
        fusionIndexPopulator.drop();

        // then
        for ( IndexPopulator populator : allPopulators )
        {
            verify( populator, times( 1 ) ).drop();
        }
        verify( dropAction ).drop( indexId );
    }

    @Test
    public void dropMustThrowIfDropNativeThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( numberPopulator ).drop();

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
    public void dropMustThrowIfDropTemporalThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( temporalPopulator ).drop();

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
        Value[] temporalValues = FusionIndexTestHelp.valuesSupportedByTemporal();
        Value[] otherValues = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();
        Value[] allValues = FusionIndexTestHelp.allValues();

        // Add with native for number values
        for ( Value numberValue : numberValues )
        {
            verifyAddWithCorrectPopulator( numberPopulator, numberValue );
        }

        // Add with spatial for geometric values
        for ( Value spatialValue : spatialValues )
        {
            verifyAddWithCorrectPopulator( spatialPopulator, spatialValue );
        }

        // Add with temporal for temporal values
        for ( Value temporalValue : temporalValues )
        {
            verifyAddWithCorrectPopulator( temporalPopulator, temporalValue );
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
        Collection<IndexEntryUpdate<LabelSchemaDescriptor>> update = Collections.singletonList( add( numberValues ) );
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
        doThrow( failure ).when( numberPopulator ).verifyDeferredConstraints( any() );

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
    public void verifyDeferredConstraintsMustThrowIfTemporalThrow() throws Exception
    {
        // given
        IndexEntryConflictException failure = mock( IndexEntryConflictException.class );
        doThrow( failure ).when( temporalPopulator ).verifyDeferredConstraints( any() );

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
    public void successfulCloseMustCloseAll() throws Exception
    {
        // when
        closeAndVerifyPropagation( true );
    }

    @Test
    public void unsuccessfulCloseMustCloseAll() throws Exception
    {
        // when
        closeAndVerifyPropagation( false );
    }

    private void closeAndVerifyPropagation( boolean populationCompletedSuccessfully ) throws IOException
    {
        fusionIndexPopulator.close( populationCompletedSuccessfully );

        // then
        for ( IndexPopulator populator : allPopulators )
        {
            verify( populator, times( 1 ) ).close( populationCompletedSuccessfully );
        }
    }

    @Test
    public void closeMustThrowIfCloseNativeThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( numberPopulator ).close( anyBoolean() );

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
    public void closeMustThrowIfCloseTemporalThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( temporalPopulator ).close( anyBoolean() );

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

    private static void verifyOtherCloseOnThrow( IndexPopulator throwingPopulator, FusionIndexPopulator fusionPopulator, IndexPopulator... populators )
            throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( throwingPopulator ).close( anyBoolean() );

        // when
        try
        {
            fusionPopulator.close( true );
            fail( "Should have failed" );
        }
        catch ( IOException ignore )
        {
        }

        // then
        for ( IndexPopulator populator : populators )
        {
            verify( populator, times( 1 ) ).close( true );
        }
    }

    @Test
    public void closeMustCloseOthersIfLuceneThrow() throws Exception
    {
        verifyOtherCloseOnThrow( lucenePopulator, fusionIndexPopulator, numberPopulator, spatialPopulator, temporalPopulator );
    }

    @Test
    public void closeMustCloseOthersIfSpatialThrow() throws Exception
    {
        verifyOtherCloseOnThrow( spatialPopulator, fusionIndexPopulator, numberPopulator, temporalPopulator, lucenePopulator );
    }

    @Test
    public void closeMustCloseOthersIfTemporalThrow() throws Exception
    {
        verifyOtherCloseOnThrow( temporalPopulator, fusionIndexPopulator, numberPopulator, spatialPopulator, lucenePopulator );
    }

    @Test
    public void closeMustCloseOthersIfNumberThrow() throws Exception
    {
        verifyOtherCloseOnThrow( numberPopulator, fusionIndexPopulator, spatialPopulator, temporalPopulator, lucenePopulator );
    }

    @Test
    public void closeMustThrowIfAllThrow() throws Exception
    {
        // given
        IOException nativeFailure = new IOException( "native" );
        IOException spatialFailure = new IOException( "spatial" );
        IOException temporalFailure = new IOException( "temporal" );
        IOException luceneFailure = new IOException( "lucene" );
        doThrow( nativeFailure ).when( numberPopulator ).close( anyBoolean() );
        doThrow( spatialFailure ).when( spatialPopulator ).close( anyBoolean() );
        doThrow( temporalFailure ).when( temporalPopulator ).close( anyBoolean() );
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
            assertThat( e, anyOf(
                    sameInstance( nativeFailure ),
                    sameInstance( spatialFailure ),
                    sameInstance( temporalFailure ),
                    sameInstance( luceneFailure ) ) );
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
        for ( IndexPopulator populator : allPopulators )
        {
            verify( populator, times( 1 ) ).markAsFailed( failureMessage );
        }
    }

    @Test
    public void markAsFailedMustThrowIfNativeThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( numberPopulator ).markAsFailed( anyString() );

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
    public void markAsFailedMustThrowIfTemporalThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( temporalPopulator ).markAsFailed( anyString() );

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
        Value[] temporalValues = FusionIndexTestHelp.valuesSupportedByTemporal();
        Value[] otherValues = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();

        verifySampleToCorrectPopulator( numberValues, numberPopulator );
        verifySampleToCorrectPopulator( spatialValues, spatialPopulator );
        verifySampleToCorrectPopulator( temporalValues, temporalPopulator );
        verifySampleToCorrectPopulator( otherValues, lucenePopulator );
    }

    private void verifySampleToCorrectPopulator( Value[] values, IndexPopulator populator )
    {
        for ( Value value : values )
        {
            // when
            IndexEntryUpdate<LabelSchemaDescriptor> update = add( value );
            fusionIndexPopulator.includeSample( update );

            // then
            verify( populator ).includeSample( update );
            reset( populator );
        }
    }
}
