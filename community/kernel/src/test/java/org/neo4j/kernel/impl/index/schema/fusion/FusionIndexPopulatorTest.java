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
import java.util.Collection;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.impl.index.schema.NativeSelector;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider.DropAction;
import org.neo4j.values.storable.Value;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
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
    private IndexPopulator lucenePopulator;
    private FusionIndexPopulator fusionIndexPopulator;
    private final long indexId = 8;
    private final DropAction dropAction = mock( DropAction.class );

    @Before
    public void mockComponents()
    {
        nativePopulator = mock( IndexPopulator.class );
        lucenePopulator = mock( IndexPopulator.class );
        fusionIndexPopulator = new FusionIndexPopulator( nativePopulator, lucenePopulator, new NativeSelector(), indexId, dropAction );
    }

    /* create */

    @Test
    public void createMustCreateBothNativeAndLucene() throws Exception
    {
        // when
        fusionIndexPopulator.create();

        // then
        verify( nativePopulator, times( 1 ) ).create();
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
        Value[] otherValues = FusionIndexTestHelp.valuesNotSupportedByNative();
        Value[] allValues = FusionIndexTestHelp.allValues();

        // Add with native for number values
        for ( Value numberValue : numberValues )
        {
            verifyAddWithCorrectPopulator( nativePopulator, lucenePopulator, numberValue );
        }

        // Add with lucene for other values
        for ( Value otherValue : otherValues )
        {
            verifyAddWithCorrectPopulator( lucenePopulator, nativePopulator, otherValue );
        }

        // All composite values should go to lucene
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectPopulator( lucenePopulator, nativePopulator, firstValue, secondValue );
            }
        }
    }

    private void verifyAddWithCorrectPopulator( IndexPopulator correctPopulator, IndexPopulator wrongPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        Collection<IndexEntryUpdate<LabelSchemaDescriptor>> update = asList( add( numberValues ) );
        fusionIndexPopulator.add( update );
        verify( correctPopulator, times( 1 ) ).add( update );
        verify( wrongPopulator, times( 0 ) ).add( update );
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
        closeAndVerifyPropagation( nativePopulator, lucenePopulator, fusionIndexPopulator, true );
    }

    @Test
    public void unsuccessfulCloseMustCloseBothNativeAndLucene() throws Exception
    {
        // when
        closeAndVerifyPropagation( nativePopulator, lucenePopulator, fusionIndexPopulator, false );
    }

    private void closeAndVerifyPropagation( IndexPopulator nativePopulator, IndexPopulator lucenePopulator,
            FusionIndexPopulator fusionIndexPopulator, boolean populationCompletedSuccessfully ) throws IOException
    {
        fusionIndexPopulator.close( populationCompletedSuccessfully );

        // then
        verify( nativePopulator, times( 1 ) ).close( populationCompletedSuccessfully );
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
    public void closeMustCloseNativeIfLuceneThrow() throws Exception
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
    }

    @Test
    public void closeMustCloseLuceneIfNativeThrow() throws Exception
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
    }

    @Test
    public void closeMustThrowIfBothThrow() throws Exception
    {
        // given
        IOException nativeFailure = new IOException( "native" );
        IOException luceneFailure = new IOException( "lucene" );
        doThrow( nativeFailure ).when( nativePopulator ).close( anyBoolean() );
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
            assertThat( e, anyOf( sameInstance( nativeFailure ), sameInstance( luceneFailure ) ) );
        }
    }

    /* markAsFailed */

    @Test
    public void markAsFailedMustMarkBothNativeAndLucene() throws Exception
    {
        // when
        String failureMessage = "failure";
        fusionIndexPopulator.markAsFailed( failureMessage );

        // then
        verify( nativePopulator, times( 1 ) ).markAsFailed( failureMessage );
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
    public void shouldIncludeSampleOnCorrectPopulator() throws Exception
    {
        // given
        Value[] numberValues = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] otherValues = FusionIndexTestHelp.valuesNotSupportedByNative();

        for ( Value value : numberValues )
        {
            // when
            IndexEntryUpdate<LabelSchemaDescriptor> update = add( value );
            fusionIndexPopulator.includeSample( update );

            // then
            verify( nativePopulator ).includeSample( update );
            reset( nativePopulator );
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
