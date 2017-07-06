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

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CombinedIndexPopulatorTest
{
    private IndexPopulator boostPopulator;
    private IndexPopulator fallbackPopulator;
    private CombinedIndexPopulator combinedIndexPopulator;
    private LabelSchemaDescriptor indexKey = SchemaDescriptorFactory.forLabel( 0, 0 );
    private LabelSchemaDescriptor compositeIndexKey = SchemaDescriptorFactory.forLabel( 0, 0, 1 );

    @Before
    public void mockComponents()
    {
        boostPopulator = mock( IndexPopulator.class );
        fallbackPopulator = mock( IndexPopulator.class );
        combinedIndexPopulator = new CombinedIndexPopulator( boostPopulator, fallbackPopulator );
    }

    /* create */

    @Test
    public void createMustCreateBothBoostAndFallback() throws Exception
    {
        // when
        combinedIndexPopulator.create();

        // then
        verify( boostPopulator, times( 1 ) ).create();
        verify( fallbackPopulator, times( 1 ) ).create();
    }

    @Test
    public void createMustThrowIfCreateBoostThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( boostPopulator ).create();

        verifyCallFail( failure, () ->
        {
            combinedIndexPopulator.create();
            return null;
        } );
    }

    @Test
    public void createMustThrowIfCreateFallbackThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( fallbackPopulator ).create();

        verifyCallFail( failure, () ->
        {
            combinedIndexPopulator.create();
            return null;
        } );
    }

    /* drop */

    @Test
    public void dropMustDropBothBoostAndFallback() throws Exception
    {
        // when
        combinedIndexPopulator.drop();

        // then
        verify( boostPopulator, times( 1 ) ).drop();
        verify( fallbackPopulator, times( 1 ) ).drop();
    }

    @Test
    public void dropMustThrowIfDropBoostThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( boostPopulator ).drop();

        verifyCallFail( failure, () ->
        {
            combinedIndexPopulator.drop();
            return null;
        } );
    }

    @Test
    public void dropMustThrowIfDropFallbackThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( fallbackPopulator ).drop();

        verifyCallFail( failure, () ->
        {
            combinedIndexPopulator.drop();
            return null;
        } );
    }

    /* add */

    @Test
    public void addMustSelectCorrectPopulator() throws Exception
    {
        // given
        Value[] numberValues = new Value[]
                {
                        Values.byteValue( (byte) 1 ),
                        Values.shortValue( (short) 2 ),
                        Values.intValue( 3 ),
                        Values.longValue( 4 ),
                        Values.floatValue( 5.6f ),
                        Values.doubleValue( 7.8 )
                };
        Value[] otherValues = new Value[]
                {
                        Values.booleanValue( true ),
                        Values.charValue( 'a' ),
                        Values.stringValue( "bcd" ),
                        Values.booleanArray( new boolean[2] ),
                        Values.byteArray( new byte[]{1, 2} ),
                        Values.shortArray( new short[]{3, 4} ),
                        Values.intArray( new int[]{5, 6} ),
                        Values.longArray( new long[]{7, 8} ),
                        Values.floatArray( new float[]{9.10f, 11.12f} ),
                        Values.doubleArray( new double[]{13.14, 15.16} ),
                        Values.charArray( new char[2] ),
                        Values.stringArray( new String[2] ),
                        Values.NO_VALUE
                };

        // Add with boost for number values
        for ( Value numberValue : numberValues )
        {
            verifyAddWithCorrectPopulator( boostPopulator, fallbackPopulator, numberValue );
        }

        // Add with fallback for other values
        for ( Value otherValue : otherValues )
        {
            verifyAddWithCorrectPopulator( fallbackPopulator, boostPopulator, otherValue );
        }

        // All composite values should go to fallback
        Value[] allValues = ArrayUtils.addAll( numberValues, otherValues );
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectPopulator( fallbackPopulator, boostPopulator, firstValue, secondValue );
            }
        }
    }

    private void verifyAddWithCorrectPopulator( IndexPopulator correctPopulator, IndexPopulator wrongPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = indexEntryUpdate( numberValues );
        combinedIndexPopulator.add( update );
        verify( correctPopulator, times( 1 ) ).add( update );
        verify( wrongPopulator, times( 0 ) ).add( update );
    }

    private IndexEntryUpdate<LabelSchemaDescriptor> indexEntryUpdate( Value... value )
    {
        switch ( value.length )
        {
        case 1:
            return IndexEntryUpdate.add( 0, indexKey, value );
        case 2:
            return IndexEntryUpdate.add( 0, compositeIndexKey, value );
        default:
            return null;
        }
    }

    /* verifyDeferredConstraints */

    @Test
    public void verifyDeferredConstraintsMustThrowIfBoostThrow() throws Exception
    {
        // given
        IndexEntryConflictException failure = mock( IndexEntryConflictException.class );
        doThrow( failure ).when( boostPopulator ).verifyDeferredConstraints( any() );

        verifyCallFail( failure, () ->
        {
            combinedIndexPopulator.verifyDeferredConstraints( null );
            return null;
        } );
    }

    @Test
    public void verifyDeferredConstraintsMustThrowIfFallbackThrow() throws Exception
    {
        // given
        IndexEntryConflictException failure = mock( IndexEntryConflictException.class );
        doThrow( failure ).when( fallbackPopulator ).verifyDeferredConstraints( any() );

        verifyCallFail( failure, () ->
        {
            combinedIndexPopulator.verifyDeferredConstraints( null );
            return null;
        } );
    }

    /* close */

    @Test
    public void successfulCloseMustCloseBothBoostAndFallback() throws Exception
    {
        // when
        closeAndVerifyPropagation( boostPopulator, fallbackPopulator, combinedIndexPopulator, true );
    }

    @Test
    public void unsuccessfulCloseMustCloseBothBoostAndFallback() throws Exception
    {
        // when
        closeAndVerifyPropagation( boostPopulator, fallbackPopulator, combinedIndexPopulator, false );
    }

    private void closeAndVerifyPropagation( IndexPopulator boostPopulator, IndexPopulator fallbackPopulator,
            CombinedIndexPopulator combinedIndexPopulator, boolean populationCompletedSuccessfully ) throws IOException
    {
        combinedIndexPopulator.close( populationCompletedSuccessfully );

        // then
        verify( boostPopulator, times( 1 ) ).close( populationCompletedSuccessfully );
        verify( fallbackPopulator, times( 1 ) ).close( populationCompletedSuccessfully );
    }

    @Test
    public void closeMustThrowIfCloseBoostThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( boostPopulator ).close( anyBoolean() );

        verifyCallFail( failure, () ->
        {
            combinedIndexPopulator.close( anyBoolean() );
            return null;
        } );
    }

    @Test
    public void closeMustThrowIfCloseFallbackThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( fallbackPopulator ).close( anyBoolean() );

        verifyCallFail( failure, () ->
        {
            combinedIndexPopulator.close( anyBoolean() );
            return null;
        } );
    }

    /* markAsFailed */

    @Test
    public void markAsFailedMustMarkBothBoostAndFallback() throws Exception
    {
        // when
        String failureMessage = "failure";
        combinedIndexPopulator.markAsFailed( failureMessage );

        // then
        verify( boostPopulator, times( 1 ) ).markAsFailed( failureMessage );
        verify( fallbackPopulator, times( 1 ) ).markAsFailed( failureMessage );
    }

    @Test
    public void markAsFailedMustThrowIfBoostThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( boostPopulator ).markAsFailed( anyString() );

        // then
        verifyCallFail( failure, () ->
        {
            combinedIndexPopulator.markAsFailed( anyString() );
            return null;
        } );
    }

    @Test
    public void markAsFailedMustThrowIfFallbackThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( fallbackPopulator ).markAsFailed( anyString() );

        // then
        verifyCallFail( failure, () ->
        {
            combinedIndexPopulator.markAsFailed( anyString() );
            return null;
        } );
    }

    // includeSample

    // configureSample

    private void verifyCallFail( Exception expectedFailure, Callable failingCall ) throws Exception
    {
        try
        {
            failingCall.call();
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertSame( expectedFailure, e );
        }
    }
}
