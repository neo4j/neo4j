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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider.DropAction;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.ArrayUtil.contains;
import static org.neo4j.helpers.ArrayUtil.without;
import static org.neo4j.helpers.collection.Iterators.array;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyCallFail;

public class FusionIndexPopulatorTest
{
    private IndexPopulator lucenePopulator;
    private IndexPopulator[] allPopulators;
    private FusionIndexPopulator fusionIndexPopulator;
    private final long indexId = 8;
    private final DropAction dropAction = mock( DropAction.class );

    @Before
    public void mockComponents()
    {
        IndexPopulator stringPopulator = mock( IndexPopulator.class );
        IndexPopulator numberPopulator = mock( IndexPopulator.class );
        IndexPopulator spatialPopulator = mock( IndexPopulator.class );
        IndexPopulator temporalPopulator = mock( IndexPopulator.class );
        lucenePopulator = mock( IndexPopulator.class );
        allPopulators = array( stringPopulator, numberPopulator, spatialPopulator, temporalPopulator, lucenePopulator );
        fusionIndexPopulator = new FusionIndexPopulator( allPopulators, new FusionSelector(), indexId, dropAction );
    }

    /* create */

    @Test
    public void createMustCreateAll() throws Exception
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
    public void createMustThrowIfAnyThrow() throws Exception
    {
        for ( IndexPopulator populator : allPopulators )
        {
            // given
            IOException failure = new IOException( "fail" );
            doThrow( failure ).when( populator ).create();

            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.create();
                return null;
            } );

            // reset throw for testing of next populator
            doAnswer( invocation -> null ).when( populator ).create();
        }
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
    public void dropMustThrowIfAnyDropThrow() throws Exception
    {
        for ( IndexPopulator populator : allPopulators )
        {
            // given
            IOException failure = new IOException( "fail" );
            doThrow( failure ).when( populator ).drop();

            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.drop();
                return null;
            } );

            // reset throw for testing of next populator
            doAnswer( invocation -> null ).when( populator ).drop();
        }
    }

    /* add */

    @Test
    public void addMustSelectCorrectPopulator() throws Exception
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( int i = 0; i < allPopulators.length; i++ )
        {
            for ( Value value : values[i] )
            {
                verifyAddWithCorrectPopulator( allPopulators[i], value );
            }
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
                verify( populator, never() ).add( update );
            }
        }
    }

    /* verifyDeferredConstraints */
    @Test
    public void verifyDeferredConstraintsMustThrowIfAnyThrow() throws Exception
    {
        for ( IndexPopulator populator : allPopulators )
        {
            // given
            IndexEntryConflictException failure = mock( IndexEntryConflictException.class );
            doThrow( failure ).when( populator ).verifyDeferredConstraints( any() );

            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.verifyDeferredConstraints( null );
                return null;
            } );

            // reset throw for testing of next populator
            doAnswer( invocation -> null ).when( populator ).verifyDeferredConstraints( any() );
        }
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
    public void closeMustThrowIfCloseAnyThrow() throws Exception
    {
        for ( IndexPopulator populator : allPopulators )
        {
            // given
            IOException failure = new IOException( "fail" );
            doThrow( failure ).when( populator ).close( anyBoolean() );

            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.close( anyBoolean() );
                return null;
            } );

            // reset throw for testing of next populator
            doAnswer( invocation -> null ).when( populator ).close( anyBoolean() );
        }
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
    public void closeMustCloseOthersIfAnyThrow() throws Exception
    {
        for ( int i = 0; i < allPopulators.length; i++ )
        {
            IndexPopulator populator = allPopulators[i];
            verifyOtherCloseOnThrow( populator, fusionIndexPopulator, without( allPopulators, populator ) );
            mockComponents();
        }
    }

    @Test
    public void closeMustThrowIfAllThrow() throws Exception
    {
        // given
        IOException[] failures = new IOException[allPopulators.length];
        for ( int i = 0; i < allPopulators.length; i++ )
        {
            failures[i] = new IOException( "FAILURE[" + i + "]" );
            doThrow( failures[i] ).when( allPopulators[i] ).close( anyBoolean() );
        }

        try
        {
            // when
            fusionIndexPopulator.close( anyBoolean() );
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            // then
            if ( !contains( failures, e ) )
            {
                fail( "Thrown exception didn't matchh any of the expected failures: " + Arrays.toString( failures ) );
            }
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
    public void markAsFailedMustThrowIfAnyThrow() throws Exception
    {
        for ( IndexPopulator populator : allPopulators )
        {
            // given
            IOException failure = new IOException( "fail" );
            doThrow( failure ).when( populator ).markAsFailed( anyString() );

            // then
            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.markAsFailed( anyString() );
                return null;
            } );

            // reset throw for testing of next populator
            doAnswer( invocation -> null ).when( populator ).markAsFailed( anyString() );
        }
    }

    @Test
    public void shouldIncludeSampleOnCorrectPopulator()
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();

        for ( int i = 0; i < allPopulators.length; i++ )
        {
            verifySampleToCorrectPopulator( values[i], allPopulators[i] );
        }
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
