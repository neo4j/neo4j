/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyCallFail;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v00;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v10;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v20;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.INSTANCE_COUNT;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.TEMPORAL;

@RunWith( Parameterized.class )
public class FusionIndexPopulatorTest
{
    private IndexPopulator[] alivePopulators;
    private IndexPopulator[] populators;
    private FusionIndexPopulator fusionIndexPopulator;
    private final long indexId = 8;
    private final DropAction dropAction = mock( DropAction.class );

    @Parameterized.Parameters( name = "{0}" )
    public static FusionVersion[] versions()
    {
        return new FusionVersion[]
                {
                        v00, v10, v20
                };
    }

    @Parameterized.Parameter
    public static FusionVersion fusionVersion;

    @Before
    public void setup()
    {
        initiateMocks();
    }

    private void initiateMocks()
    {
        int[] aliveSlots = fusionVersion.aliveSlots();
        populators = new IndexPopulator[INSTANCE_COUNT];
        Arrays.fill( populators, IndexPopulator.EMPTY );
        alivePopulators = new IndexPopulator[aliveSlots.length];
        for ( int i = 0; i < aliveSlots.length; i++ )
        {
            IndexPopulator mock = mock( IndexPopulator.class );
            alivePopulators[i] = mock;
            switch ( aliveSlots[i] )
            {
            case STRING:
                populators[STRING] = mock;
                break;
            case NUMBER:
                populators[NUMBER] = mock;
                break;
            case SPATIAL:
                populators[SPATIAL] = mock;
                break;
            case TEMPORAL:
                populators[TEMPORAL] = mock;
                break;
            case LUCENE:
                populators[LUCENE] = mock;
                break;
            default:
                throw new RuntimeException();
            }
        }
        fusionIndexPopulator = new FusionIndexPopulator( fusionVersion.slotSelector(), new InstanceSelector<>( populators ), indexId, dropAction, false );
    }

    private void resetMocks()
    {
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            reset( alivePopulator );
        }
    }

    /* create */

    @Test
    public void createMustCreateAll() throws Exception
    {
        // when
        fusionIndexPopulator.create();

        // then
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            verify( alivePopulator, times( 1 ) ).create();
        }
    }

    @Test
    public void createRemoveAnyLeftOversThatWasThereInIndexDirectoryBeforePopulation() throws IOException
    {
        fusionIndexPopulator.create();

        verify( dropAction ).drop( indexId, false );
    }

    @Test
    public void createMustThrowIfAnyThrow() throws Exception
    {
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            // given
            IOException failure = new IOException( "fail" );
            doThrow( failure ).when( alivePopulator ).create();

            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.create();
                return null;
            } );

            // reset throw for testing of next populator
            doAnswer( invocation -> null ).when( alivePopulator ).create();
        }
    }

    /* drop */

    @Test
    public void dropMustDropAll() throws Exception
    {
        // when
        fusionIndexPopulator.drop();

        // then
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            verify( alivePopulator, times( 1 ) ).drop();
        }
        verify( dropAction ).drop( indexId );
    }

    @Test
    public void dropMustThrowIfAnyDropThrow() throws Exception
    {
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            // given
            IOException failure = new IOException( "fail" );
            doThrow( failure ).when( alivePopulator ).drop();

            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.drop();
                return null;
            } );

            // reset throw for testing of next populator
            doAnswer( invocation -> null ).when( alivePopulator ).drop();
        }
    }

    /* add */

    @Test
    public void addMustSelectCorrectPopulator() throws Exception
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( int i = 0; i < populators.length; i++ )
        {
            for ( Value value : values[i] )
            {
                verifyAddWithCorrectPopulator( orLucene( populators[i] ), value );
            }
        }

        // All composite values should go to lucene
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectPopulator( populators[LUCENE], firstValue, secondValue );
            }
        }
    }

    private void verifyAddWithCorrectPopulator( IndexPopulator correctPopulator, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        Collection<IndexEntryUpdate<LabelSchemaDescriptor>> update = Collections.singletonList( add( numberValues ) );
        fusionIndexPopulator.add( update );
        verify( correctPopulator, times( 1 ) ).add( update );
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            if ( alivePopulator != correctPopulator )
            {
                verify( alivePopulator, never() ).add( update );
            }
        }
    }

    /* verifyDeferredConstraints */
    @Test
    public void verifyDeferredConstraintsMustThrowIfAnyThrow() throws Exception
    {
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            // given
            IndexEntryConflictException failure = mock( IndexEntryConflictException.class );
            doThrow( failure ).when( alivePopulator ).verifyDeferredConstraints( any() );

            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.verifyDeferredConstraints( null );
                return null;
            } );

            // reset throw for testing of next populator
            doAnswer( invocation -> null ).when( alivePopulator ).verifyDeferredConstraints( any() );
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
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            verify( alivePopulator, times( 1 ) ).close( populationCompletedSuccessfully );
        }
    }

    @Test
    public void closeMustThrowIfCloseAnyThrow() throws Exception
    {
        for ( int i = 0; i < alivePopulators.length; i++ )
        {
            // given
            IndexPopulator alivePopulator = alivePopulators[i];
            IOException failure = new IOException( "fail" );
            doThrow( failure ).when( alivePopulator ).close( anyBoolean() );

            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.close( anyBoolean() );
                return null;
            } );

            initiateMocks();
        }
    }

    private void verifyOtherCloseOnThrow( IndexPopulator throwingPopulator ) throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( throwingPopulator ).close( anyBoolean() );

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
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            verify( alivePopulator, times( 1 ) ).close( true );
        }
    }

    @Test
    public void closeMustCloseOthersIfAnyThrow() throws Exception
    {
        for ( int i = 0; i < alivePopulators.length; i++ )
        {
            IndexPopulator throwingPopulator = alivePopulators[i];
            verifyOtherCloseOnThrow( throwingPopulator );
            initiateMocks();
        }
    }

    @Test
    public void closeMustThrowIfAllThrow() throws Exception
    {
        // given
        List<IOException> failures = new ArrayList<>();
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            IOException failure = new IOException( "FAILURE[" + alivePopulator + "]" );
            failures.add( failure );
            doThrow( failure ).when( alivePopulator ).close( anyBoolean() );
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
            if ( !failures.contains( e ) )
            {
                fail( "Thrown exception didn't match any of the expected failures: " + failures );
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
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            verify( alivePopulator, times( 1 ) ).markAsFailed( failureMessage );
        }
    }

    @Test
    public void markAsFailedMustThrowIfAnyThrow() throws Exception
    {
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            // given
            IOException failure = new IOException( "fail" );
            doThrow( failure ).when( alivePopulator ).markAsFailed( anyString() );

            // then
            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.markAsFailed( anyString() );
                return null;
            } );

            // reset throw for testing of next populator
            doAnswer( invocation -> null ).when( alivePopulator ).markAsFailed( anyString() );
        }
    }

    @Test
    public void shouldIncludeSampleOnCorrectPopulator()
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();

        for ( int activeSlot : fusionVersion.aliveSlots() )
        {
            verifySampleToCorrectPopulator( values[activeSlot], populators[activeSlot] );
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

    private IndexPopulator orLucene( IndexPopulator populator )
    {
        return populator != IndexPopulator.EMPTY ? populator : populators[LUCENE];
    }
}
