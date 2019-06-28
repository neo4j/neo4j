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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;

import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.neo4j.internal.schema.IndexProviderDescriptor.UNDECIDED;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.fill;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyCallFail;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.GENERIC;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.LUCENE;

abstract class FusionIndexPopulatorTest
{
    private static final long indexId = 8;
    private IndexPopulator[] alivePopulators;
    private EnumMap<IndexSlot,IndexPopulator> populators;
    private FusionIndexPopulator fusionIndexPopulator;
    private FileSystemAbstraction fs;
    private IndexDirectoryStructure directoryStructure;

    private final FusionVersion fusionVersion;

    FusionIndexPopulatorTest( FusionVersion fusionVersion )
    {
        this.fusionVersion = fusionVersion;
    }

    @BeforeEach
    void setup()
    {
        initiateMocks();
    }

    private void initiateMocks()
    {
        IndexSlot[] aliveSlots = fusionVersion.aliveSlots();
        populators = new EnumMap<>( IndexSlot.class );
        fill( populators, IndexPopulator.EMPTY );
        alivePopulators = new IndexPopulator[aliveSlots.length];
        for ( int i = 0; i < aliveSlots.length; i++ )
        {
            IndexPopulator mock = mock( IndexPopulator.class );
            alivePopulators[i] = mock;
            switch ( aliveSlots[i] )
            {
            case GENERIC:
                populators.put( GENERIC, mock );
                break;
            case LUCENE:
                populators.put( LUCENE, mock );
                break;
            default:
                throw new RuntimeException();
            }
        }
        SlotSelector slotSelector = fusionVersion.slotSelector();
        InstanceSelector<IndexPopulator> instanceSelector = new InstanceSelector<>( populators );
        fs = mock( FileSystemAbstraction.class );
        directoryStructure = directoriesByProvider( new File( "storeDir" ) ).forProvider( UNDECIDED );
        fusionIndexPopulator = new FusionIndexPopulator( slotSelector, instanceSelector, indexId, fs, directoryStructure, false );
    }

    /* create */

    @Test
    void createMustCreateAll()
    {
        // when
        fusionIndexPopulator.create();

        // then
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            verify( alivePopulator ).create();
        }
    }

    @Test
    void createRemoveAnyLeftOversThatWasThereInIndexDirectoryBeforePopulation() throws IOException
    {
        fusionIndexPopulator.create();

        verify( fs ).deleteRecursively( directoryStructure.directoryForIndex( indexId ) );
    }

    @Test
    void createMustThrowIfAnyThrow()
    {
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            // given
            UncheckedIOException failure = new UncheckedIOException( new IOException( "fail" ) );
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
    void dropMustDropAll() throws IOException
    {
        // when
        fusionIndexPopulator.drop();

        // then
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            verify( alivePopulator ).drop();
        }
        verify( fs ).deleteRecursively( directoryStructure.directoryForIndex( indexId ) );
    }

    @Test
    void dropMustThrowIfAnyDropThrow()
    {
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            // given
            UncheckedIOException failure = new UncheckedIOException( new IOException( "fail" ) );
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
    void addMustSelectCorrectPopulator() throws Exception
    {
        // given
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( IndexSlot slot : IndexSlot.values() )
        {
            for ( Value value : values.get( slot ) )
            {
                verifyAddWithCorrectPopulator( orLucene( populators.get( slot ) ), value );
            }
        }

        // All composite values should go to generic
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyAddWithCorrectPopulator( populators.get( GENERIC ), firstValue, secondValue );
            }
        }
    }

    private void verifyAddWithCorrectPopulator( IndexPopulator correctPopulator, Value... numberValues )
            throws IndexEntryConflictException
    {
        Collection<IndexEntryUpdate<LabelSchemaDescriptor>> update = Collections.singletonList( add( numberValues ) );
        fusionIndexPopulator.add( update );
        verify( correctPopulator ).add( update );
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
    void verifyDeferredConstraintsMustThrowIfAnyThrow() throws Exception
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
    void successfulCloseMustCloseAll()
    {
        // when
        closeAndVerifyPropagation( true );
    }

    @Test
    void unsuccessfulCloseMustCloseAll()
    {
        // when
        closeAndVerifyPropagation( false );
    }

    private void closeAndVerifyPropagation( boolean populationCompletedSuccessfully )
    {
        fusionIndexPopulator.close( populationCompletedSuccessfully );

        // then
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            verify( alivePopulator ).close( populationCompletedSuccessfully );
        }
    }

    @Test
    void closeMustThrowIfCloseAnyThrow()
    {
        for ( IndexSlot aliveSlot : fusionVersion.aliveSlots() )
        {
            // given
            UncheckedIOException failure = new UncheckedIOException( new IOException( "fail" ) );
            doThrow( failure ).when( populators.get( aliveSlot ) ).close( anyBoolean() );

            verifyCallFail( failure, () ->
            {
                fusionIndexPopulator.close( anyBoolean() );
                return null;
            } );

            initiateMocks();
        }
    }

    private void verifyOtherCloseOnThrow( IndexPopulator throwingPopulator )
    {
        // given
        UncheckedIOException failure = new UncheckedIOException( new IOException( "fail" ) );
        doThrow( failure ).when( throwingPopulator ).close( anyBoolean() );

        assertThrows( UncheckedIOException.class, () -> fusionIndexPopulator.close( true ) );

        // then
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            verify( alivePopulator ).close( true );
        }
    }

    @Test
    void closeMustCloseOthersIfAnyThrow()
    {
        for ( IndexSlot throwingSlot : fusionVersion.aliveSlots() )
        {
            verifyOtherCloseOnThrow( populators.get( throwingSlot ) );
            initiateMocks();
        }
    }

    @Test
    void closeMustThrowIfAllThrow()
    {
        // given
        List<UncheckedIOException> failures = new ArrayList<>();
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            UncheckedIOException failure = new UncheckedIOException( new IOException( "FAILURE[" + alivePopulator + "]" ) );
            failures.add( failure );
            doThrow( failure ).when( alivePopulator ).close( anyBoolean() );
        }

        var e = assertThrows( UncheckedIOException.class, () -> fusionIndexPopulator.close( anyBoolean() ) );
        if ( !failures.contains( e ) )
        {
            fail( "Thrown exception didn't match any of the expected failures: " + failures );
        }
    }

    /* markAsFailed */
    @Test
    void markAsFailedMustMarkAll()
    {
        // when
        String failureMessage = "failure";
        fusionIndexPopulator.markAsFailed( failureMessage );

        // then
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            verify( alivePopulator ).markAsFailed( failureMessage );
        }
    }

    @Test
    void markAsFailedMustThrowIfAnyThrow()
    {
        for ( IndexPopulator alivePopulator : alivePopulators )
        {
            // given
            UncheckedIOException failure = new UncheckedIOException( new IOException( "fail" ) );
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
    void shouldIncludeSampleOnCorrectPopulator()
    {
        // given
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();

        for ( IndexSlot activeSlot : fusionVersion.aliveSlots() )
        {
            verifySampleToCorrectPopulator( values.get( activeSlot ), populators.get( activeSlot ) );
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
        return populator != IndexPopulator.EMPTY ? populator : populators.get( LUCENE );
    }
}
