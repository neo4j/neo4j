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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.index.schema.SpatialKnownIndex;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.verifyCallFail;

public class SpatialFusionIndexPopulatorTest
{

    private Map<CoordinateReferenceSystem,IndexPopulator> populatorMap = new HashMap<>();
    private SpatialFusionIndexPopulator fusionIndexPopulator;
    private Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap = new HashMap<>();

    @Before
    public void setup() throws Exception
    {
        SpatialKnownIndex.Factory indexFactory = mock( SpatialKnownIndex.Factory.class );
        for ( CoordinateReferenceSystem crs : asList( CoordinateReferenceSystem.WGS84, CoordinateReferenceSystem.Cartesian ) )
        {
            populatorMap.put( crs, mock( IndexPopulator.class ) );
            indexMap.put( crs, mock( SpatialKnownIndex.class ) );

            when( indexMap.get( crs ).getPopulator( any(), any() ) ).thenReturn( populatorMap.get( crs ) );
            when( indexFactory.selectAndCreate( indexMap, 0, crs ) ).thenReturn( indexMap.get( crs ) );
            when( indexMap.get( crs ).getPopulator( any(), any() ) ).thenReturn( populatorMap.get( crs ) );
        }

        when( indexFactory.selectAndCreate( eq( indexMap ), eq( 0L ), any( PointValue.class ) ) ).thenAnswer( a ->
        {
            PointValue pointValue = a.getArgument( 2 );
            return indexMap.get( pointValue.getCoordinateReferenceSystem() );
        } );

        fusionIndexPopulator = new SpatialFusionIndexPopulator( indexMap, 0, mock( IndexDescriptor.class ), null, indexFactory );
    }

    @Test
    public void dropMustDropAll() throws Exception
    {
        // when
        fusionIndexPopulator.drop();

        // then
        for ( IndexPopulator populator : populatorMap.values() )
        {
            verify( populator, times( 1 ) ).drop();
        }
    }

    @Test
    public void dropMustThrowIfWGSThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( populatorMap.get( CoordinateReferenceSystem.WGS84 ) ).drop();

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.drop();
            return null;
        } );
        verify( populatorMap.get( CoordinateReferenceSystem.Cartesian ), times( 1 ) ).drop();
    }

    @Test
    public void dropMustThrowIfCartesianThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( populatorMap.get( CoordinateReferenceSystem.Cartesian ) ).drop();

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.drop();
            return null;
        } );
        verify( populatorMap.get( CoordinateReferenceSystem.WGS84 ), times( 1 ) ).drop();
    }

    @Test
    public void addMustSelectCorrectPopulator() throws Exception
    {
        for ( Value value : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            PointValue point = (PointValue) value;
            verifyAddWithCorrectPopulator( populatorMap.get( point.getCoordinateReferenceSystem() ), value );
        }
    }

    @Test
    public void verifyDeferredConstraintsMustThrowIfWGSThrow() throws Exception
    {
        // given
        IndexEntryConflictException failure = mock( IndexEntryConflictException.class );
        doThrow( failure ).when( populatorMap.get( CoordinateReferenceSystem.WGS84 ) ).verifyDeferredConstraints( any() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.verifyDeferredConstraints( null );
            return null;
        } );
        verify( populatorMap.get( CoordinateReferenceSystem.Cartesian ), times( 1 ) ).verifyDeferredConstraints( any() );
    }

    @Test
    public void verifyDeferredConstraintsMustThrowIfCartesianThrow() throws Exception
    {
        // given
        IndexEntryConflictException failure = mock( IndexEntryConflictException.class );
        doThrow( failure ).when( populatorMap.get( CoordinateReferenceSystem.Cartesian ) ).verifyDeferredConstraints( any() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.verifyDeferredConstraints( null );
            return null;
        } );
        verify( populatorMap.get( CoordinateReferenceSystem.WGS84 ), times( 1 ) ).verifyDeferredConstraints( any() );
    }

    @Test
    public void successfulCloseMustCloseAll() throws Exception
    {
        closeAndVerifyPropagation( true );
    }

    @Test
    public void unsuccessfulCloseMustCloseAll() throws Exception
    {
        closeAndVerifyPropagation( false );
    }

    @Test
    public void closeMustCloseOtherAndThrowIfCloseWGSThrow() throws Exception
    {
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( indexMap.get( CoordinateReferenceSystem.WGS84 ) ).closePopulator( any(), any(), anyBoolean() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.close( true );
            return null;
        } );
        verify( indexMap.get( CoordinateReferenceSystem.Cartesian ), times( 1 ) ).closePopulator( any(), any(), eq( true ) );
    }

    @Test
    public void closeMustCloseOtherAndThrowIfCloseCartesianThrow() throws Exception
    {
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( indexMap.get( CoordinateReferenceSystem.Cartesian ) ).closePopulator( any(), any(), anyBoolean() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.close( true );
            return null;
        } );
        verify( indexMap.get( CoordinateReferenceSystem.WGS84 ), times( 1 ) ).closePopulator( any(), any(), eq( true ) );
    }

    @Test
    public void closeMustThrowIfAllThrow() throws Exception
    {
        IOException wgsFailure = new IOException( "fail" );
        IOException cartesianFailure = new IOException( "fail" );
        doThrow( cartesianFailure ).when( indexMap.get( CoordinateReferenceSystem.Cartesian ) ).closePopulator( any(), any(), anyBoolean() );
        doThrow( wgsFailure ).when( indexMap.get( CoordinateReferenceSystem.WGS84 ) ).closePopulator( any(), any(), anyBoolean() );

        try
        {
            // when
            fusionIndexPopulator.close( true );
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            //then
            assertThat( e, anyOf( sameInstance( wgsFailure ), sameInstance( cartesianFailure ) ) );
        }
    }

    @Test
    public void markAsFailedMustMarkAll() throws Exception
    {
        // when
        String failureMessage = "failure";
        fusionIndexPopulator.markAsFailed( failureMessage );

        // then
        for ( IndexPopulator populator : populatorMap.values() )
        {
            verify( populator, times( 1 ) ).markAsFailed( failureMessage );
        }
    }

    @Test
    public void markAsFailedMustThrowIfWGSThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( populatorMap.get( CoordinateReferenceSystem.WGS84 ) ).markAsFailed( "failed" );

        // then
        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.markAsFailed( "failed" );
            return null;
        } );
        verify( populatorMap.get( CoordinateReferenceSystem.Cartesian ) ).markAsFailed( "failed" );
    }

    @Test
    public void markAsFailedMustThrowIfCartesianThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( populatorMap.get( CoordinateReferenceSystem.Cartesian ) ).markAsFailed( "failed" );

        // then
        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.markAsFailed( "failed" );
            return null;
        } );
        verify( populatorMap.get( CoordinateReferenceSystem.WGS84 ) ).markAsFailed( "failed" );
    }

    @Test
    public void shouldIncludeSampleOnCorrectPopulator() throws Exception
    {
        // given
        for ( Value value : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            PointValue point = (PointValue) value;
            // when
            IndexEntryUpdate<LabelSchemaDescriptor> update = add( value );
            fusionIndexPopulator.includeSample( update );

            // then
            IndexPopulator populator = populatorMap.get( point.getCoordinateReferenceSystem() );
            verify( populator ).includeSample( update );
            reset( populator );
        }
    }

    private void closeAndVerifyPropagation( boolean populationCompletedSuccessfully ) throws IOException
    {
        fusionIndexPopulator.close( populationCompletedSuccessfully );

        // then
        for ( SpatialKnownIndex index : indexMap.values() )
        {
            verify( index, times( 1 ) ).closePopulator( any(), any(), eq( populationCompletedSuccessfully ) );
        }
    }

    private void verifyAddWithCorrectPopulator( IndexPopulator correctPopulator, Value numberValues ) throws IndexEntryConflictException, IOException
    {
        Collection<IndexEntryUpdate<LabelSchemaDescriptor>> update = Collections.singletonList( add( numberValues ) );
        fusionIndexPopulator.add( update );
        verify( correctPopulator, times( 1 ) ).add( update );
        for ( IndexPopulator populator : populatorMap.values() )
        {
            if ( populator != correctPopulator )
            {
                verify( populator, times( 0 ) ).add( update );
            }
        }
    }
}
