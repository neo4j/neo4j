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

    private SpatialFusionIndexPopulator fusionIndexPopulator;
    private Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap = new HashMap<>();

    @Before
    public void setup() throws Exception
    {
        SpatialKnownIndex.Factory indexFactory = mock( SpatialKnownIndex.Factory.class );
        IndexDescriptor descriptor = mock( IndexDescriptor.class );

        for ( CoordinateReferenceSystem crs : asList( CoordinateReferenceSystem.WGS84, CoordinateReferenceSystem.Cartesian ) )
        {
            indexMap.put( crs, mock( SpatialKnownIndex.class ) );

            when( indexFactory.selectAndCreate( indexMap, 0, crs ) ).thenReturn( indexMap.get( crs ) );
        }

        fusionIndexPopulator = new SpatialFusionIndexPopulator( indexMap, 0, descriptor, null, indexFactory );
    }

    @Test
    public void dropMustDropAll() throws Exception
    {
        // when
        fusionIndexPopulator.drop();

        // then
        for ( SpatialKnownIndex spatialKnownIndex : indexMap.values() )
        {
            verify( spatialKnownIndex, times( 1 ) ).drop();
        }
    }

    @Test
    public void dropMustThrowIfWGSThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( indexMap.get( CoordinateReferenceSystem.WGS84 ) ).drop();

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.drop();
            return null;
        } );
        verify( indexMap.get( CoordinateReferenceSystem.Cartesian ), times( 1 ) ).drop();
    }

    @Test
    public void dropMustThrowIfCartesianThrow() throws Exception
    {
        // given
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( indexMap.get( CoordinateReferenceSystem.Cartesian ) ).drop();

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.drop();
            return null;
        } );
        verify( indexMap.get( CoordinateReferenceSystem.WGS84 ), times( 1 ) ).drop();
    }

    @Test
    public void addMustSelectCorrectPopulator() throws Exception
    {
        for ( Value value : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            PointValue point = (PointValue) value;
            verifyAddWithCorrectSpatialIndex( indexMap.get( point.getCoordinateReferenceSystem() ), value );
        }
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
        doThrow( failure ).when( indexMap.get( CoordinateReferenceSystem.WGS84 ) ).finishPopulation( anyBoolean() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.close( true );
            return null;
        } );
        verify( indexMap.get( CoordinateReferenceSystem.Cartesian ), times( 1 ) ).finishPopulation( true );
    }

    @Test
    public void closeMustCloseOtherAndThrowIfCloseCartesianThrow() throws Exception
    {
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( indexMap.get( CoordinateReferenceSystem.Cartesian ) ).finishPopulation( anyBoolean() );

        verifyCallFail( failure, () ->
        {
            fusionIndexPopulator.close( true );
            return null;
        } );
        verify( indexMap.get( CoordinateReferenceSystem.WGS84 ), times( 1 ) ).finishPopulation( true );
    }

    @Test
    public void closeMustThrowIfAllThrow() throws Exception
    {
        IOException wgsFailure = new IOException( "fail" );
        IOException cartesianFailure = new IOException( "fail" );
        doThrow( cartesianFailure ).when( indexMap.get( CoordinateReferenceSystem.Cartesian ) ).finishPopulation( anyBoolean() );
        doThrow( wgsFailure ).when( indexMap.get( CoordinateReferenceSystem.WGS84 ) ).finishPopulation( anyBoolean() );

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
        for ( SpatialKnownIndex spatialKnownIndex : indexMap.values() )
        {
            verify( spatialKnownIndex, times( 1 ) ).markAsFailed( failureMessage );
        }
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
            SpatialKnownIndex spatialKnownIndex = indexMap.get( point.getCoordinateReferenceSystem() );
            verify( spatialKnownIndex ).includeSample( update );
            reset( spatialKnownIndex );
        }
    }

    private void closeAndVerifyPropagation( boolean populationCompletedSuccessfully ) throws IOException
    {
        fusionIndexPopulator.close( populationCompletedSuccessfully );

        // then
        for ( SpatialKnownIndex index : indexMap.values() )
        {
            verify( index, times( 1 ) ).finishPopulation( populationCompletedSuccessfully );
        }
    }

    private void verifyAddWithCorrectSpatialIndex( SpatialKnownIndex correctSpatialIndex, Value numberValues ) throws IndexEntryConflictException, IOException
    {
        Collection<IndexEntryUpdate<?>> update = Collections.singletonList( add( numberValues ) );
        fusionIndexPopulator.add( update );
        verify( correctSpatialIndex, times( 1 ) ).add( update );
        for ( SpatialKnownIndex spatialKnownIndex : indexMap.values() )
        {
            if ( spatialKnownIndex != correctSpatialIndex )
            {
                verify( spatialKnownIndex, times( 0 ) ).add( update );
            }
        }
    }
}
