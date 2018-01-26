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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.SpatialKnownIndex;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.change;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.remove;

public class SpatialFusionIndexUpdaterTest
{
    private Map<CoordinateReferenceSystem,IndexUpdater> updaterMap = new HashMap<>();
    private Map<CoordinateReferenceSystem,SpatialKnownIndex> indexMap = new HashMap<>();
    private Map<CoordinateReferenceSystem,IndexAccessor> accessorMap = new HashMap<>();
    private Map<CoordinateReferenceSystem,IndexPopulator> populatorMap = new HashMap<>();
    private SpatialFusionIndexUpdater fusionIndexAccessorUpdater;
    private SpatialFusionIndexUpdater fusionIndexPopulatorUpdater;

    @Before
    public void setup() throws Exception
    {
        SpatialKnownIndex.Factory indexFactory = mock( SpatialKnownIndex.Factory.class );

        for ( CoordinateReferenceSystem crs : asList( CoordinateReferenceSystem.WGS84, CoordinateReferenceSystem.Cartesian ) )
        {
            updaterMap.put( crs, mock( IndexUpdater.class ) );
            indexMap.put( crs, mock( SpatialKnownIndex.class ) );
            accessorMap.put( crs, mock( IndexAccessor.class ) );
            populatorMap.put( crs, mock( IndexPopulator.class ) );
            when( indexFactory.selectAndCreate( indexMap, 0, crs ) ).thenReturn( indexMap.get( crs ) );
            when( indexMap.get( crs ).getOnlineAccessor( any(), any() ) ).thenReturn( accessorMap.get( crs ) );
            when( accessorMap.get( crs ).newUpdater( any() ) ).thenReturn( updaterMap.get( crs ) );
            when( indexMap.get( crs ).getPopulator( any(), any() ) ).thenReturn( populatorMap.get( crs ) );
            when( populatorMap.get( crs ).newPopulatingUpdater( any() ) ).thenReturn( updaterMap.get( crs ) );
        }

        fusionIndexAccessorUpdater = new SpatialFusionIndexUpdater( indexMap, 0, indexFactory, mock( IndexDescriptor.class ), null, IndexUpdateMode.ONLINE );
        fusionIndexPopulatorUpdater =
                new SpatialFusionIndexUpdater( indexMap, 0, indexFactory, mock( IndexDescriptor.class ), null, mock( PropertyAccessor.class ) );
    }

    @Test
    public void processMustSelectCorrectForAdd() throws Exception
    {
        for ( Value value : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            PointValue point = (PointValue) value;
            CoordinateReferenceSystem crs = point.getCoordinateReferenceSystem();
            verifyAddWithCorrectUpdater( fusionIndexAccessorUpdater, updaterMap.get( crs ), value );
            reset( updaterMap.get( crs ) );
            verifyAddWithCorrectUpdater( fusionIndexPopulatorUpdater, updaterMap.get( crs ), value );
        }
    }

    @Test
    public void processMustSelectCorrectForRemove() throws Exception
    {
        for ( Value value : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            PointValue point = (PointValue) value;
            CoordinateReferenceSystem crs = point.getCoordinateReferenceSystem();
            verifyRemoveWithCorrectUpdater( fusionIndexAccessorUpdater, updaterMap.get( crs ), value );
            reset( updaterMap.get( crs ) );
            verifyRemoveWithCorrectUpdater( fusionIndexPopulatorUpdater, updaterMap.get( crs ), value );
        }
    }

    @Test
    public void processMustSelectCorrectForChangeSameCRS() throws Exception
    {
        PointValue from = Values.pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 1.0 );
        PointValue to = Values.pointValue( CoordinateReferenceSystem.Cartesian, 2.0, 2.0 );
        verifyChangeWithCorrectUpdaterNotMixed( fusionIndexAccessorUpdater, updaterMap.get( CoordinateReferenceSystem.Cartesian ), from, to );
        reset( updaterMap.get( CoordinateReferenceSystem.Cartesian ) );
        verifyChangeWithCorrectUpdaterNotMixed( fusionIndexPopulatorUpdater, updaterMap.get( CoordinateReferenceSystem.Cartesian ), from, to );
    }

    @Test
    public void processMustSelectCorrectForChangeCRS() throws Exception
    {
        PointValue from = Values.pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 1.0 );
        PointValue to = Values.pointValue( CoordinateReferenceSystem.WGS84, 2.0, 2.0 );
        verifyChangeWithCorrectUpdaterMixed( fusionIndexAccessorUpdater, updaterMap.get( CoordinateReferenceSystem.Cartesian ),
                updaterMap.get( CoordinateReferenceSystem.WGS84 ), from, to );
        reset( updaterMap.values().toArray() );
        verifyChangeWithCorrectUpdaterMixed( fusionIndexPopulatorUpdater, updaterMap.get( CoordinateReferenceSystem.Cartesian ),
                updaterMap.get( CoordinateReferenceSystem.WGS84 ), from, to );
    }

    @Test
    public void closeMustCloseAll() throws Exception
    {
        //given
        populateUpdaters();

        // when
        fusionIndexPopulatorUpdater.close();

        // then
        for ( IndexUpdater updater : updaterMap.values() )
        {
            verify( updater, times( 1 ) ).close();
            reset( updater );
        }

        // when
        fusionIndexAccessorUpdater.close();

        // then
        for ( IndexUpdater updater : updaterMap.values() )
        {
            verify( updater, times( 1 ) ).close();
        }
    }

    @Test
    public void closeMustThrowIfWGSThrow() throws Exception
    {
        populateUpdaters();
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updaterMap.get( CoordinateReferenceSystem.WGS84 ), fusionIndexAccessorUpdater );
        reset( updaterMap.values().toArray() );
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updaterMap.get( CoordinateReferenceSystem.WGS84 ), fusionIndexPopulatorUpdater );
    }

    @Test
    public void closeMustThrowIfCartesianThrow() throws Exception
    {
        populateUpdaters();
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updaterMap.get( CoordinateReferenceSystem.Cartesian ), fusionIndexAccessorUpdater );
        reset( updaterMap.values().toArray() );
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updaterMap.get( CoordinateReferenceSystem.Cartesian ), fusionIndexPopulatorUpdater );
    }

    @Test
    public void closeMustThrowIfAllThrow() throws Exception
    {
        populateUpdaters();
        FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow( fusionIndexAccessorUpdater, updaterMap.values().toArray(new AutoCloseable[2]) );
        reset( updaterMap.values().toArray() );
        FusionIndexTestHelp.verifyFusionCloseThrowIfAllThrow( fusionIndexPopulatorUpdater, updaterMap.values().toArray(new AutoCloseable[2]) );

    }

    @Test
    public void closeMustCloseIfWGSThrow() throws Exception
    {
        populateUpdaters();
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( updaterMap.get( CoordinateReferenceSystem.WGS84 ), fusionIndexAccessorUpdater,
                updaterMap.get( CoordinateReferenceSystem.Cartesian ) );
        reset( updaterMap.values().toArray() );
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( updaterMap.get( CoordinateReferenceSystem.WGS84 ), fusionIndexPopulatorUpdater,
                updaterMap.get( CoordinateReferenceSystem.Cartesian ) );
    }

    @Test
    public void closeMustCloseIfCartesianThrow() throws Exception
    {
        populateUpdaters();
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( updaterMap.get( CoordinateReferenceSystem.Cartesian ), fusionIndexAccessorUpdater,
                updaterMap.get( CoordinateReferenceSystem.WGS84 ) );
        reset( updaterMap.values().toArray() );
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( updaterMap.get( CoordinateReferenceSystem.Cartesian ), fusionIndexPopulatorUpdater,
                updaterMap.get( CoordinateReferenceSystem.WGS84 ) );
    }

    private void verifyAddWithCorrectUpdater( SpatialFusionIndexUpdater fusionIndexUpdater, IndexUpdater indexUpdater, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = add( numberValues );
        fusionIndexUpdater.process( update );
        verify( indexUpdater, times( 1 ) ).process( update );
        for ( IndexUpdater populator : updaterMap.values() )
        {
            if ( populator != indexUpdater )
            {
                verify( populator, times( 0 ) ).process( update );
            }
        }
    }

    private void verifyRemoveWithCorrectUpdater( SpatialFusionIndexUpdater fusionIndexUpdater, IndexUpdater indexUpdater, Value... numberValues )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = remove( numberValues );
        fusionIndexUpdater.process( update );
        verify( indexUpdater, times( 1 ) ).process( update );
        for ( IndexUpdater populator : updaterMap.values() )
        {
            if ( populator != indexUpdater )
            {
                verify( populator, times( 0 ) ).process( update );
            }
        }
    }

    private void verifyChangeWithCorrectUpdaterNotMixed( SpatialFusionIndexUpdater fusionIndexUpdater, IndexUpdater indexUpdater, Value before, Value after )
            throws IndexEntryConflictException, IOException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> update = FusionIndexTestHelp.change( before, after );
        fusionIndexUpdater.process( update );
        verify( indexUpdater, times( 1 ) ).process( update );
        for ( IndexUpdater populator : updaterMap.values() )
        {
            if ( populator != indexUpdater )
            {
                verify( populator, times( 0 ) ).process( update );
            }
        }
    }

    private void verifyChangeWithCorrectUpdaterMixed( SpatialFusionIndexUpdater fusionIndexUpdater, IndexUpdater expectRemoveFrom, IndexUpdater expectAddTo,
            Value beforeValue, Value afterValue ) throws IOException, IndexEntryConflictException
    {
        IndexEntryUpdate<LabelSchemaDescriptor> change = change( beforeValue, afterValue );
        IndexEntryUpdate<LabelSchemaDescriptor> remove = remove( beforeValue );
        IndexEntryUpdate<LabelSchemaDescriptor> add = add( afterValue );
        fusionIndexUpdater.process( change );
        verify( expectRemoveFrom, times( 1 ) ).process( remove );
        verify( expectAddTo, times( 1 ) ).process( add );
    }

    private void populateUpdaters() throws IOException, IndexEntryConflictException
    {
        fusionIndexPopulatorUpdater.process( add( Values.pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 1.0 ) ) );
        fusionIndexPopulatorUpdater.process( add( Values.pointValue( CoordinateReferenceSystem.WGS84, 1.0, 1.0 ) ) );
        fusionIndexAccessorUpdater.process( add( Values.pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 1.0 ) ) );
        fusionIndexAccessorUpdater.process( add( Values.pointValue( CoordinateReferenceSystem.WGS84, 1.0, 1.0 ) ) );
    }
}
