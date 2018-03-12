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
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.SpatialCRSSchemaIndex;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.add;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.change;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.remove;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

public class SpatialFusionIndexUpdaterTest
{
    private Map<CoordinateReferenceSystem,IndexUpdater> updaterMap = new HashMap<>();
    private Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexMap = new HashMap<>();
    private SpatialFusionIndexUpdater fusionIndexAccessorUpdater;
    private SpatialFusionIndexUpdater fusionIndexPopulatorUpdater;

    @Before
    public void setup() throws Exception
    {
        SpatialCRSSchemaIndex.Supplier indexSupplier = mock( SpatialCRSSchemaIndex.Supplier.class );
        SchemaIndexDescriptor descriptor = mock( SchemaIndexDescriptor.class );
        IndexSamplingConfig samplingConfig = mock( IndexSamplingConfig.class );

        for ( CoordinateReferenceSystem crs : asList( WGS84,
                                                      Cartesian,
                                                      Cartesian_3D ) )
        {
            updaterMap.put( crs, mock( IndexUpdater.class ) );
            indexMap.put( crs, mock( SpatialCRSSchemaIndex.class ) );
            when( indexSupplier.get( descriptor, indexMap, 0, crs ) ).thenReturn( indexMap.get( crs ) );
            when( indexMap.get( crs ).updaterWithCreate( true ) ).thenReturn( updaterMap.get( crs ) );
            when( indexMap.get( crs ).updaterWithCreate( false ) ).thenReturn( updaterMap.get( crs ) );
        }

        fusionIndexAccessorUpdater = SpatialFusionIndexUpdater.updaterForAccessor(
                indexMap, 0, indexSupplier, descriptor );
        fusionIndexPopulatorUpdater = SpatialFusionIndexUpdater.updaterForPopulator(
                indexMap, 0, indexSupplier, descriptor );
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
        PointValue from = Values.pointValue( Cartesian, 1.0, 1.0 );
        PointValue to = Values.pointValue( Cartesian, 2.0, 2.0 );
        verifyChangeWithCorrectUpdaterNotMixed( fusionIndexAccessorUpdater, updaterMap.get( Cartesian ), from, to );
        reset( updaterMap.get( Cartesian ) );
        verifyChangeWithCorrectUpdaterNotMixed( fusionIndexPopulatorUpdater, updaterMap.get( Cartesian ), from, to );
    }

    @Test
    public void processMustSelectCorrectForChangeCRS() throws Exception
    {
        PointValue from = Values.pointValue( Cartesian, 1.0, 1.0 );
        PointValue to = Values.pointValue( WGS84, 2.0, 2.0 );
        verifyChangeWithCorrectUpdaterMixed( fusionIndexAccessorUpdater, updaterMap.get( Cartesian ),
                updaterMap.get( WGS84 ), from, to );
        reset( updaterMap.values().toArray() );
        verifyChangeWithCorrectUpdaterMixed( fusionIndexPopulatorUpdater, updaterMap.get( Cartesian ),
                updaterMap.get( WGS84 ), from, to );
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
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updaterMap.get( WGS84 ), fusionIndexAccessorUpdater );
        reset( updaterMap.values().toArray() );
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updaterMap.get( WGS84 ), fusionIndexPopulatorUpdater );
    }

    @Test
    public void closeMustThrowIfCartesianThrow() throws Exception
    {
        populateUpdaters();
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updaterMap.get( Cartesian ), fusionIndexAccessorUpdater );
        reset( updaterMap.values().toArray() );
        FusionIndexTestHelp.verifyFusionCloseThrowOnSingleCloseThrow( updaterMap.get( Cartesian ), fusionIndexPopulatorUpdater );
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
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( updaterMap.get( WGS84 ), fusionIndexAccessorUpdater,
                updaterMap.get( Cartesian ) );
        reset( updaterMap.values().toArray() );
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( updaterMap.get( WGS84 ), fusionIndexPopulatorUpdater,
                updaterMap.get( Cartesian ) );
    }

    @Test
    public void closeMustCloseIfCartesianThrow() throws Exception
    {
        populateUpdaters();
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( updaterMap.get( Cartesian ), fusionIndexAccessorUpdater,
                updaterMap.get( WGS84 ) );
        reset( updaterMap.values().toArray() );
        FusionIndexTestHelp.verifyOtherIsClosedOnSingleThrow( updaterMap.get( Cartesian ), fusionIndexPopulatorUpdater,
                updaterMap.get( WGS84 ) );
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
        fusionIndexPopulatorUpdater.process( add( Values.pointValue( Cartesian, 1.0, 1.0 ) ) );
        fusionIndexPopulatorUpdater.process( add( Values.pointValue( WGS84, 1.0, 1.0 ) ) );
        fusionIndexPopulatorUpdater.process( add( Values.pointValue( Cartesian_3D, 1.0, 1.0, 2.0 ) ) );
        fusionIndexAccessorUpdater.process( add( Values.pointValue( Cartesian, 1.0, 1.0 ) ) );
        fusionIndexAccessorUpdater.process( add( Values.pointValue( WGS84, 1.0, 1.0 ) ) );
        fusionIndexAccessorUpdater.process( add( Values.pointValue( Cartesian_3D, 1.0, 1.0, 2.0 ) ) );
    }
}
