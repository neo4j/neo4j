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
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.SpatialCRSSchemaIndex;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.NONE;

public class SpatialFusionSchemaIndexProviderTest
{
    @Rule
    public RandomRule random = new RandomRule();

    private Map<CoordinateReferenceSystem,SpatialCRSSchemaIndex> indexMap;
    private SpatialFusionSchemaIndexProvider provider;
    private IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 1, 1 );

    @Before
    public void setup()
    {
        provider = new SpatialFusionSchemaIndexProvider(
                mock( PageCache.class ),
                mock( FileSystemAbstraction.class ),
                NONE,
                SchemaIndexProvider.Monitor.EMPTY,
                null,
                false );
        indexMap = provider.indexesFor( 0 );
        indexMap.put( CoordinateReferenceSystem.WGS84, mock( SpatialCRSSchemaIndex.class ) );
        indexMap.put( CoordinateReferenceSystem.Cartesian, mock( SpatialCRSSchemaIndex.class ) );
    }

    @Test
    public void shouldReportPopulatingIfAnyIsPopulating() throws Exception
    {
        for ( InternalIndexState state : array( InternalIndexState.ONLINE, InternalIndexState.POPULATING ) )
        {
            // when
            for ( SpatialCRSSchemaIndex index : indexMap.values() )
            {
                setInitialState( index, state );
            }

            for ( SpatialCRSSchemaIndex index : indexMap.values() )
            {
                setInitialState( index, InternalIndexState.POPULATING );
                // then
                assertEquals( InternalIndexState.POPULATING, provider.getInitialState( 0, descriptor ) );
                setInitialState( index, state );
            }
        }
    }

    @Test
    public void getPopulationFailureMustThrowIfNoFailure() throws Exception
    {
        // when
        // ... no failure
        for ( SpatialCRSSchemaIndex index : indexMap.values() )
        {
            when( index.readPopulationFailure( descriptor ) ).thenReturn( null );
        }
        // then
        try
        {
            provider.getPopulationFailure( 0, descriptor );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {   // good
        }
    }

    @Test
    public void mustSelectCorrectTargetForValues() throws Exception
    {
        for ( Value spatialValue : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            PointValue point = (PointValue) spatialValue;
            CoordinateReferenceSystem crs = point.getCoordinateReferenceSystem();
            SpatialCRSSchemaIndex index = provider.get( descriptor, indexMap, 0, point.getCoordinateReferenceSystem() );
            assertSame( indexMap.get( crs ), index );
            index = provider.get( descriptor, indexMap, 0, crs );
            assertSame( indexMap.get( crs ), index );
        }
    }

    private void setInitialState( SpatialCRSSchemaIndex mockedIndex, InternalIndexState state ) throws IOException
    {
        when( mockedIndex.indexExists() ).thenReturn( true );
        when( mockedIndex.readState( descriptor ) ).thenReturn( state );
    }
}
