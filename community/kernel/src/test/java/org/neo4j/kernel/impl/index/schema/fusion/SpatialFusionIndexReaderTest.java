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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.RangePredicate;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SpatialFusionIndexReaderTest
{
    private SpatialFusionIndexReader fusionIndexReader;
    private Map<CoordinateReferenceSystem,IndexReader> readerMap;
    private static final int PROP_KEY = 1;
    private static final int LABEL_KEY = 11;

    @Before
    public void setup()
    {
        readerMap = new HashMap<>();
        readerMap.put( CoordinateReferenceSystem.WGS84, mock( IndexReader.class ) );
        readerMap.put( CoordinateReferenceSystem.Cartesian, mock( IndexReader.class ) );
        readerMap.put( CoordinateReferenceSystem.Cartesian_3D, mock( IndexReader.class ) );
        fusionIndexReader = new SpatialFusionIndexReader( readerMap,
                                                          SchemaIndexDescriptorFactory.forLabel( LABEL_KEY, PROP_KEY ) );
    }

    @Test
    public void closeMustCloseAll() throws Exception
    {
        // when
        fusionIndexReader.close();

        // then
        for ( IndexReader reader : readerMap.values() )
        {
            verify( reader, times( 1 ) ).close();
        }
    }

    @Test
    public void countIndexedNodesMustSelectCorrectReader() throws Exception
    {
        for ( Value spatialValue : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            PointValue point = (PointValue) spatialValue;
            CoordinateReferenceSystem crs = point.getCoordinateReferenceSystem();
            verifyCountIndexedNodesWithCorrectReader( readerMap.get( crs ), spatialValue );
        }
    }

    @Test
    public void mustSelectCorrectForExactPredicate() throws Exception
    {
        for ( Value spatialValue : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            PointValue point = (PointValue) spatialValue;
            CoordinateReferenceSystem crs = point.getCoordinateReferenceSystem();
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, spatialValue );

            verifyQueryWithCorrectReader( readerMap.get( crs ), indexQuery );
        }
    }

    @Test
    public void mustSelectCorrectForRangeGeometricPredicate() throws Exception
    {
        {
            PointValue from = Values.pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 1.0 );
            PointValue to = Values.pointValue( CoordinateReferenceSystem.Cartesian, 2.0, 2.0 );
            RangePredicate geometryRange = IndexQuery.range( PROP_KEY, from, true, to, false );

            verifyQueryWithCorrectReader( readerMap.get( CoordinateReferenceSystem.Cartesian ), geometryRange );
        }

        {
            PointValue from = Values.pointValue( CoordinateReferenceSystem.WGS84, 1.0, 1.0 );
            PointValue to = Values.pointValue( CoordinateReferenceSystem.WGS84, 2.0, 2.0 );
            RangePredicate geometryRange = IndexQuery.range( PROP_KEY, from, true, to, false );

            verifyQueryWithCorrectReader( readerMap.get( CoordinateReferenceSystem.WGS84 ), geometryRange );
        }
    }

    private void verifyCountIndexedNodesWithCorrectReader( IndexReader correct, Value... nativeValue )
    {
        fusionIndexReader.countIndexedNodes( 0, nativeValue );
        verify( correct, times( 1 ) ).countIndexedNodes( 0, nativeValue );
        for ( IndexReader reader : readerMap.values() )
        {
            if ( reader != correct )
            {
                verify( reader, times( 0 ) ).countIndexedNodes( 0, nativeValue );
            }
        }
    }

    private void verifyQueryWithCorrectReader( IndexReader expectedReader, IndexQuery indexQuery )
            throws IndexNotApplicableKernelException
    {
        // when
        fusionIndexReader.query( indexQuery );

        // then
        verify( expectedReader, times( 1 ) ).
                query( any(), eq( IndexOrder.NONE ), eq( indexQuery ) );
        for ( IndexReader reader : readerMap.values() )
        {
            if ( reader != expectedReader )
            {
                verifyNoMoreInteractions( reader );
            }
        }
    }

}
