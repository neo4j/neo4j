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
package org.neo4j.server.rest.transactional;

import org.codehaus.jackson.JsonGenerator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class Neo4jJsonCodecTest extends TxStateCheckerTestSupport
{

    private Neo4jJsonCodec jsonCodec;
    private JsonGenerator jsonGenerator;

    @Before
    public void init() throws IOException
    {
        jsonCodec = new Neo4jJsonCodec( TPTPMC );
        jsonGenerator = mock( JsonGenerator.class );
    }

    @Test
    public void testPropertyContainerWriting() throws IOException
    {
        //Given
        PropertyContainer propertyContainer = mock( PropertyContainer.class );
        when( propertyContainer.getAllProperties() ).thenThrow( RuntimeException.class );

        boolean exceptionThrown = false;
        //When
        try
        {
            jsonCodec.writeValue( jsonGenerator, propertyContainer );
        }
        catch ( IllegalArgumentException e )
        {
            //Then
            verify( jsonGenerator, times( 0 ) ).writeEndObject();
            exceptionThrown = true;
        }

        assertTrue( exceptionThrown );
    }

    @Test
    public void testNodeWriting() throws IOException
    {
        //Given
        PropertyContainer node = mock( Node.class );
        when( node.getAllProperties() ).thenThrow( RuntimeException.class );

        //When
        try
        {
            jsonCodec.writeValue( jsonGenerator, node );
        }
        catch ( RuntimeException e )
        {
            // do nothing
        }

        //Then
        verify( jsonGenerator, times( 1 ) ).writeEndObject();
    }

    @Test
    public void testRelationshipWriting() throws IOException
    {
        //Given
        PropertyContainer relationship = mock( Relationship.class );
        when( relationship.getAllProperties() ).thenThrow( RuntimeException.class );

        //When
        try
        {
            jsonCodec.writeValue( jsonGenerator, relationship );
        }
        catch ( Exception e )
        {
            // do nothing
        }

        //Then
        verify( jsonGenerator, times( 1 ) ).writeEndObject();
    }

    @Test
    public void testPathWriting() throws IOException
    {
        //Given
        Path path = mock( Path.class );
        PropertyContainer propertyContainer = mock( PropertyContainer.class );
        when( propertyContainer.getAllProperties() ).thenThrow( RuntimeException.class );
        when( path.iterator() ).thenReturn( Arrays.asList(propertyContainer).listIterator() );

        //When
        try
        {
            jsonCodec.writeValue( jsonGenerator, path );
        }
        catch ( Exception ignored )
        {

        }

        //Then
        verify( jsonGenerator, times( 1 ) ).writeEndArray();
    }

    @Test
    public void testIteratorWriting() throws IOException
    {
        //Given
        PropertyContainer propertyContainer = mock( PropertyContainer.class );
        when( propertyContainer.getAllProperties() ).thenThrow( RuntimeException.class );

        //When
        try
        {
            jsonCodec.writeValue( jsonGenerator, Arrays.asList( propertyContainer ) );
        }
        catch ( Exception ignored )
        {

        }

        //Then
        verify( jsonGenerator, times( 1 ) ).writeEndArray();
    }

    @Test
    public void testByteArrayWriting() throws IOException
    {
        //Given
        doThrow( new RuntimeException() ).when( jsonGenerator ).writeNumber( anyInt() );
        byte[] byteArray = new byte[]{ 1, 2, 3};

        //When
        try
        {
            jsonCodec.writeValue( jsonGenerator, byteArray );
        }
        catch ( Exception ignored )
        {

        }

        //Then
        verify( jsonGenerator, times( 1 ) ).writeEndArray();
    }

    @Test
    public void testMapWriting() throws IOException
    {
        //Given
        doThrow( new RuntimeException() ).when( jsonGenerator ).writeFieldName( anyString() );

        //When
        try
        {
            jsonCodec.writeValue( jsonGenerator, new HashMap<String, String>() );
        }
        catch ( Exception ignored )
        {

        }

        //Then
        verify( jsonGenerator, times( 1 ) ).writeEndObject();
    }

    @Test
    public void shouldWriteAMapContainingNullAsKeysAndValues() throws IOException
    {
        // given
        Map<String,String> map = new HashMap<>();
        map.put( null, null );

        // when
        jsonCodec.writeValue( jsonGenerator, map );

        // then
        verify( jsonGenerator, times( 1 ) ).writeFieldName( "null" );
    }

    @Test
    public void testGeographicPointWriting() throws IOException
    {
        //Given
        Point value = new MockPoint( 12.3, 45.6, mockWGS84() );

        //When
        jsonCodec.writeValue( jsonGenerator, value );

        //Then
        verify( jsonGenerator, times( 3 ) ).writeEndObject();
    }

    @Test
    public void testCartesianPointWriting() throws IOException
    {
        //Given
        Point value = new MockPoint( 123.0, 456.0, mockCartesian() );

        //When
        jsonCodec.writeValue( jsonGenerator, value );

        //Then
        verify( jsonGenerator, times( 3 ) ).writeEndObject();
    }

    @Test
    public void testGeometryWriting() throws IOException
    {
        //Given
        List<Coordinate> points = new ArrayList<>();
        points.add( new Coordinate( 1, 2 ) );
        points.add( new Coordinate( 2, 3 ) );
        Geometry value = new MockGeometry( "LineString", points, mockCartesian() );

        //When
        jsonCodec.writeValue( jsonGenerator, value );

        //Then
        verify( jsonGenerator, times( 3 ) ).writeEndObject();
    }

    public static CRS mockWGS84()
    {
        return mockCRS( 4326, "WGS-84", "http://spatialreference.org/ref/epsg/4326/" );
    }

    public static CRS mockCartesian()
    {
        return mockCRS( 7203, "cartesian", "http://spatialreference.org/ref/sr-org/7203/" );
    }

    public static CRS mockCRS( final int code, final String type, final String href )
    {
        return new CRS()
        {
            public int getCode()
            {
                return code;
            }

            public String getType()
            {
                return type;
            }

            public String getHref()
            {
                return href;
            }
        };
    }

    public static class MockPoint extends MockGeometry implements Point
    {
        private final Coordinate coordinate;

        public MockPoint( final double x, final double y, final CRS crs )
        {
            super( "Point", new ArrayList<>(), crs );
            this.coordinate = new Coordinate( x, y );
            this.coordinates.add( this.coordinate );
        }
    }

    public static class MockGeometry implements Geometry
    {
        protected final String geometryType;
        protected final CRS crs;
        protected final List<Coordinate> coordinates;

        public MockGeometry( String geometryType, final List<Coordinate> coordinates, final CRS crs )
        {
            this.geometryType = geometryType;
            this.coordinates = coordinates;
            this.crs = crs;
        }

        @Override
        public String getGeometryType()
        {
            return geometryType;
        }

        @Override
        public List<Coordinate> getCoordinates()
        {
            return coordinates;
        }

        @Override
        public CRS getCRS()
        {
            return crs;
        }
    }

}
