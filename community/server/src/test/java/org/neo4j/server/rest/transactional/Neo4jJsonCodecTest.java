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
package org.neo4j.server.rest.transactional;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JsonGenerator.class)
public class Neo4jJsonCodecTest
{

    private Neo4jJsonCodec jsonCodec;
    private JsonGenerator jsonGenerator;

    @Before
    public void init() throws IOException
    {
        jsonCodec = new Neo4jJsonCodec();
        jsonGenerator = mock( JsonGenerator.class );
    }

    @Test
    public void testPropertyContainerWriting() throws IOException
    {
        //Given
        PropertyContainer propertyContainer = mock( PropertyContainer.class );
        when( propertyContainer.getAllProperties() ).thenThrow( RuntimeException.class );

        //When
        try
        {
            jsonCodec.writeValue( jsonGenerator, propertyContainer );
        }
        catch ( Exception e )
        {

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
        catch ( Exception e )
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
        catch ( Exception e )
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
        catch ( Exception e )
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
        catch ( Exception e )
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
}
