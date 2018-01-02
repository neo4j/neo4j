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
package org.neo4j.server.rest.repr;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.neo4j.graphdb.PropertyContainer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.server.rest.repr.RepresentationTestAccess.serialize;

public class PropertiesRepresentationTest
{
    @Test
    public void shouldContainAddedPropertiesWhenCreatedFromPropertyContainer()
    {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put( "foo", "bar" );
        Map<String, Object> serialized = serialize( new PropertiesRepresentation( container( values ) ) );
        assertEquals( "bar", serialized.get( "foo" ) );
    }

    @Test
    public void shouldSerializeToMapWithSamePropertiesWhenCreatedFromPropertyContainer()
    {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put( "foo", "bar" );
        PropertiesRepresentation properties = new PropertiesRepresentation( container( values ) );
        Map<String, Object> map = serialize( properties );
        assertEquals( values, map );
    }

    @Test
    public void shouldSerializeToMap()
    {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put( "string", "value" );
        values.put( "int", 5 );
        values.put( "long", 17L );
        values.put( "double", 3.14 );
        values.put( "float", 42.0f );
        values.put( "string array", new String[] { "one", "two" } );
        values.put( "long array", new long[] { 5L, 17L } );
        values.put( "double array", new double[] { 3.14, 42.0 } );

        PropertiesRepresentation properties = new PropertiesRepresentation( container( values ) );
        Map<String, Object> map = serialize( properties );

        assertEquals( "value", map.get( "string" ) );
        assertEquals( 5, ( (Number) map.get( "int" ) ).longValue() );
        assertEquals( 17, ( (Number) map.get( "long" ) ).longValue() );
        assertEquals( 3.14, ( (Number) map.get( "double" ) ).doubleValue(), 0.0 );
        assertEquals( 42.0, ( (Number) map.get( "float" ) ).doubleValue(), 0.0 );
        assertEqualContent( Arrays.asList( "one", "two" ), (List) map.get( "string array" ) );
        assertEqualContent( Arrays.asList( 5L, 17L ), (List) map.get( "long array" ) );
        assertEqualContent( Arrays.asList( 3.14, 42.0 ), (List) map.get( "double array" ) );
    }

    @Test
    public void shouldBeAbleToSignalEmptiness()
    {
        PropertiesRepresentation properties = new PropertiesRepresentation( container( new HashMap<String, Object>() ) );
        Map<String, Object> values = new HashMap<String, Object>();
        values.put( "key", "value" );
        assertTrue( properties.isEmpty() );
        properties = new PropertiesRepresentation( container( values ) );
        assertFalse( properties.isEmpty() );
    }

    private void assertEqualContent( List<?> expected, List<?> actual )
    {
        assertEquals( expected.size(), actual.size() );
        for ( Iterator<?> ex = expected.iterator(), ac = actual.iterator(); ex.hasNext() && ac.hasNext(); )
        {
            assertEquals( ex.next(), ac.next() );
        }
    }

    static PropertyContainer container( Map<String, Object> values )
    {
        PropertyContainer container = mock( PropertyContainer.class );
        when( container.getPropertyKeys() ).thenReturn( values.keySet() );
        when( container.getAllProperties() ).thenReturn( values );
        for ( Map.Entry<String, Object> entry : values.entrySet() )
        {
            when( container.getProperty( entry.getKey(), null ) ).thenReturn( entry.getValue() );
        }
        return container;
    }
}
