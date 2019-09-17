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
package org.neo4j.server.rest.repr;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.server.rest.repr.RepresentationTestAccess.serialize;

class PropertiesRepresentationTest
{
    @Test
    void shouldContainAddedPropertiesWhenCreatedFromEntity()
    {
        Map<String, Object> values = new HashMap<>();
        values.put( "foo", "bar" );
        Map<String, Object> serialized = serialize( new PropertiesRepresentation( container( values ) ) );
        assertEquals( "bar", serialized.get( "foo" ) );
    }

    @Test
    void shouldSerializeToMapWithSamePropertiesWhenCreatedFromEntity()
    {
        Map<String, Object> values = new HashMap<>();
        values.put( "foo", "bar" );
        PropertiesRepresentation properties = new PropertiesRepresentation( container( values ) );
        Map<String, Object> map = serialize( properties );
        assertEquals( values, map );
    }

    @Test
    void shouldSerializeToMap()
    {
        Map<String, Object> values = new HashMap<>();
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
    void shouldBeAbleToSignalEmptiness()
    {
        PropertiesRepresentation properties = new PropertiesRepresentation( container( new HashMap<>() ) );
        Map<String, Object> values = new HashMap<>();
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

    static Entity container( Map<String, Object> values )
    {
        Entity container = mock( Entity.class );
        when( container.getPropertyKeys() ).thenReturn( values.keySet() );
        when( container.getAllProperties() ).thenReturn( values );
        for ( Map.Entry<String, Object> entry : values.entrySet() )
        {
            when( container.getProperty( entry.getKey(), null ) ).thenReturn( entry.getValue() );
        }
        return container;
    }
}
