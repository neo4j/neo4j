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
package org.neo4j.values;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.charArray;
import static org.neo4j.values.storable.Values.charValue;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatArray;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.numberValue;
import static org.neo4j.values.storable.Values.pointArray;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.shortArray;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.emptyMap;
import static org.neo4j.values.virtual.VirtualValues.fromList;
import static org.neo4j.values.virtual.VirtualValues.list;
import static org.neo4j.values.virtual.VirtualValues.map;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.path;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;

@RunWith( Parameterized.class )
public class ValueMapperTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> parameters()
    {
        NodeValue node1 = nodeValue( 1, stringArray(), emptyMap() );
        NodeValue node2 = nodeValue( 2, stringArray(), emptyMap() );
        NodeValue node3 = nodeValue( 3, stringArray(), emptyMap() );
        RelationshipValue relationship1 = relationshipValue( 100, node1, node2, stringValue( "ONE" ), emptyMap() );
        RelationshipValue relationship2 = relationshipValue( 200, node2, node2, stringValue( "TWO" ), emptyMap() );
        return Arrays.asList(
                new Object[] {node1},
                new Object[] {relationship1},
                new Object[] {
                        path(
                                new NodeValue[] {node1, node2, node3},
                                new RelationshipValue[] {relationship1, relationship2} )},
                new Object[] {
                        map(
                                new String[] {"alpha", "beta"},
                                new AnyValue[] {stringValue( "one" ), numberValue( 2 )} )},
                new Object[] {NO_VALUE},
                new Object[] {list( numberValue( 1 ), stringValue( "fine" ), node2 )},
                new Object[] {stringValue( "hello world" )},
                new Object[] {stringArray( "hello", "brave", "new", "world" )},
                new Object[] {booleanValue( false )},
                new Object[] {booleanArray( new boolean[] {true, false, true} )},
                new Object[] {charValue( '\n' )},
                new Object[] {charArray( new char[] {'h', 'e', 'l', 'l', 'o'} )},
                new Object[] {byteValue( (byte) 3 )},
                new Object[] {byteArray( new byte[] {0x00, (byte) 0x99, (byte) 0xcc} )},
                new Object[] {shortValue( (short) 42 )},
                new Object[] {shortArray( new short[] {1337, (short) 0xcafe, (short) 0xbabe} )},
                new Object[] {intValue( 987654321 )},
                new Object[] {intArray( new int[] {42, 11} )},
                new Object[] {longValue( 9876543210L )},
                new Object[] {longArray( new long[] {0xcafebabe, 0x1ee7} )},
                new Object[] {floatValue( Float.MAX_VALUE )},
                new Object[] {floatArray( new float[] {Float.NEGATIVE_INFINITY, Float.MIN_VALUE} )},
                new Object[] {doubleValue( Double.MIN_NORMAL )},
                new Object[] {doubleArray( new double[] {Double.POSITIVE_INFINITY, Double.MAX_VALUE} )},
                new Object[] {datetime( 2018, 1, 16, 10, 36, 43, 123456788, ZoneId.of( "Europe/Stockholm" ) )},
                new Object[] {localDateTime( 2018, 1, 16, 10, 36, 43, 123456788 )},
                new Object[] {date( 2018, 1, 16 )},
                new Object[] {time( 10, 36, 43, 123456788, ZoneOffset.ofHours( 1 ) )},
                new Object[] {localTime( 10, 36, 43, 123456788 )},
                new Object[] {duration( 399, 4, 48424, 133701337 )},
                new Object[] {pointValue( Cartesian, 11, 32 )},
                new Object[] {
                        pointArray( new Point[] {pointValue( Cartesian, 11, 32 ), pointValue( WGS84, 13, 56 )} )} );
    }

    private final AnyValue value;

    public ValueMapperTest( AnyValue value )
    {
        this.value = value;
    }

    @Test
    public void shouldMapToJavaObject()
    {
        // given
        ValueMapper<Object> mapper = new Mapper();

        // when
        Object mapped = value.map( mapper );

        // then
        assertEquals( value, valueOf( mapped ) );
    }

    private static AnyValue valueOf( Object obj )
    {
        if ( obj instanceof MappedGraphType )
        {
            return ((MappedGraphType) obj).value;
        }
        Value value = Values.unsafeOf( obj, true );
        if ( value != null )
        {
            return value;
        }
        if ( obj instanceof List<?> )
        {
            return fromList( ((List<?>) obj).stream().map( ValueMapperTest::valueOf ).collect( toList() ) );
        }
        if ( obj instanceof Map<?,?> )
        {
            @SuppressWarnings( "unchecked" )
            Map<String,?> map = (Map<String,?>) obj;
            return map( map.entrySet().stream()
                    .map( e -> new Entry( e.getKey(), valueOf( e.getValue() ) ) )
                    .collect( toMap( e -> e.key, e -> e.value ) ) );
        }
        throw new AssertionError( "cannot convert: " + obj + " (a " + obj.getClass().getName() + ")" );
    }

    private static class Mapper extends ValueMapper.JavaMapper
    {
        @Override
        public Object mapPath( PathValue value )
        {
            return new MappedGraphType( value );
        }

        @Override
        public Object mapNode( VirtualNodeValue value )
        {
            return new MappedGraphType( value );
        }

        @Override
        public Object mapRelationship( VirtualRelationshipValue value )
        {
            return new MappedGraphType( value );
        }
    }

    private static class MappedGraphType
    {
        private final VirtualValue value;

        MappedGraphType( VirtualValue value )
        {

            this.value = value;
        }
    }

    private static class Entry
    {
        final String key;
        final AnyValue value;

        private Entry( String key, AnyValue value )
        {
            this.key = key;
            this.value = value;
        }
    }
}
