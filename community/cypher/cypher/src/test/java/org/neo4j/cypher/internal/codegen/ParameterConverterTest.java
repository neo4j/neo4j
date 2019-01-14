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
package org.neo4j.cypher.internal.codegen;

import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.floatValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;
import static org.neo4j.values.virtual.VirtualValues.list;
import static org.neo4j.values.virtual.VirtualValues.map;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.path;

public class ParameterConverterTest
{
    private ParameterConverter converter = new ParameterConverter( mock( EmbeddedProxySPI.class ) );

    @Before
    public void setup()
    {
        EmbeddedProxySPI manager = mock( EmbeddedProxySPI.class );
        when( manager.newNodeProxy( anyLong() ) ).thenAnswer( invocationOnMock ->
        {
            long id = invocationOnMock.getArgument( 0 );
            NodeProxy mock = mock( NodeProxy.class );
            when( mock.getId() ).thenReturn( id );
            return mock;
        } );
        when( manager.newRelationshipProxy( anyLong() ) ).thenAnswer( invocationOnMock ->
        {
            long id = invocationOnMock.getArgument( 0 );
            RelationshipProxy mock = mock( RelationshipProxy.class );
            when( mock.getId() ).thenReturn( id );
            return mock;
        } );
        converter = new ParameterConverter( manager );
    }

    @Test
    public void shouldTurnAllIntegerTypesToLongs()
    {
        AnyValue[] values =
                new AnyValue[]{byteValue( (byte) 13 ), shortValue( (short) 13 ), intValue( 13 ), longValue( 13L )};

        for ( AnyValue val : values )
        {
            val.writeTo( converter );
            Object value = converter.value();
            assertThat( value, instanceOf( Long.class ) );
            assertThat( value, equalTo( 13L ) );
        }
    }

    @Test
    public void shouldTurnAllFloatingTypesToDoubles()
    {
        AnyValue[] values = new AnyValue[]{floatValue( 13f ), doubleValue( 13d )};

        for ( AnyValue val : values )
        {
            val.writeTo( converter );
            Object value = converter.value();
            assertThat( value, instanceOf( Double.class ) );
            assertThat( value, equalTo( 13d ) );
        }
    }

    @Test
    public void shouldHandleNodes()
    {
        // Given
        NodeValue nodeValue = nodeValue( 42L, stringArray( "L" ), EMPTY_MAP );

        // When
        nodeValue.writeTo( converter );

        // Then
        assertThat( converter.value(), equalTo( VirtualValues.node( 42L ) ) );
    }

    @Test
    public void shouldHandleRelationships()
    {
        // Given
        RelationshipValue relValue = relationshipValue( 1L, nodeValue( 42L, stringArray( "L" ), EMPTY_MAP ),
                nodeValue( 42L, stringArray( "L" ), EMPTY_MAP ), stringValue( "R" ), EMPTY_MAP );

        // When
        relValue.writeTo( converter );

        // Then
        assertThat( converter.value(), equalTo( VirtualValues.relationship( 1L ) ) );
    }

    @Test
    public void shouldHandleBooleans()
    {
        TRUE.writeTo( converter );
        assertThat( converter.value(), equalTo( true ) );
        FALSE.writeTo( converter );
        assertThat( converter.value(), equalTo( false ) );
    }

    @Test
    public void shouldHandlePaths()
    {
        // Given
        NodeValue n1 = nodeValue( 42L, stringArray( "L" ), EMPTY_MAP );
        NodeValue n2 = nodeValue( 43L, stringArray( "L" ), EMPTY_MAP );
        PathValue p = path(
                new NodeValue[]{n1, n2},
                new RelationshipValue[]{relationshipValue( 1L, n1, n2, stringValue( "T" ), EMPTY_MAP )} );

        // When
        p.writeTo( converter );

        // Then
        Object value = converter.value();
        assertThat( value, instanceOf( Path.class ) );
        Path path = (Path) value;
        assertThat( path.length(), equalTo( 1 ) );
        assertThat( path.startNode().getId(), equalTo( 42L ) );
        assertThat( path.endNode().getId(), equalTo( 43L ) );
        assertThat( path.relationships().iterator().next().getId(), equalTo( 1L ) );
    }

    @Test
    public void shouldHandlePoints()
    {
        // Given
        PointValue pointValue = Values.pointValue( CoordinateReferenceSystem.WGS84, 1.0, 2.0 );

        // When
        pointValue.writeTo( converter );

        // Then
        Object value = converter.value();
        assertThat( value, instanceOf( Point.class ) );
        Point point = (Point) value;
        assertThat( point.getCoordinate().getCoordinate().get( 0 ), equalTo( 1.0 ) );
        assertThat( point.getCoordinate().getCoordinate().get( 1 ), equalTo( 2.0 ) );
        assertThat( point.getCRS().getCode(), equalTo( 4326 ) );
    }

    @Test
    public void shouldHandleDateTimeWithZoneOffset()
    {
        // Given
        DateTimeValue dvalue = DateTimeValue.datetime( 1, 2, 3, 4, 5, 6, 7, "+00:00" );

        // When
        dvalue.writeTo( converter );

        // Then
        Object value = converter.value();
        assertThat( value, instanceOf( ZonedDateTime.class ) );
        assertThat( value, equalTo( dvalue.asObjectCopy() ) );
    }

    @Test
    public void shouldHandleDateTimeWithZoneId()
    {
        // Given
        DateTimeValue dvalue = DateTimeValue.datetime( 1, 2, 3, 4, 5, 6, 7, ZoneId.of( "Europe/Stockholm" ) );

        // When
        dvalue.writeTo( converter );

        // Then
        Object value = converter.value();
        assertThat( value, instanceOf( ZonedDateTime.class ) );
        assertThat( value, equalTo( dvalue.asObjectCopy() ) );
    }

    @Test
    public void shouldHandleLocalDateTime()
    {
        // Given
        LocalDateTimeValue dvalue = LocalDateTimeValue.localDateTime( 1, 2, 3, 4, 5, 6, 7 );

        // When
        dvalue.writeTo( converter );

        // Then
        Object value = converter.value();
        assertThat( value, instanceOf( LocalDateTime.class ) );
        assertThat( value, equalTo( dvalue.asObjectCopy() ) );
    }

    @Test
    public void shouldHandleDate()
    {
        // Given
        DateValue dvalue = DateValue.date( 1, 2, 3 );

        // When
        dvalue.writeTo( converter );

        // Then
        Object value = converter.value();
        assertThat( value, instanceOf( LocalDate.class ) );
        assertThat( value, equalTo( dvalue.asObjectCopy() ) );
    }

    @Test
    public void shouldHandleTimeUTC()
    {
        // Given
        TimeValue time = TimeValue.time( 1, 2, 3, 4, "+00:00" );

        // When
        time.writeTo( converter );

        // Then
        Object value = converter.value();
        assertThat( value, instanceOf( OffsetTime.class ) );
        assertThat( value, equalTo( time.asObjectCopy() ) );
    }

    @Test
    public void shouldHandleTimeWithOffset()
    {
        // Given
        TimeValue time = TimeValue.time( 1, 2, 3, 4, "+01:00" );

        // When
        time.writeTo( converter );

        // Then
        Object value = converter.value();
        assertThat( value, instanceOf( OffsetTime.class ) );
        assertThat( value, equalTo( time.asObjectCopy() ) );
    }

    @Test
    public void shouldHandleLocalTime()
    {
        // Given
        LocalTimeValue dvalue = LocalTimeValue.localTime( 1, 2, 3, 4 );

        // When
        dvalue.writeTo( converter );

        // Then
        Object value = converter.value();
        assertThat( value, instanceOf( LocalTime.class ) );
        assertThat( value, equalTo( dvalue.asObjectCopy() ) );
    }

    @Test
    public void shouldHandleDurations()
    {
        // Given
        DurationValue dvalue = DurationValue.duration( 1, 2, 3, 4 );

        // When
        dvalue.writeTo( converter );

        // Then
        Object value = converter.value();
        assertThat( value, instanceOf( DurationValue.class ) );
        assertThat( value, equalTo( dvalue.asObjectCopy() ) );
    }

    @Test
    public void shouldHandleLists()
    {
        // Given
        ListValue list = list( stringValue( "foo" ), longValue( 42L ), TRUE );

        // When
        list.writeTo( converter );

        // Then
        assertThat( converter.value(), equalTo( Arrays.asList( "foo", 42L, true ) ) );
    }

    @Test
    public void shouldHandleArrays()
    {
        // Given
        LongArray longArray = Values.longArray( new long[]{1L, 2L, 3L} );

        // When
        longArray.writeTo( converter );

        // Then
        assertThat( converter.value(), equalTo( new long[]{1L, 2L, 3L} ) );
    }

    @Test
    public void shouldHandleMaps()
    {
        // Given
        MapValue map = map( new String[]{"foo", "bar"}, new AnyValue[]{longValue( 42L ), stringValue( "baz" )} );

        // When
        map.writeTo( converter );

        // Then
        assertThat( converter.value(), equalTo( MapUtil.map( "foo", 42L, "bar", "baz" ) ) );
    }

    @Test
    public void shouldHandleListWithMaps()
    {
        // Given
        ListValue list =
                list( longValue( 42L ),
                        map( new String[]{"foo", "bar"}, new AnyValue[]{longValue( 42L ), stringValue( "baz" )} ) );

        // When
        list.writeTo( converter );

        // Then
        List<?> converted = (List<?>) converter.value();
        assertThat( converted.get( 0 ), equalTo( 42L ) );
        assertThat( converted.get( 1 ), equalTo( MapUtil.map( "foo", 42L, "bar", "baz" ) ) );
    }

    @Test
    public void shouldHandleMapsWithLists()
    {
        // Given
        MapValue map =
                map( new String[]{"foo", "bar"}, new AnyValue[]{longValue( 42L ), list( stringValue( "baz" ) )} );

        // When
        map.writeTo( converter );

        // Then
        Map<?,?> value = (Map<?,?>) converter.value();
        assertThat( value.get( "foo" ), equalTo( 42L ) );
        assertThat( value.get( "bar" ), equalTo( singletonList( "baz" ) ) );
        assertThat( value.size(), equalTo( 2 ) );
    }
}
