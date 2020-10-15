/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.http.cypher.format.jolt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.when;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
class JoltSerializerTest
{

    private final ObjectMapper objectMapper;

    JoltSerializerTest()
    {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule( JoltModule.STRICT.getInstance() );
    }

    @Nested
    class SparseMode
    {
        private ObjectMapper spareObjectMapper;

        SparseMode()
        {
            this.spareObjectMapper = new ObjectMapper();
            this.spareObjectMapper.registerModule( JoltModule.DEFAULT.getInstance() );
        }

        @Test
        void shouldUseJSONString() throws JsonProcessingException
        {
            spareObjectMapper = new ObjectMapper();
            var result = spareObjectMapper.writeValueAsString( "Hello, World" );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "\"Hello, World\"" );
        }

        @Test
        void shouldUseJSONBoolean() throws JsonProcessingException
        {
            spareObjectMapper = new ObjectMapper();
            var result = spareObjectMapper.writeValueAsString( true );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "true" );
        }

        @Test
        void shouldUseJSONList() throws JsonProcessingException
        {
            var result = spareObjectMapper.writeValueAsString( List.of( 1, 2, "3" ) );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "[1,2,\"3\"]" );
        }
    }

    @Nested
    class SimpleTypes
    {

        @Test
        void shouldSerializeNull() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( null );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "null" );
        }

        @Test
        void shouldSerializeInteger() throws JsonProcessingException
        {

            var result = objectMapper.writeValueAsString( 123 );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"Z\":\"123\"}" );
        }

        @Test
        void shouldSerializeBoolean() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( true );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"?\":\"true\"}" );
        }

        @Test
        void shouldSerializeLongInsideInt32Range() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( 123L );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"Z\":\"123\"}" );
        }

        @Test
        void shouldSerializeLongAboveInt32Range() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( (long) Integer.MAX_VALUE + 1 );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"R\":\"2147483648\"}" );
        }

        @Test
        void shouldSerializeLongBelowInt32Range() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( (long) Integer.MIN_VALUE - 1 );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"R\":\"-2147483649\"}" );
        }

        @Test
        void shouldSerializeDouble() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( 42.23 );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"R\":\"42.23\"}" );
        }

        @Test
        void shouldSerializeString() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( "Hello, World" );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"U\":\"Hello, World\"}" );
        }

        @Test
        void shouldSerializePoint() throws JsonProcessingException
        {
            var point = Values.pointValue( CoordinateReferenceSystem.WGS84, 12.994823, 55.612191 );
            var result = objectMapper.writeValueAsString( point );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"@\":\"SRID=4326;POINT(12.994823 55.612191)\"}" );
        }
    }

    @Nested
    class DateTimeDuration
    {
        @Test
        void shouldSerializeDuration() throws JsonProcessingException
        {
            var duration = DurationValue.duration( Duration.ofDays( 20 ) );
            var result = objectMapper.writeValueAsString( duration );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"T\":\"PT480H\"}" );
        }

        @Test
        void shouldSerializeLargeDuration() throws JsonProcessingException
        {
            var durationString = "P3Y6M4DT12H30M5S";
            var durationValue = DurationValue.parse( durationString );
            var result = objectMapper.writeValueAsString( durationValue );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"T\":\"" + durationString + "\"}" );
        }

        @Test
        void shouldSerializeDate() throws JsonProcessingException
        {
            var dateString = "2020-08-25";
            var date = LocalDate.parse( dateString );
            var result = objectMapper.writeValueAsString( date );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"T\":\"" + dateString + "\"}" );
        }

        @Test
        void shouldSerializeTime() throws JsonProcessingException
        {
            var timeString = "12:52:58.513775";
            var time = LocalTime.parse( timeString );
            var result = objectMapper.writeValueAsString( time );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"T\":\"" + timeString + "\"}" );
        }

        @Test
        void shouldSerializeOffsetTime() throws JsonProcessingException
        {
            var offsetTimeString = "12:55:10.775607+01:00";
            var time = OffsetTime.parse( offsetTimeString );
            var result = objectMapper.writeValueAsString( time );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"T\":\"" + offsetTimeString + "\"}" );
        }

        @Test
        void shouldSerializeLocalDateTime() throws JsonProcessingException
        {
            var localDateTimeString = "2020-08-25T12:57:36.069665";
            var dateTime = LocalDateTime.parse( localDateTimeString );
            var result = objectMapper.writeValueAsString( dateTime );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"T\":\"" + localDateTimeString + "\"}" );
        }

        @Test
        void shouldSerializeZonedDateTime() throws JsonProcessingException
        {
            var zonedDateTimeString = "2020-08-25T13:03:39.11733+01:00[Europe/London]";
            var dateTime = ZonedDateTime.parse( zonedDateTimeString );
            var result = objectMapper.writeValueAsString( dateTime );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"T\":\"" + zonedDateTimeString + "\"}" );
        }
    }

    @Nested
    class Arrays
    {

        @Test
        void shouldSerializeLongArray() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( new Long[]{0L, 1L, 2L} );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "[{\"Z\":\"0\"},{\"Z\":\"1\"},{\"Z\":\"2\"}]" );
        }

        @Test
        void shouldSerializeByteArray() throws JsonProcessingException
        {
            var result = objectMapper
                    .writeValueAsString( new byte[]{0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16} );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"#\":\"0001020304050608090A0B0C0D0E0F10\"}" );
        }
    }

    @Nested
    class Collections
    {
        @Test
        void shouldSerializeArrays() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( new String[]{"A", "B"} );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "[{\"U\":\"A\"},{\"U\":\"B\"}]" );
        }

        @Test
        void shouldSerializeHomogenousList() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( List.of( 1, 2, 3 ) );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"[]\":[{\"Z\":\"1\"},{\"Z\":\"2\"},{\"Z\":\"3\"}]}" );
        }

        @Test
        void shouldSerializeHeterogeneousList() throws JsonProcessingException
        {
            var result = objectMapper.writeValueAsString( List.of( "A", 21, 42.3 ) );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"[]\":[{\"U\":\"A\"},{\"Z\":\"21\"},{\"R\":\"42.3\"}]}" );
        }

        @Test
        void shouldSerializeMap() throws JsonProcessingException
        {
            // Treemap only created to have a stable iterator for a non flaky test ;)
            var result = objectMapper.writeValueAsString( new TreeMap<>( Map.of( "name", "Alice", "age", 33 ) ) );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"{}\":{\"age\":{\"Z\":\"33\"},\"name\":{\"U\":\"Alice\"}}}" );
        }
    }

    @Nested
    @ExtendWith( MockitoExtension.class )
    class Entities
    {
        @Test
        void shouldSerializeNode( @Mock Node node ) throws JsonProcessingException
        {
            when( node.getId() ).thenReturn( 4711L );
            when( node.getLabels() ).thenReturn( List.of( Label.label( "A" ), Label.label( "B" ) ) );
            when( node.getAllProperties() ).thenReturn( new TreeMap<>( Map.of( "prop1", 1, "prop2", "Peng" ) ) );
            var result = objectMapper.writeValueAsString( node );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"()\":[4711,[\"A\",\"B\"],{\"prop1\":{\"Z\":\"1\"},\"prop2\":{\"U\":\"Peng\"}}]}" );
        }

        @Test
        void shouldSerializeNodeWithoutLabelsOrProperties( @Mock Node node ) throws JsonProcessingException
        {
            when( node.getId() ).thenReturn( 4711L );
            when( node.getLabels() ).thenReturn( List.of() );
            var result = objectMapper.writeValueAsString( node );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"()\":[4711,[],{}]}" );
        }

        @Test
        void shouldSerializeRelationship( @Mock Relationship relationship ) throws JsonProcessingException
        {
            when( relationship.getId() ).thenReturn( 4711L );
            when( relationship.getType() ).thenReturn( RelationshipType.withName( "KNOWS" ) );
            when( relationship.getStartNodeId() ).thenReturn( 123L );
            when( relationship.getEndNodeId() ).thenReturn( 124L );
            when( relationship.getAllProperties() ).thenReturn( Map.of( "since", 1999 ) );

            var result = objectMapper.writeValueAsString( relationship );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"->\":[4711,123,\"KNOWS\",124,{\"since\":{\"Z\":\"1999\"}}]}" );
        }

        @Test
        void shouldSerializePath( @Mock Path path, @Mock Node start, @Mock Relationship rel,
                                  @Mock Node end ) throws JsonProcessingException
        {
            when( start.getId() ).thenReturn( 111L );
            when( start.getLabels() ).thenReturn( List.of() );

            when( end.getId() ).thenReturn( 222L );
            when( end.getLabels() ).thenReturn( List.of() );

            when( rel.getId() ).thenReturn( 9090L );
            when( rel.getType() ).thenReturn( RelationshipType.withName( "KNOWS" ) );
            when( rel.getStartNodeId() ).thenReturn( 111L );
            when( rel.getEndNodeId() ).thenReturn( 222L );
            when( rel.getAllProperties() ).thenReturn( Map.of( "since", 1999 ) );

            List<Entity> pathList = List.of( start, rel, end );

            when( path.iterator() ).thenReturn( pathList.iterator() );

            var result = objectMapper.writeValueAsString( path );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"..\":[" +
                                            "{\"()\":[111,[],{}]}," +
                                            "{\"->\":[9090,111,\"KNOWS\",222,{\"since\":{\"Z\":\"1999\"}}]}," +
                                            "{\"()\":[222,[],{}]}]}" );
        }

        @Test
        void shouldSerializeReversedPath( @Mock Path path, @Mock Node start, @Mock Relationship rel,
                                          @Mock Node end ) throws JsonProcessingException
        {
            when( start.getId() ).thenReturn( 111L );
            when( start.getLabels() ).thenReturn( List.of() );

            when( end.getId() ).thenReturn( 222L );
            when( end.getLabels() ).thenReturn( List.of() );

            when( rel.getId() ).thenReturn( 9090L );
            when( rel.getType() ).thenReturn( RelationshipType.withName( "KNOWS" ) );
            when( rel.getStartNodeId() ).thenReturn( 222L );
            when( rel.getEndNodeId() ).thenReturn( 111L );
            when( rel.getAllProperties() ).thenReturn( Map.of( "since", 1999 ) );

            List<Entity> pathList = List.of( start, rel, end );

            when( path.iterator() ).thenReturn( pathList.iterator() );

            var result = objectMapper.writeValueAsString( path );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"..\":[" +
                                            "{\"()\":[111,[],{}]}," +
                                            "{\"<-\":[9090,111,\"KNOWS\",222,{\"since\":{\"Z\":\"1999\"}}]}," +
                                            "{\"()\":[222,[],{}]}]}" );
        }

        @Test
        void shouldSerializeLongPath( @Mock Path path, @Mock Node start, @Mock Relationship relA,
                                      @Mock Node middle, @Mock Relationship relB, @Mock Node end ) throws JsonProcessingException
        {
            when( start.getId() ).thenReturn( 111L );
            when( start.getLabels() ).thenReturn( List.of() );
            when( middle.getId() ).thenReturn( 222L );
            when( middle.getLabels() ).thenReturn( List.of() );
            when( end.getId() ).thenReturn( 333L );
            when( end.getLabels() ).thenReturn( List.of() );

            when( relA.getId() ).thenReturn( 9090L );
            when( relA.getType() ).thenReturn( RelationshipType.withName( "KNOWS" ) );
            when( relA.getStartNodeId() ).thenReturn( 111L );
            when( relA.getEndNodeId() ).thenReturn( 222L );
            when( relA.getAllProperties() ).thenReturn( Map.of( "since", 1999 ) );

            when( relB.getId() ).thenReturn( 9090L );
            when( relB.getType() ).thenReturn( RelationshipType.withName( "KNOWS" ) );
            when( relB.getStartNodeId() ).thenReturn( 333L );
            when( relB.getEndNodeId() ).thenReturn( 222L );
            when( relB.getAllProperties() ).thenReturn( Map.of( "since", 1990 ) );

            List<Entity> pathList = List.of( start, relA, middle, relB, end );

            when( path.iterator() ).thenReturn( pathList.iterator() );

            var result = objectMapper.writeValueAsString( path );
            assertValidJSON( result );
            assertThat( result ).isEqualTo( "{\"..\":[" +
                                            "{\"()\":[111,[],{}]}," +
                                            "{\"->\":[9090,111,\"KNOWS\",222,{\"since\":{\"Z\":\"1999\"}}]}," +
                                            "{\"()\":[222,[],{}]}," +
                                            "{\"<-\":[9090,222,\"KNOWS\",333,{\"since\":{\"Z\":\"1990\"}}]}," +
                                            "{\"()\":[333,[],{}]}]}" );
        }
    }

    public void assertValidJSON( final String json )
    {
        try
        {
            objectMapper.readTree( json );
        }
        catch ( JsonProcessingException e )
        {
            fail( "Invalid JSON: ", json );
        }
    }
}
