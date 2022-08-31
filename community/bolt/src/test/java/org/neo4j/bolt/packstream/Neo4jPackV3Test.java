/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.packstream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.StringValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.bolt.packstream.Neo4jPackV2.DATE_TIME_WITH_ZONE_NAME;
import static org.neo4j.bolt.packstream.Neo4jPackV2.DATE_TIME_WITH_ZONE_NAME_SIZE;
import static org.neo4j.bolt.packstream.Neo4jPackV2.DATE_TIME_WITH_ZONE_OFFSET;
import static org.neo4j.bolt.packstream.Neo4jPackV2.DATE_TIME_WITH_ZONE_OFFSET_SIZE;
import static org.neo4j.bolt.packstream.Neo4jPackV3.DATE_TIME_WITH_ZONE_NAME_UTC;
import static org.neo4j.bolt.packstream.Neo4jPackV3.DATE_TIME_WITH_ZONE_NAME_UTC_SIZE;
import static org.neo4j.bolt.packstream.Neo4jPackV3.DATE_TIME_WITH_ZONE_OFFSET_UTC;
import static org.neo4j.bolt.packstream.Neo4jPackV3.DATE_TIME_WITH_ZONE_OFFSET_UTC_SIZE;
import static org.neo4j.values.storable.DateTimeValue.datetime;

@ExtendWith( RandomExtension.class )
public class Neo4jPackV3Test
{

    @Inject
    public RandomSupport random;

    @ParameterizedTest
    @MethodSource( "offsetArguments" )
    void shouldPackLocalDateTimeWithTimeZoneOffset( ZonedDateTime zonedDateTime, long epochSeconds, long nanoSeconds, long offsetSeconds ) throws IOException
    {
        PackedOutputArray packedOutput = pack(datetime(zonedDateTime));

        Neo4jPack.Unpacker unpacker = new Neo4jPackV3().newUnpacker( new PackedInputArray( packedOutput.bytes() ) );

        assertEquals( DATE_TIME_WITH_ZONE_OFFSET_UTC_SIZE, unpacker.unpackStructHeader() );
        assertEquals( DATE_TIME_WITH_ZONE_OFFSET_UTC, ((LongValue) unpacker.unpack()).longValue() );
        assertEquals(epochSeconds, ((LongValue) unpacker.unpack()).longValue());
        assertEquals(nanoSeconds, ((LongValue) unpacker.unpack()).longValue());
        assertEquals(offsetSeconds, ((LongValue) unpacker.unpack()).longValue());
    }

    @ParameterizedTest
    @MethodSource( "zoneArguments" )
    void shouldPackLocalDateTimeWithTimeZoneId( ZonedDateTime zonedDateTime, long epochSeconds, long nanoSeconds, String zoneId ) throws IOException
    {
        PackedOutputArray packedOutput = pack(datetime(zonedDateTime));

        Neo4jPack.Unpacker unpacker = new Neo4jPackV3().newUnpacker( new PackedInputArray( packedOutput.bytes() ) );

        assertEquals( DATE_TIME_WITH_ZONE_NAME_UTC_SIZE, unpacker.unpackStructHeader() );
        assertEquals( DATE_TIME_WITH_ZONE_NAME_UTC, ((LongValue) unpacker.unpack()).longValue() );
        assertEquals(epochSeconds, ((LongValue) unpacker.unpack()).longValue());
        assertEquals(nanoSeconds, ((LongValue) unpacker.unpack()).longValue());
        assertEquals(zoneId, ((StringValue) unpacker.unpack()).stringValue());
    }
    @Test
    void shouldThrowIfNonUTCDateTimeOffsetProvided() throws IOException
    {
        Neo4jPackV3 neo4jPack = new Neo4jPackV3();
        PackedOutputArray output = new PackedOutputArray();

        Neo4jPack.Packer packer = neo4jPack.newPacker(output);
        packer.packStructHeader(DATE_TIME_WITH_ZONE_OFFSET_SIZE, DATE_TIME_WITH_ZONE_OFFSET);

        PackedInputArray input = new PackedInputArray( output.bytes() );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        var ex = assertThrows(BoltIOException.class, unpacker::unpack);
        assertThat( ex.getMessage() ).isEqualTo( "Unable to unpack struct: 70 when UTC DateTime has been negotiated." );
    }

    @Test
    void shouldThrowIfNonUTCDateTimeZoneProvided() throws IOException
    {
        Neo4jPackV3 neo4jPack = new Neo4jPackV3();
        PackedOutputArray output = new PackedOutputArray();

        Neo4jPack.Packer packer = neo4jPack.newPacker(output);
        packer.packStructHeader( DATE_TIME_WITH_ZONE_NAME_SIZE, DATE_TIME_WITH_ZONE_NAME );

        PackedInputArray input = new PackedInputArray( output.bytes() );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        var ex = assertThrows(BoltIOException.class, unpacker::unpack);
        assertThat( ex.getMessage() ).isEqualTo( "Unable to unpack struct: 102 when UTC DateTime has been negotiated." );
    }

    @Test
    void shouldPackAndUnpackLocalDateTimeWithZoneOffset()
    {
        DateTimeValue zonedDateTime = random.randomValues().nextDateTimeValue();

        var packedAndUnpacked = ((DateTimeValue) unpack(pack(zonedDateTime).bytes())).asObjectCopy();

        assertEquals(zonedDateTime.asObjectCopy(), packedAndUnpacked);
    }

    @Test
    void shouldPackAndUnpackLocalDateTimeWithZoneId()
    {
        DateTimeValue zonedDateTime = random.randomValues().nextDateTimeValue( ZoneId.systemDefault() );

        var packedAndUnpacked = ((DateTimeValue) unpack( pack( zonedDateTime ).bytes() ) ).asObjectCopy();

        assertEquals(zonedDateTime.asObjectCopy(), packedAndUnpacked);
    }

    private static Stream<Arguments> offsetArguments()
    {
        return Stream.of(
                Arguments.of(
                        ZonedDateTime.of( 1978, 12, 16, 10, 5, 59, 128000987, ZoneOffset.ofTotalSeconds( -150 * 60 ) ),
                        282659759,
                        128000987,
                        ZoneOffset.ofTotalSeconds( -150 * 60 ).getTotalSeconds() ),
                Arguments.of(
                        ZonedDateTime.of( 2022, 6, 14, 15, 21, 18, 183_000_000, ZoneOffset.ofTotalSeconds( 120 * 60 ) ),
                        1655212878,
                        183_000_000,
                        ZoneOffset.ofTotalSeconds( 120 * 60 ).getTotalSeconds() ),
                Arguments.of(
                        ZonedDateTime.of( 2020, 6, 15, 12, 30, 0, 42, ZoneOffset.ofTotalSeconds( -2 * 60 * 60 ) ),
                        1592231400,
                        42,
                        ZoneOffset.ofTotalSeconds( -2 * 60 * 60 ).getTotalSeconds() ) );
    }

    private static Stream<Arguments> zoneArguments()
    {
        return Stream.of(
                Arguments.of(
                        ZonedDateTime.of( 1978, 12, 16, 12, 35, 59, 128000987, ZoneId.of( "Europe/Istanbul" ) ),
                        282648959,
                        128000987,
                        "Europe/Istanbul" ),
                Arguments.of(
                        ZonedDateTime.of( 2022, 6, 14, 15, 21, 18, 183_000_000, ZoneId.of( "Europe/Berlin" ) ),
                        1655212878,
                        183_000_000L,
                        "Europe/Berlin" ),
                Arguments.of(
                        ZonedDateTime.of( 2022, 6, 14, 22, 6, 18, 183_000_000, ZoneId.of( "Australia/Eucla" ) ),
                        1655212878,
                        183_000_000L,
                        "Australia/Eucla" ),
                Arguments.of(
                        ZonedDateTime.of( 2020, 6, 15, 4, 30, 0, 183_000_000, ZoneId.of( "Pacific/Honolulu" ) ),
                        1592231400,
                        183_000_000L,
                        "Pacific/Honolulu" ) );
    }

    private static PackedOutputArray pack( AnyValue value )
    {
        try
        {
            Neo4jPackV3 neo4jPack = new Neo4jPackV3();
            PackedOutputArray output = new PackedOutputArray();
            Neo4jPack.Packer packer = neo4jPack.newPacker(output);
            packer.pack( value );
            return output;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @SuppressWarnings( "unchecked" )
    private static <T extends AnyValue> T unpack( byte[] output )
    {
        try
        {
            Neo4jPackV3 neo4jPack = new Neo4jPackV3();
            PackedInputArray input = new PackedInputArray( output );
            Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker(input);
            AnyValue unpack = unpacker.unpack();
            return (T) unpack;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
