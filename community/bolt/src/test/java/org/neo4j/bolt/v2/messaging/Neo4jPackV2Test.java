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
package org.neo4j.bolt.v2.messaging;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.packstream.PackedInputArray;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.virtual.ListValue;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.bolt.v1.packstream.PackStream.INT_16;
import static org.neo4j.bolt.v1.packstream.PackStream.INT_32;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.unsafePointValue;
import static org.neo4j.values.virtual.VirtualValues.list;

public class Neo4jPackV2Test
{
    private static final String[] TIME_ZONE_NAMES =
            TimeZones.supportedTimeZones().stream()
                    .filter( s -> ZoneId.getAvailableZoneIds().contains( s ) )
                    .toArray( String[]::new );

    private static final int RANDOM_VALUES_TO_TEST = 1_000;
    private static final int RANDOM_LISTS_TO_TEST = 1_000;
    private static final int RANDOM_LIST_MAX_SIZE = 500;

    @Rule
    public RandomRule random = new RandomRule();

    @Test
    public void shouldFailToPackPointWithIllegalDimensions()
    {
        testPackingPointsWithWrongDimensions( 0 );
        testPackingPointsWithWrongDimensions( 1 );
        testPackingPointsWithWrongDimensions( 4 );
        testPackingPointsWithWrongDimensions( 100 );
    }

    @Test
    public void shouldFailToUnpack2DPointWithIncorrectCoordinate() throws IOException
    {
        Neo4jPackV2 neo4jPack = new Neo4jPackV2();
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );

        packer.packStructHeader( 3, Neo4jPackV2.POINT_2D );
        packer.pack( intValue( WGS84.getCode() ) );
        packer.pack( doubleValue( 42.42 ) );

        try
        {
            unpack( output );
            fail( "Exception expected" );
        }
        catch ( UncheckedIOException ignore )
        {
        }
    }

    @Test
    public void shouldFailToUnpack3DPointWithIncorrectCoordinate() throws IOException
    {
        Neo4jPackV2 neo4jPack = new Neo4jPackV2();
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );

        packer.packStructHeader( 4, Neo4jPackV2.POINT_3D );
        packer.pack( intValue( Cartesian.getCode() ) );
        packer.pack( doubleValue( 1.0 ) );
        packer.pack( doubleValue( 100.1 ) );

        try
        {
            unpack( output );
            fail( "Exception expected" );
        }
        catch ( UncheckedIOException ignore )
        {
        }
    }

    @Test
    public void shouldPackAndUnpack2DPoints()
    {
        testPackingAndUnpacking( this::randomPoint2D );
    }

    @Test
    public void shouldPackAndUnpack3DPoints()
    {
        testPackingAndUnpacking( this::randomPoint3D );
    }

    @Test
    public void shouldPackAndUnpackListsOf2DPoints()
    {
        testPackingAndUnpacking( () -> randomList( this::randomPoint2D ) );
    }

    @Test
    public void shouldPackAndUnpackListsOf3DPoints()
    {
        testPackingAndUnpacking( () -> randomList( this::randomPoint3D ) );
    }

    @Test
    public void shouldPackAndUnpackDuration()
    {
        testPackingAndUnpacking( this::randomDuration );
    }

    @Test
    public void shouldPackAndUnpackPeriod()
    {
        testPackingAndUnpacking( this::randomPeriod );
    }

    @Test
    public void shouldPackAndUnpackListsOfDuration()
    {
        testPackingAndUnpacking( () -> randomList( this::randomDuration ) );
    }

    @Test
    public void shouldPackAndUnpackDate()
    {
        testPackingAndUnpacking( this::randomDate );
    }

    @Test
    public void shouldPackAndUnpackListsOfDate()
    {
        testPackingAndUnpacking( () -> randomList( this::randomDate ) );
    }

    @Test
    public void shouldPackAndUnpackLocalTime()
    {
        testPackingAndUnpacking( this::randomLocalTime );
    }

    @Test
    public void shouldPackAndUnpackListsOfLocalTime()
    {
        testPackingAndUnpacking( () -> randomList( this::randomLocalTime ) );
    }

    @Test
    public void shouldPackAndUnpackTime()
    {
        testPackingAndUnpacking( this::randomTime );
    }

    @Test
    public void shouldPackAndUnpackListsOfTime()
    {
        testPackingAndUnpacking( () -> randomList( this::randomTime ) );
    }

    @Test
    public void shouldPackAndUnpackLocalDateTime()
    {
        testPackingAndUnpacking( this::randomLocalDateTime );
    }

    @Test
    public void shouldPackAndUnpackListsOfLocalDateTime()
    {
        testPackingAndUnpacking( () -> randomList( this::randomLocalDateTime ) );
    }

    @Test
    public void shouldPackAndUnpackDateTimeWithTimeZoneName()
    {
        testPackingAndUnpacking( this::randomDateTimeWithTimeZoneName );
    }

    @Test
    public void shouldPackAndUnpackListsOfDateTimeWithTimeZoneName()
    {
        testPackingAndUnpacking( () -> randomList( this::randomDateTimeWithTimeZoneName ) );
    }

    @Test
    public void shouldPackAndUnpackDateTimeWithTimeZoneOffset()
    {
        testPackingAndUnpacking( this::randomDateTimeWithTimeZoneOffset );
    }

    @Test
    public void shouldPackAndUnpackListsOfDateTimeWithTimeZoneOffset()
    {
        testPackingAndUnpacking( () -> randomList( this::randomDateTimeWithTimeZoneOffset ) );
    }

    @Test
    public void shouldPackLocalDateTimeWithTimeZoneOffset()
    {
        LocalDateTime localDateTime = LocalDateTime.of( 2015, 3, 23, 19, 15, 59, 10 );
        ZoneOffset offset = ZoneOffset.ofHoursMinutes( -5, -15 );
        ZonedDateTime zonedDateTime = ZonedDateTime.of( localDateTime, offset );

        PackedOutputArray packedOutput = pack( datetime( zonedDateTime ) );
        ByteBuffer buffer = ByteBuffer.wrap( packedOutput.bytes() );

        buffer.getShort(); // skip struct header
        assertEquals( INT_32, buffer.get() );
        assertEquals( localDateTime.toEpochSecond( UTC ), buffer.getInt() );
        assertEquals( localDateTime.getNano(), buffer.get() );
        assertEquals( INT_16, buffer.get() );
        assertEquals( offset.getTotalSeconds(), buffer.getShort() );
    }

    @Test
    public void shouldPackLocalDateTimeWithTimeZoneId()
    {
        LocalDateTime localDateTime = LocalDateTime.of( 1999, 12, 30, 9, 49, 20, 999999999 );
        ZoneId zoneId = ZoneId.of( "Europe/Stockholm" );
        ZonedDateTime zonedDateTime = ZonedDateTime.of( localDateTime, zoneId );

        PackedOutputArray packedOutput = pack( datetime( zonedDateTime ) );
        ByteBuffer buffer = ByteBuffer.wrap( packedOutput.bytes() );

        buffer.getShort(); // skip struct header
        assertEquals( INT_32, buffer.get() );
        assertEquals( localDateTime.toEpochSecond( UTC ), buffer.getInt() );
        assertEquals( INT_32, buffer.get() );
        assertEquals( localDateTime.getNano(), buffer.getInt() );
        buffer.getShort(); // skip zoneId string header
        byte[] zoneIdBytes = new byte[zoneId.getId().getBytes( UTF_8 ).length];
        buffer.get( zoneIdBytes );
        assertEquals( zoneId.getId(), new String( zoneIdBytes, UTF_8 ) );
    }

    private static <T extends AnyValue> void testPackingAndUnpacking( Supplier<T> randomValueGenerator )
    {
        testPackingAndUnpacking( index -> randomValueGenerator.get() );
    }

    private static <T extends AnyValue> void testPackingAndUnpacking( Function<Integer,T> randomValueGenerator )
    {
        IntStream.range( 0, RANDOM_VALUES_TO_TEST )
                .mapToObj( randomValueGenerator::apply )
                .forEach( originalValue ->
                {
                    T unpackedValue = packAndUnpack( originalValue );
                    assertEquals( originalValue, unpackedValue );
                } );
    }

    private void testPackingPointsWithWrongDimensions( int dimensions )
    {
        PointValue point = randomPoint( 0, dimensions );
        try
        {
            pack( point );
            fail( "Exception expected" );
        }
        catch ( IllegalArgumentException ignore )
        {
        }
    }

    private static <T extends AnyValue> T packAndUnpack( T value )
    {
        return unpack( pack( value ) );
    }

    private static PackedOutputArray pack( AnyValue value )
    {
        try
        {
            Neo4jPackV2 neo4jPack = new Neo4jPackV2();
            PackedOutputArray output = new PackedOutputArray();
            Neo4jPack.Packer packer = neo4jPack.newPacker( output );
            packer.pack( value );
            return output;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @SuppressWarnings( "unchecked" )
    private static <T extends AnyValue> T unpack( PackedOutputArray output )
    {
        try
        {
            Neo4jPackV2 neo4jPack = new Neo4jPackV2();
            PackedInputArray input = new PackedInputArray( output.bytes() );
            Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );
            AnyValue unpack = unpacker.unpack();
            return (T) unpack;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private <T extends AnyValue> ListValue randomList( Supplier<T> randomValueGenerator )
    {
        return randomList( index -> randomValueGenerator.get() );
    }

    private <T extends AnyValue> ListValue randomList( Function<Integer,T> randomValueGenerator )
    {
        AnyValue[] values = random.ints( RANDOM_LISTS_TO_TEST, 1, RANDOM_LIST_MAX_SIZE )
                .mapToObj( randomValueGenerator::apply )
                .toArray( AnyValue[]::new );

        return list( values );
    }

    private PointValue randomPoint2D( int index )
    {
        return randomPoint( index, 2 );
    }

    private PointValue randomPoint3D( int index )
    {
        return randomPoint( index, 3 );
    }

    private PointValue randomPoint( int index, int dimension )
    {
        CoordinateReferenceSystem crs;
        if ( index % 2 == 0 )
        {
            crs = dimension == 2 ? WGS84 : WGS84_3D;
        }
        else
        {
            crs = dimension == 2 ? Cartesian : Cartesian_3D;
        }

        return unsafePointValue( crs, random.doubles( dimension, Double.MIN_VALUE, Double.MAX_VALUE ).toArray() );
    }

    private DurationValue randomDuration()
    {
        return duration( random.randoms().randomDuration() );
    }

    private DurationValue randomPeriod()
    {
        return duration( random.randoms().randomPeriod() );
    }

    private DateValue randomDate()
    {
        return date( random.randoms().randomDate() );
    }

    private LocalTimeValue randomLocalTime()
    {
        return localTime( random.randoms().randomLocalTime() );
    }

    private TimeValue randomTime()
    {
        return TimeValue.time( random.randoms().randomTime() );
    }

    private LocalDateTimeValue randomLocalDateTime()
    {
        return localDateTime( random.randoms().randomLocalDateTime() );
    }

    private DateTimeValue randomDateTimeWithTimeZoneName()
    {
        return datetime( random.randoms().randomDateTime( randomZoneIdWithName() ) );
    }

    private DateTimeValue randomDateTimeWithTimeZoneOffset()
    {
        return datetime( random.randoms().randomDateTime( randomZoneOffset() ) );
    }

    private ZoneOffset randomZoneOffset()
    {
        return ZoneOffset.ofTotalSeconds( random.nextInt( ZoneOffset.MIN.getTotalSeconds(), ZoneOffset.MAX.getTotalSeconds() ) );
    }

    private ZoneId randomZoneIdWithName()
    {
        String timeZoneName = TIME_ZONE_NAMES[random.nextInt( TIME_ZONE_NAMES.length )];
        return ZoneId.of( timeZoneName );
    }
}
