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

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

import org.neo4j.bolt.messaging.StructType;
import org.neo4j.bolt.v1.messaging.BoltIOException;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.packstream.PackInput;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.packstream.PackStream;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.utils.TemporalUtil;

import static java.time.ZoneOffset.UTC;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.pointValue;

public class Neo4jPackV2 extends Neo4jPackV1
{
    public static final long VERSION = 2;

    public static final byte POINT_2D = 'X';
    public static final int POINT_2D_SIZE = 3;

    public static final byte POINT_3D = 'Y';
    public static final int POINT_3D_SIZE = 4;

    public static final byte DATE = 'D';
    public static final int DATE_SIZE = 1;

    public static final byte TIME = 'T';
    public static final int TIME_SIZE = 2;

    public static final byte LOCAL_TIME = 't';
    public static final int LOCAL_TIME_SIZE = 1;

    public static final byte LOCAL_DATE_TIME = 'd';
    public static final int LOCAL_DATE_TIME_SIZE = 2;

    public static final byte DATE_TIME_WITH_ZONE_OFFSET = 'F';
    public static final int DATE_TIME_WITH_ZONE_OFFSET_SIZE = 3;

    public static final byte DATE_TIME_WITH_ZONE_NAME = 'f';
    public static final int DATE_TIME_WITH_ZONE_NAME_SIZE = 3;

    public static final byte DURATION = 'E';
    public static final int DURATION_SIZE = 4;

    @Override
    public Neo4jPack.Packer newPacker( PackOutput output )
    {
        return new PackerV2( output );
    }

    @Override
    public Neo4jPack.Unpacker newUnpacker( PackInput input )
    {
        return new UnpackerV2( input );
    }

    @Override
    public long version()
    {
        return VERSION;
    }

    private static class PackerV2 extends Neo4jPackV1.PackerV1
    {
        PackerV2( PackOutput output )
        {
            super( output );
        }

        @Override
        public void writePoint( CoordinateReferenceSystem crs, double[] coordinate ) throws IOException
        {
            if ( coordinate.length == 2 )
            {
                packStructHeader( POINT_2D_SIZE, POINT_2D );
                pack( crs.getCode() );
                pack( coordinate[0] );
                pack( coordinate[1] );
            }
            else if ( coordinate.length == 3 )
            {
                packStructHeader( POINT_3D_SIZE, POINT_3D );
                pack( crs.getCode() );
                pack( coordinate[0] );
                pack( coordinate[1] );
                pack( coordinate[2] );
            }
            else
            {
                throw new IllegalArgumentException( "Point with 2D or 3D coordinate expected, " +
                                                    "got crs=" + crs + ", coordinate=" + Arrays.toString( coordinate ) );
            }
        }

        @Override
        public void writeDuration( long months, long days, long seconds, int nanos ) throws IOException
        {
            packStructHeader( DURATION_SIZE, DURATION );
            pack( months );
            pack( days );
            pack( seconds );
            pack( nanos );
        }

        @Override
        public void writeDate( LocalDate localDate ) throws IOException
        {
            long epochDay = localDate.toEpochDay();

            packStructHeader( DATE_SIZE, DATE );
            pack( epochDay );
        }

        @Override
        public void writeLocalTime( LocalTime localTime ) throws IOException
        {
            long nanoOfDay = localTime.toNanoOfDay();

            packStructHeader( LOCAL_TIME_SIZE, LOCAL_TIME );
            pack( nanoOfDay );
        }

        @Override
        public void writeTime( OffsetTime offsetTime ) throws IOException
        {
            long nanosOfDayLocal = offsetTime.toLocalTime().toNanoOfDay();
            int offsetSeconds = offsetTime.getOffset().getTotalSeconds();

            packStructHeader( TIME_SIZE, TIME );
            pack( nanosOfDayLocal );
            pack( offsetSeconds );
        }

        @Override
        public void writeLocalDateTime( LocalDateTime localDateTime ) throws IOException
        {
            long epochSecond = localDateTime.toEpochSecond( UTC );
            int nano = localDateTime.getNano();

            packStructHeader( LOCAL_DATE_TIME_SIZE, LOCAL_DATE_TIME );
            pack( epochSecond );
            pack( nano );
        }

        @Override
        public void writeDateTime( ZonedDateTime zonedDateTime ) throws IOException
        {
            long epochSecondLocal = zonedDateTime.toLocalDateTime().toEpochSecond( UTC );
            int nano = zonedDateTime.getNano();

            ZoneId zone = zonedDateTime.getZone();
            if ( zone instanceof ZoneOffset )
            {
                int offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();

                packStructHeader( DATE_TIME_WITH_ZONE_OFFSET_SIZE, DATE_TIME_WITH_ZONE_OFFSET );
                pack( epochSecondLocal );
                pack( nano );
                pack( offsetSeconds );
            }
            else
            {
                String zoneId = zone.getId();

                packStructHeader( DATE_TIME_WITH_ZONE_NAME_SIZE, DATE_TIME_WITH_ZONE_NAME );
                pack( epochSecondLocal );
                pack( nano );
                pack( zoneId );
            }
        }
    }

    private static class UnpackerV2 extends Neo4jPackV1.UnpackerV1
    {
        UnpackerV2( PackInput input )
        {
            super( input );
        }

        @Override
        protected AnyValue unpackStruct( char signature, long size ) throws IOException
        {
            try
            {
                switch ( signature )
                {
                case POINT_2D:
                    ensureCorrectStructSize( StructType.POINT_2D, POINT_2D_SIZE, size );
                    return unpackPoint2D();
                case POINT_3D:
                    ensureCorrectStructSize( StructType.POINT_3D, POINT_3D_SIZE, size );
                    return unpackPoint3D();
                case DURATION:
                    ensureCorrectStructSize( StructType.DURATION, DURATION_SIZE, size );
                    return unpackDuration();
                case DATE:
                    ensureCorrectStructSize( StructType.DATE, DATE_SIZE, size );
                    return unpackDate();
                case LOCAL_TIME:
                    ensureCorrectStructSize( StructType.LOCAL_TIME, LOCAL_TIME_SIZE, size );
                    return unpackLocalTime();
                case TIME:
                    ensureCorrectStructSize( StructType.TIME, TIME_SIZE, size );
                    return unpackTime();
                case LOCAL_DATE_TIME:
                    ensureCorrectStructSize( StructType.LOCAL_DATE_TIME, LOCAL_DATE_TIME_SIZE, size );
                    return unpackLocalDateTime();
                case DATE_TIME_WITH_ZONE_OFFSET:
                    ensureCorrectStructSize( StructType.DATE_TIME_WITH_ZONE_OFFSET, DATE_TIME_WITH_ZONE_OFFSET_SIZE, size );
                    return unpackDateTimeWithZoneOffset();
                case DATE_TIME_WITH_ZONE_NAME:
                    ensureCorrectStructSize( StructType.DATE_TIME_WITH_ZONE_NAME, DATE_TIME_WITH_ZONE_NAME_SIZE, size );
                    return unpackDateTimeWithZoneName();
                default:
                    return super.unpackStruct( signature, size );
                }
            }
            catch ( PackStream.PackStreamException | BoltIOException ex )
            {
                throw ex;
            }
            catch ( Throwable ex )
            {
                StructType type = StructType.valueOf( signature );
                if ( type != null )
                {
                    throw new BoltIOException( Status.Statement.TypeError,
                            String.format( "Unable to construct %s value: `%s`", type.description(), ex.getMessage() ), ex );
                }

                throw ex;
            }
        }

        private PointValue unpackPoint2D() throws IOException
        {

            int crsCode = unpackInteger();
            CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( crsCode );
            double[] coordinates = {unpackDouble(), unpackDouble()};
            return pointValue( crs, coordinates );
        }

        private PointValue unpackPoint3D() throws IOException
        {
            int crsCode = unpackInteger();
            CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( crsCode );
            double[] coordinates = {unpackDouble(), unpackDouble(), unpackDouble()};
            return pointValue( crs, coordinates );
        }

        private DurationValue unpackDuration() throws IOException
        {
            long months = unpackLong();
            long days = unpackLong();
            long seconds = unpackLong();
            long nanos = unpackInteger();
            return duration( months, days, seconds, nanos );
        }

        private DateValue unpackDate() throws IOException
        {
            long epochDay = unpackLong();
            return epochDate( epochDay );
        }

        private LocalTimeValue unpackLocalTime() throws IOException
        {
            long nanoOfDay = unpackLong();
            return localTime( nanoOfDay );
        }

        private TimeValue unpackTime() throws IOException
        {
            long nanosOfDayLocal = unpackLong();
            int offsetSeconds = unpackInteger();
            return time( TemporalUtil.nanosOfDayToUTC( nanosOfDayLocal, offsetSeconds ), ZoneOffset.ofTotalSeconds( offsetSeconds ) );
        }

        private LocalDateTimeValue unpackLocalDateTime() throws IOException
        {
            long epochSecond = unpackLong();
            long nano = unpackLong();
            return localDateTime( epochSecond, nano );
        }

        private DateTimeValue unpackDateTimeWithZoneOffset() throws IOException
        {
            long epochSecondLocal = unpackLong();
            long nano = unpackLong();
            int offsetSeconds = unpackInteger();
            return datetime( newZonedDateTime( epochSecondLocal, nano, ZoneOffset.ofTotalSeconds( offsetSeconds ) ) );
        }

        private DateTimeValue unpackDateTimeWithZoneName() throws IOException
        {
            long epochSecondLocal = unpackLong();
            long nano = unpackLong();
            String zoneId = unpackString();
            return datetime( newZonedDateTime( epochSecondLocal, nano, ZoneId.of( zoneId ) ) );
        }

        private static ZonedDateTime newZonedDateTime( long epochSecondLocal, long nano, ZoneId zoneId )
        {
            Instant instant = Instant.ofEpochSecond( epochSecondLocal, nano );
            LocalDateTime localDateTime = LocalDateTime.ofInstant( instant, UTC );
            return ZonedDateTime.of( localDateTime, zoneId );
        }
    }
}
