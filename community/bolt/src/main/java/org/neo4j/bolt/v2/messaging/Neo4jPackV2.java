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
package org.neo4j.bolt.v2.messaging;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;

import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.packstream.PackInput;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;

import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.DurationValue.duration;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.storable.TimeValue.time;
import static org.neo4j.values.storable.Values.pointValue;

public class Neo4jPackV2 extends Neo4jPackV1
{
    public static final int VERSION = 2;

    public static final byte POINT_2D = 'X';
    public static final byte POINT_3D = 'Y';

    public static final byte DATE = 'D';
    public static final byte TIME = 'T';
    public static final byte LOCAL_TIME = 't';
    public static final byte LOCAL_DATE_TIME = 'd';
    public static final byte DATE_TIME_WITH_ZONE_OFFSET = 'F';
    public static final byte DATE_TIME_WITH_ZONE_NAME = 'f';
    public static final byte DURATION = 'E';

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
    public int version()
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
                packStructHeader( 3, POINT_2D );
                pack( crs.getCode() );
                pack( coordinate[0] );
                pack( coordinate[1] );
            }
            else if ( coordinate.length == 3 )
            {
                packStructHeader( 4, POINT_3D );
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
            packStructHeader( 4, DURATION );
            pack( months );
            pack( days );
            pack( seconds );
            pack( nanos );
        }

        @Override
        public void writeDate( long epochDay ) throws IOException
        {
            packStructHeader( 1, DATE );
            pack( epochDay );
        }

        @Override
        public void writeLocalTime( long nanoOfDay ) throws IOException
        {
            packStructHeader( 1, LOCAL_TIME );
            pack( nanoOfDay );
        }

        @Override
        public void writeTime( long nanosOfDayLocal, int offsetSeconds ) throws IOException
        {
            packStructHeader( 2, TIME );
            pack( nanosOfDayLocal );
            pack( offsetSeconds );
        }

        @Override
        public void writeLocalDateTime( long epochSecond, int nano ) throws IOException
        {
            packStructHeader( 2, LOCAL_DATE_TIME );
            pack( epochSecond );
            pack( nano );
        }

        @Override
        public void writeDateTime( long epochSecondUTC, int nano, int offsetSeconds ) throws IOException
        {
            packStructHeader( 3, DATE_TIME_WITH_ZONE_OFFSET );
            pack( epochSecondUTC );
            pack( nano );
            pack( offsetSeconds );
        }

        @Override
        public void writeDateTime( long epochSecondUTC, int nano, String zoneId ) throws IOException
        {
            packStructHeader( 3, DATE_TIME_WITH_ZONE_NAME );
            pack( epochSecondUTC );
            pack( nano );
            pack( zoneId );
        }
    }

    private static class UnpackerV2 extends Neo4jPackV1.UnpackerV1
    {
        UnpackerV2( PackInput input )
        {
            super( input );
        }

        @Override
        protected AnyValue unpackStruct( char signature ) throws IOException
        {
            switch ( signature )
            {
            case POINT_2D:
                return unpackPoint2D();
            case POINT_3D:
                return unpackPoint3D();
            case DURATION:
                return unpackDuration();
            case DATE:
                return unpackDate();
            case LOCAL_TIME:
                return unpackLocalTime();
            case TIME:
                return unpackTime();
            case LOCAL_DATE_TIME:
                return unpackLocalDateTime();
            case DATE_TIME_WITH_ZONE_OFFSET:
                return unpackDateTimeWithZoneOffset();
            case DATE_TIME_WITH_ZONE_NAME:
                return unpackDateTimeWithZoneName();
            default:
                return super.unpackStruct( signature );
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
            long nanosOfDayUTC = unpackLong();
            int offsetSeconds = unpackInteger();
            return time( nanosOfDayUTC, ZoneOffset.ofTotalSeconds( offsetSeconds ) );
        }

        private LocalDateTimeValue unpackLocalDateTime() throws IOException
        {
            long epochSecond = unpackLong();
            long nano = unpackLong();
            return localDateTime( epochSecond, nano );
        }

        private DateTimeValue unpackDateTimeWithZoneOffset() throws IOException
        {
            long epochSecondUTC = unpackLong();
            long nano = unpackLong();
            int offsetSeconds = unpackInteger();
            return datetime( epochSecondUTC, nano, ZoneOffset.ofTotalSeconds( offsetSeconds ) );
        }

        private DateTimeValue unpackDateTimeWithZoneName() throws IOException
        {
            long epochSecondUTC = unpackLong();
            long nano = unpackLong();
            String zoneId = unpackString();
            return datetime( epochSecondUTC, nano, ZoneId.of( zoneId ) );
        }
    }
}
