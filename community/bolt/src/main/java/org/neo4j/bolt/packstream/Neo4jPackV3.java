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

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.StructType;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DateTimeValue;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static java.util.Objects.requireNonNull;

public class Neo4jPackV3 extends Neo4jPackV2
{
    public static final long VERSION = 3;

    public static final byte DATE_TIME_WITH_ZONE_OFFSET_UTC = 'I';
    public static final int DATE_TIME_WITH_ZONE_OFFSET_UTC_SIZE = 3;

    public static final byte DATE_TIME_WITH_ZONE_NAME_UTC = 'i';
    public static final int DATE_TIME_WITH_ZONE_NAME_UTC_SIZE = 3;

    @Override
    public Neo4jPack.Packer newPacker( PackOutput output )
    {
        return new PackerV3( output );
    }

    @Override
    public Neo4jPack.Unpacker newUnpacker( PackInput input )
    {
        return new UnpackerV3( input );
    }

    @Override
    public long version()
    {
        return VERSION;
    }

    private static class PackerV3 extends PackerV2
    {
        PackerV3( PackOutput output )
        {
            super( output );
        }

        @Override
        public void writeDateTime( ZonedDateTime zonedDateTime ) throws IOException
        {
            requireNonNull( zonedDateTime, "dateTime cannot be null" );
            var instant = zonedDateTime.toInstant();
            var epochSeconds = instant.getEpochSecond();
            var epochNano = zonedDateTime.getNano();
            var zone = zonedDateTime.getZone();

            if ( zone instanceof ZoneOffset )
            {
                var offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();

                packStructHeader( DATE_TIME_WITH_ZONE_OFFSET_UTC_SIZE, DATE_TIME_WITH_ZONE_OFFSET_UTC );
                pack( epochSeconds );
                pack( epochNano );
                pack( offsetSeconds );
                return;
            }

            packStructHeader( DATE_TIME_WITH_ZONE_NAME_UTC_SIZE, DATE_TIME_WITH_ZONE_NAME_UTC );
            pack( epochSeconds );
            pack( epochNano );
            pack( zone.getId() );
        }
    }

    private static class UnpackerV3 extends UnpackerV2
    {

        UnpackerV3( PackInput input )
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
                    case DATE_TIME_WITH_ZONE_OFFSET_UTC:
                        ensureCorrectStructSize( StructType.DATE_TIME_WITH_ZONE_OFFSET_UTC, DATE_TIME_WITH_ZONE_OFFSET_UTC_SIZE, size );
                        return unpackDateTimeWithZoneOffset();
                    case DATE_TIME_WITH_ZONE_NAME_UTC:
                        ensureCorrectStructSize( StructType.DATE_TIME_WITH_ZONE_NAME_UTC, DATE_TIME_WITH_ZONE_NAME_UTC_SIZE, size );
                        return unpackDateTimeWithZoneName();
                    case DATE_TIME_WITH_ZONE_OFFSET:
                        throw new BoltIOException( Status.Statement.TypeError,
                                String.format( "Unable to unpack struct: %s when UTC DateTime has been negotiated.", DATE_TIME_WITH_ZONE_OFFSET) );
                    case DATE_TIME_WITH_ZONE_NAME:
                        throw new BoltIOException( Status.Statement.TypeError,
                                String.format( "Unable to unpack struct: %s when UTC DateTime has been negotiated.", DATE_TIME_WITH_ZONE_NAME) );
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

        @Override
        protected DateTimeValue unpackDateTimeWithZoneOffset() throws IOException
        {
            var epochSecond = unpackLong();
            var nanos = unpackLong();
            var offsetSeconds = unpackLong();

            ZoneOffset offset = ZoneOffset.ofTotalSeconds((int) offsetSeconds);
            Instant instant = Instant.ofEpochSecond(epochSecond, nanos);
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, offset);

            return DateTimeValue.datetime(OffsetDateTime.of(localDateTime, offset));
        }

        @Override
        protected DateTimeValue unpackDateTimeWithZoneName() throws IOException
        {
            var epochSecond = unpackLong();
            var nanos = unpackLong();
            var zoneName = unpackString();

            Instant instant = Instant.ofEpochSecond(epochSecond, nanos);
            ZoneId zoneId = ZoneId.of(zoneName);
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zoneId);

            return DateTimeValue.datetime(ZonedDateTime.of(localDateTime, zoneId));
        }

    }

}
