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
package org.neo4j.values.storable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalQueries;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.values.ValueMapper;

import static java.time.Instant.ofEpochSecond;
import static java.time.ZonedDateTime.ofInstant;
import static java.util.Objects.requireNonNull;
import static org.neo4j.values.storable.DateValue.DATE_PATTERN;
import static org.neo4j.values.storable.DateValue.parseDate;
import static org.neo4j.values.storable.LocalDateTimeValue.optTime;
import static org.neo4j.values.storable.TimeValue.TIME_PATTERN;
import static org.neo4j.values.storable.TimeValue.parseOffset;

public final class DateTimeValue extends TemporalValue<ZonedDateTime,DateTimeValue>
{
    public static DateTimeValue datetime( DateValue date, LocalTimeValue time, ZoneId zone )
    {
        return new DateTimeValue( ZonedDateTime.of( date.temporal(), time.temporal(), zone ) );
    }

    public static DateTimeValue datetime( DateValue date, TimeValue time )
    {
        OffsetTime t = time.temporal();
        return new DateTimeValue( ZonedDateTime.of( date.temporal(), t.toLocalTime(), t.getOffset() ) );
    }

    public static DateTimeValue datetime(
            int year, int month, int day, int hour, int minute, int second, int nanoOfSecond, ZoneId zone )
    {
        return new DateTimeValue( ZonedDateTime.of( year, month, day, hour, minute, second, nanoOfSecond, zone ) );
    }

    public static DateTimeValue datetime( ZonedDateTime datetime )
    {
        return new DateTimeValue( requireNonNull( datetime, "ZonedDateTime" ) );
    }

    public static DateTimeValue datetime( OffsetDateTime datetime )
    {
        return new DateTimeValue( requireNonNull( datetime, "OffsetDateTime" ).toZonedDateTime() );
    }

    public static DateTimeValue datetime( long epochSecondUTC, long nano, ZoneId zone )
    {
        return new DateTimeValue( ofInstant( ofEpochSecond( epochSecondUTC, nano ), zone ) );
    }

    public static DateTimeValue parse( CharSequence text, Supplier<ZoneId> defaultZone )
    {
        return parse( DateTimeValue.class, PATTERN, DateTimeValue::parse, text, defaultZone );
    }

    public static DateTimeValue parse( TextValue text, Supplier<ZoneId> defaultZone )
    {
        return parse( DateTimeValue.class, PATTERN, DateTimeValue::parse, text, defaultZone );
    }

    private final ZonedDateTime value;

    private DateTimeValue( ZonedDateTime value )
    {
        this.value = value;
    }

    @Override
    ZonedDateTime temporal()
    {
        return value;
    }

    @Override
    public boolean equals( Value other )
    {
        if ( other instanceof DateTimeValue )
        {
            DateTimeValue that = (DateTimeValue) other;
            return value.toInstant().equals( that.value.toInstant() );
        }
        return false;
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        Instant instant = value.toInstant();
        ZoneId zone = value.getZone();
        if ( zone instanceof ZoneOffset )
        {
            ZoneOffset offset = (ZoneOffset) zone;
            writer.writeDateTime( instant.getEpochSecond(), instant.getNano(), offset.getTotalSeconds() );
        }
        else
        {
            writer.writeDateTime( instant.getEpochSecond(), instant.getNano(), zone.getId() );
        }
    }

    @Override
    public String prettyPrint()
    {
        return value.format( DateTimeFormatter.ISO_DATE_TIME );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.ZONED_DATE_TIME;
    }

    @Override
    protected int computeHash()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapDateTime( this );
    }

    @Override
    public DateTimeValue add( DurationValue duration )
    {
        return replacement( value.plus( duration ) );
    }

    @Override
    public DateTimeValue sub( DurationValue duration )
    {
        return replacement( value.minus( duration ) );
    }

    @Override
    DateTimeValue replacement( ZonedDateTime datetime )
    {
        return value == datetime ? this : new DateTimeValue( datetime );
    }

    private static final String ZONE_NAME = "(?<zoneName>[a-zA-Z0-9_ /+-]+)";
    private static final Pattern PATTERN = Pattern.compile(
            DATE_PATTERN + "(?<time>T" + TIME_PATTERN + "(?:\\[" + ZONE_NAME + "\\])?" + ")?",
            Pattern.CASE_INSENSITIVE );
    private static final DateTimeFormatter ZONE_NAME_PARSER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendZoneRegionId()
            .toFormatter();

    private static DateTimeValue parse( Matcher matcher, Supplier<ZoneId> defaultZone )
    {
        LocalDateTime local = LocalDateTime.of( parseDate( matcher ), optTime( matcher ) );
        String zoneName = matcher.group( "zoneName" );
        ZoneOffset offset = parseOffset( matcher );
        ZoneId zone;
        if ( zoneName != null )
        {
            zone = parseZoneName( zoneName );
            if ( offset != null )
            {
                ZoneOffset expected = zone.getRules().getOffset( local );
                if ( !expected.equals( offset ) )
                {
                    throw new IllegalArgumentException( "Timezone and offset do not match: " + matcher.group() );
                }
            }
        }
        else if ( offset != null )
        {
            zone = offset;
        }
        else
        {
            zone = defaultZone.get();
        }
        return new DateTimeValue( ZonedDateTime.of( local, zone ) );
    }

    private static ZoneId parseZoneName( String zoneName )
    {
        return ZONE_NAME_PARSER.parse( zoneName.replace( ' ', '_' ) ).query( TemporalQueries.zoneId() );
    }
}
