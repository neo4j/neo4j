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

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.values.AnyValue;
import org.neo4j.values.StructureBuilder;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.MapValue;

import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.ofEpochSecond;
import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.ofInstant;
import static java.util.Objects.requireNonNull;
import static org.neo4j.values.storable.DateValue.DATE_PATTERN;
import static org.neo4j.values.storable.DateValue.parseDate;
import static org.neo4j.values.storable.IntegralValue.safeCastIntegral;
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
            int year, int month, int day, int hour, int minute, int second, int nanoOfSecond, String zone )
    {
        return datetime( year, month, day, hour, minute, second, nanoOfSecond, parseZoneName( zone ) );
    }

    public static DateTimeValue datetime(
            int year, int month, int day, int hour, int minute, int second, int nanoOfSecond, ZoneId zone )
    {
        return new DateTimeValue( ZonedDateTime.of( year, month, day, hour, minute, second, nanoOfSecond, zone ) );
    }

    public static DateTimeValue datetime( long epochSecond, long nano, ZoneOffset zoneOffset )
    {
        return new DateTimeValue( ofInstant( ofEpochSecond( epochSecond, nano ), zoneOffset ) );
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

    public static DateTimeValue ofEpoch( IntegralValue epochSecondUTC, IntegralValue nano )
    {
        long ns = safeCastIntegral( "nanosecond", nano, 0 );
        if ( ns < 0 || ns >=  1000_000_000 )
        {
            throw new IllegalArgumentException( "Invalid nanosecond: " + ns );
        }
        return new DateTimeValue( ofInstant( ofEpochSecond( epochSecondUTC.longValue(), ns ), UTC ) );
    }

    public static DateTimeValue ofEpochMillis( IntegralValue millisUTC )
    {
        return new DateTimeValue( ofInstant( ofEpochMilli( millisUTC.longValue() ), UTC ) );
    }

    public static DateTimeValue parse( CharSequence text, Supplier<ZoneId> defaultZone )
    {
        return parse( DateTimeValue.class, PATTERN, DateTimeValue::parse, text, defaultZone );
    }

    public static DateTimeValue parse( TextValue text, Supplier<ZoneId> defaultZone )
    {
        return parse( DateTimeValue.class, PATTERN, DateTimeValue::parse, text, defaultZone );
    }

    public static DateTimeValue now( Clock clock )
    {
        return new DateTimeValue( ZonedDateTime.now( clock ) );
    }

    public static DateTimeValue now( Clock clock, String timezone )
    {
        return now( clock.withZone( parseZoneName( timezone ) ) );
    }

    public static DateTimeValue build( MapValue map, Supplier<ZoneId> defaultZone )
    {
        return StructureBuilder.build( builder( defaultZone ), map );
    }

    public static DateTimeValue truncate(
            TemporalUnit unit,
            TemporalValue input,
            MapValue fields,
            Supplier<ZoneId> defaultZone )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    static StructureBuilder<AnyValue,DateTimeValue> builder( Supplier<ZoneId> defaultZone )
    {
        return new DateTimeBuilder<DateTimeValue>( defaultZone )
        {
            private final ZonedDateTime defaulZonedDateTime =
                    ZonedDateTime.of( Field.year.defaultValue, Field.month.defaultValue, Field.day.defaultValue, Field.hour.defaultValue,
                            Field.minute.defaultValue, Field.second.defaultValue, Field.nanosecond.defaultValue, timezone() );

            @Override
            public DateTimeValue buildInternal()
            {
                ZonedDateTime result;
                if ( fields.containsKey( Field.time ) || fields.containsKey( Field.date ) || fields.containsKey( Field.datetime ) )
                {
//                    AnyValue time = fields.get( Field.time );
//                    if ( !(time instanceof TemporalValue) )
//                    {
//                        throw new IllegalArgumentException( String.format( "Cannot construct local date time from: %s", time ) );
//                    }
                    // TODO select date, time, or both
                    throw new UnsupportedOperationException();
                }
                else if ( fields.containsKey( Field.week ) )
                {
                    // Be sure to be in the start of the week based year (which can be later than 1st Jan)
                    result = defaulZonedDateTime.with( IsoFields.WEEK_BASED_YEAR,
                            safeCastIntegral( Field.year.name(), fields.get( Field.year ), Field.year.defaultValue ) ).with( IsoFields.WEEK_OF_WEEK_BASED_YEAR,
                            1 ).with( ChronoField.DAY_OF_WEEK, 1 );
                }
                else
                {
                    result = defaulZonedDateTime;
                }

                result = assignAllFields( result );
                if ( timezone != null )
                {
                    // TODO this will be different when selecting
                    result = result.withZoneSameLocal( timezone() );
                }
                return datetime( result );
            }

            @Override
            protected DateTimeValue selectDateTime( AnyValue datetime )
            {
                if ( datetime instanceof DateTimeValue )
                {
                    DateTimeValue value = (DateTimeValue) datetime;
                    ZoneId zone = optionalTimezone();
                    return zone == null ? value : new DateTimeValue(
                            ZonedDateTime.of( value.temporal().toLocalDateTime(), zone ) );
                }
                if ( datetime instanceof LocalDateTimeValue )
                {
                    return new DateTimeValue( ZonedDateTime.of(
                            ((LocalDateTimeValue) datetime).temporal(), timezone() ) );
                }
                throw new IllegalArgumentException( "Cannot select datetime from: " + datetime );
            }
        };
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
    LocalDate getDatePart()
    {
        return value.toLocalDate();
    }

    @Override
    LocalTime getLocalTimePart()
    {
        return value.toLocalTime();
    }

    @Override
    OffsetTime getTimePart( Supplier<ZoneId> defaultZone )
    {
        ZoneOffset offset = value.getOffset();
        LocalTime localTime = value.toLocalTime();
        return OffsetTime.of( localTime, offset );
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
    int unsafeCompareTo( Value otherValue )
    {
        DateTimeValue other = (DateTimeValue) otherValue;
        return value.compareTo( other.value );
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
        return value.toInstant().hashCode();
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

    private static final String ZONE_NAME = "(?<zoneName>[a-zA-Z0-9~._ /+-]+)";
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

    static ZoneId parseZoneName( String zoneName )
    {
        return ZONE_NAME_PARSER.parse( zoneName.replace( ' ', '_' ) ).query( TemporalQueries.zoneId() );
    }

    abstract static class DateTimeBuilder<Result> extends Builder<Result>
    {
        DateTimeBuilder( Supplier<ZoneId> defaultZone )
        {
            super( defaultZone );
        }

        @Override
        protected final boolean supportsDate()
        {
            return true;
        }

        @Override
        protected final boolean supportsTime()
        {
            return true;
        }

        protected abstract Result selectDateTime( AnyValue date );
    }
}
