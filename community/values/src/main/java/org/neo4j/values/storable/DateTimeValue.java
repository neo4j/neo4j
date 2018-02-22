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

import java.lang.invoke.MethodHandle;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
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

import org.neo4j.helpers.collection.Pair;
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
import static org.neo4j.values.storable.TimeValue.validNano;

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
        return new DateTimeBuilder<AnyValue,DateTimeValue>()
        {
            @Override
            protected ZoneId timezone( AnyValue timezone )
            {
                return timezone == null ? defaultZone.get() : timezoneOf( timezone );
            }

            @Override
            protected DateTimeValue selectDateTime( AnyValue temporal )
            {
                if ( temporal instanceof DateTimeValue )
                {
                    DateTimeValue value = (DateTimeValue) temporal;
                    ZoneId zone = optionalTimezone();
                    return zone == null ? value : new DateTimeValue(
                            ZonedDateTime.of( value.temporal().toLocalDateTime(), zone ) );
                }
                if ( temporal instanceof LocalDateTimeValue )
                {
                    return new DateTimeValue( ZonedDateTime.of(
                            ((LocalDateTimeValue) temporal).temporal(), timezone() ) );
                }
                throw new IllegalArgumentException( "Cannot select datetime from: " + temporal );
            }

            @Override
            protected DateTimeValue selectDateAndTime( AnyValue date, AnyValue time )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected DateTimeValue selectDateWithConstructedTime(
                    AnyValue date,
                    AnyValue hour,
                    AnyValue minute,
                    AnyValue second,
                    AnyValue millisecond,
                    AnyValue microsecond,
                    AnyValue nanosecond )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected DateTimeValue selectDate( AnyValue temporal )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected DateTimeValue constructYear( AnyValue year )
            {
                return constructCalendarDateWithConstructedTime( year, null, null, null, null, null, null, null, null );
            }

            @Override
            protected DateTimeValue constructCalendarDate( AnyValue year, AnyValue month, AnyValue day )
            {
                return constructCalendarDateWithConstructedTime( year, month, day, null, null, null, null, null, null );
            }

            @Override
            protected DateTimeValue constructCalendarDateWithSelectedTime(
                    AnyValue year, AnyValue month, AnyValue day, AnyValue time )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected DateTimeValue constructCalendarDateWithConstructedTime(
                    AnyValue year,
                    AnyValue month,
                    AnyValue day,
                    AnyValue hour,
                    AnyValue minute,
                    AnyValue second,
                    AnyValue millisecond,
                    AnyValue microsecond,
                    AnyValue nanosecond )
            {
                assertDefinedInOrder(
                        Pair.of( year, "year" ),
                        Pair.of( month, "month" ),
                        Pair.of( day, "day" ),
                        Pair.of( hour, "hour" ),
                        Pair.of( minute, "minute" ),
                        Pair.of( second, "second" ),
                        Pair.of( oneOf( millisecond, microsecond, nanosecond ), "subsecond" ) );
                return datetime(
                        (int) safeCastIntegral( "year", year, 0 ),
                        (int) safeCastIntegral( "month", month, 1 ),
                        (int) safeCastIntegral( "day", day, 1 ),
                        (int) safeCastIntegral( "hour", hour, 0 ),
                        (int) safeCastIntegral( "minute", minute, 0 ),
                        (int) safeCastIntegral( "second", second, 0 ),
                        validNano( millisecond, microsecond, nanosecond ),
                        timezone() );
            }

            @Override
            protected DateTimeValue constructWeekDate( AnyValue year, AnyValue week, AnyValue dayOfWeek )
            {
                return constructWeekDateWithConstructedTime( year, week, dayOfWeek, null, null, null, null, null, null );
            }

            @Override
            protected DateTimeValue constructWeekDateWithSelectedTime(
                    AnyValue year, AnyValue week, AnyValue dayOfWeek, AnyValue time )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected DateTimeValue constructWeekDateWithConstructedTime(
                    AnyValue year,
                    AnyValue week,
                    AnyValue dayOfWeek,
                    AnyValue hour,
                    AnyValue minute,
                    AnyValue second,
                    AnyValue millisecond,
                    AnyValue microsecond,
                    AnyValue nanosecond )
            {
                assertDefinedInOrder(
                        Pair.of( year, "year" ),
                        Pair.of( week, "week" ),
                        Pair.of( dayOfWeek, "dayOfWeek" ),
                        Pair.of( hour, "hour" ),
                        Pair.of( minute, "minute" ),
                        Pair.of( second, "second" ),
                        Pair.of( oneOf( millisecond, microsecond, nanosecond ), "subsecond" ) );
                return datetime(
                        ZonedDateTime.now()
                                .withYear( (int) safeCastIntegral( "year", year, 0 ) )
                                .with( IsoFields.WEEK_OF_WEEK_BASED_YEAR, (int) safeCastIntegral( "week", week, 1 ) )
                                .with( ChronoField.DAY_OF_WEEK, (int) safeCastIntegral( "dayOfWeek", dayOfWeek, 1 ) )
                                .withHour( (int) safeCastIntegral( "hour", hour, 0 ) )
                                .withMinute( (int) safeCastIntegral( "minute", minute, 0 ) )
                                .withSecond( (int) safeCastIntegral( "second", second, 0 ) )
                                .withNano( validNano( millisecond, microsecond, nanosecond ) )
                                .withZoneSameLocal( timezone() )
                );
            }

            @Override
            protected DateTimeValue constructQuarterDate( AnyValue year, AnyValue quarter, AnyValue dayOfQuarter )
            {
                return constructQuarterDateWithConstructedTime( year, quarter, dayOfQuarter, null, null, null, null, null, null );
            }

            @Override
            protected DateTimeValue constructQuarterDateWithSelectedTime(
                    AnyValue year, AnyValue quarter, AnyValue dayOfQuarter, AnyValue time )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected DateTimeValue constructQuarterDateWithConstructedTime(
                    AnyValue year,
                    AnyValue quarter,
                    AnyValue dayOfQuarter,
                    AnyValue hour,
                    AnyValue minute,
                    AnyValue second,
                    AnyValue millisecond,
                    AnyValue microsecond,
                    AnyValue nanosecond )
            {
                assertDefinedInOrder(
                        Pair.of( year, "year" ),
                        Pair.of( quarter, "quarter" ),
                        Pair.of( dayOfQuarter, "dayOfQuarter" ),
                        Pair.of( hour, "hour" ),
                        Pair.of( minute, "minute" ),
                        Pair.of( second, "second" ),
                        Pair.of( oneOf( millisecond, microsecond, nanosecond ), "subsecond" ) );
                return datetime(
                        ZonedDateTime.now()
                                .withYear( (int) safeCastIntegral( "year", year, 0 ) )
                                .with( IsoFields.QUARTER_OF_YEAR, (int) safeCastIntegral( "quarter", quarter, 1 ) )
                                .with( IsoFields.DAY_OF_QUARTER, (int) safeCastIntegral( "dayOfQuarter", dayOfQuarter, 1 ) )
                                .withHour( (int) safeCastIntegral( "hour", hour, 0 ) )
                                .withMinute( (int) safeCastIntegral( "minute", minute, 0 ) )
                                .withSecond( (int) safeCastIntegral( "second", second, 0 ) )
                                .withNano( validNano( millisecond, microsecond, nanosecond ) )
                                .withZoneSameLocal( timezone() )
                );
            }

            @Override
            protected DateTimeValue constructOrdinalDate( AnyValue year, AnyValue ordinalDay )
            {
                return constructOrdinalDateWithConstructedTime( year, ordinalDay, null, null, null, null, null, null );
            }

            @Override
            protected DateTimeValue constructOrdinalDateWithSelectedTime(
                    AnyValue year, AnyValue ordinalDay, AnyValue time )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected DateTimeValue constructOrdinalDateWithConstructedTime(
                    AnyValue year,
                    AnyValue ordinalDay,
                    AnyValue hour,
                    AnyValue minute,
                    AnyValue second,
                    AnyValue millisecond,
                    AnyValue microsecond,
                    AnyValue nanosecond )
            {
                assertDefinedInOrder(
                        Pair.of( year, "year" ),
                        Pair.of( ordinalDay, "ordinalDay" ),
                        Pair.of( hour, "hour" ),
                        Pair.of( minute, "minute" ),
                        Pair.of( second, "second" ),
                        Pair.of( oneOf( millisecond, microsecond, nanosecond ), "subsecond" ) );
                return datetime(
                        ZonedDateTime.now()
                                .withYear( (int) safeCastIntegral( "year", year, 0 ) )
                                .withDayOfYear( (int) safeCastIntegral( "ordinalDay", ordinalDay, 1 ) )
                                .withHour( (int) safeCastIntegral( "hour", hour, 0 ) )
                                .withMinute( (int) safeCastIntegral( "minute", minute, 0 ) )
                                .withSecond( (int) safeCastIntegral( "second", second, 0 ) )
                                .withNano( validNano( millisecond, microsecond, nanosecond ) )
                                .withZoneSameLocal( timezone() )
                );
            }
        };
    }

    public abstract static class Compiler<Input> extends DateTimeBuilder<Input,MethodHandle>
    {
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

    public int compareTo( DateTimeValue other )
    {

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

    abstract static class DateTimeBuilder<Input, Result> extends Builder<Input,Result>
    {
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

        @Override
        protected final Result selectTime( Input temporal )
        {
            throw new IllegalStateException( "Cannot select time without date for datetime." );
        }

        @Override
        protected final Result constructTime(
                Input hour, Input minute, Input second, Input millisecond, Input microsecond, Input nanosecond )
        {
            throw new IllegalStateException( "Cannot construct time without date for datetime." );
        }
    }
}
