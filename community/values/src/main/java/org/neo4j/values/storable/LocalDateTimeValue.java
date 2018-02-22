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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.values.AnyValue;
import org.neo4j.values.StructureBuilder;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.MapValue;

import static java.time.Instant.ofEpochSecond;
import static java.time.LocalDateTime.ofInstant;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.neo4j.values.storable.DateTimeValue.parseZoneName;
import static org.neo4j.values.storable.DateValue.DATE_PATTERN;
import static org.neo4j.values.storable.DateValue.parseDate;
import static org.neo4j.values.storable.IntegralValue.safeCastIntegral;
import static org.neo4j.values.storable.LocalTimeValue.TIME_PATTERN;
import static org.neo4j.values.storable.LocalTimeValue.parseTime;
import static org.neo4j.values.storable.TimeValue.validNano;

public final class LocalDateTimeValue extends TemporalValue<LocalDateTime,LocalDateTimeValue>
{
    public static LocalDateTimeValue localDateTime( DateValue date, LocalTimeValue time )
    {
        return new LocalDateTimeValue( LocalDateTime.of( date.temporal(), time.temporal() ) );
    }

    public static LocalDateTimeValue localDateTime(
            int year, int month, int day, int hour, int minute, int second, int nanoOfSecond )
    {
        return new LocalDateTimeValue( LocalDateTime.of( year, month, day, hour, minute, second, nanoOfSecond ) );
    }

    public static LocalDateTimeValue localDateTime( LocalDateTime value )
    {
        return new LocalDateTimeValue( requireNonNull( value, "LocalDateTime" ) );
    }

    public static LocalDateTimeValue localDateTime( long epochSecond, long nano )
    {
        return new LocalDateTimeValue( ofInstant( ofEpochSecond( epochSecond, nano ), UTC ) );
    }

    public static LocalDateTimeValue inUTC( DateTimeValue datetime )
    {
        return new LocalDateTimeValue( datetime.temporal().withZoneSameInstant( UTC ).toLocalDateTime() );
    }

    public static LocalDateTimeValue parse( CharSequence text )
    {
        return parse( LocalDateTimeValue.class, PATTERN, LocalDateTimeValue::parse, text );
    }

    public static LocalDateTimeValue parse( TextValue text )
    {
        return parse( LocalDateTimeValue.class, PATTERN, LocalDateTimeValue::parse, text );
    }

    public static LocalDateTimeValue now( Clock clock )
    {
        return new LocalDateTimeValue( LocalDateTime.now( clock ) );
    }

    public static LocalDateTimeValue now( Clock clock, String timezone )
    {
        return now( clock.withZone( parseZoneName( timezone ) ) );
    }

    public static LocalDateTimeValue build( MapValue map, Supplier<ZoneId> defaultZone )
    {
        return StructureBuilder.build( builder( defaultZone ), map );
    }

    public static LocalDateTimeValue truncate(
            TemporalUnit unit,
            TemporalValue input,
            MapValue fields,
            Supplier<ZoneId> defaultZone )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    static StructureBuilder<AnyValue,LocalDateTimeValue> builder( Supplier<ZoneId> defaultZone )
    {
        return new DateTimeValue.DateTimeBuilder<AnyValue,LocalDateTimeValue>()
        {
            @Override
            protected ZoneId timezone( AnyValue timezone )
            {
                return timezone == null ? defaultZone.get() : timezoneOf( timezone );
            }

            @Override
            protected LocalDateTimeValue selectDateTime( AnyValue temporal )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected LocalDateTimeValue selectDateAndTime( AnyValue date, AnyValue time )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected LocalDateTimeValue selectDateWithConstructedTime(
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
            protected LocalDateTimeValue selectDate( AnyValue temporal )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected LocalDateTimeValue constructYear( AnyValue year )
            {
                return constructCalendarDateWithConstructedTime( year, null, null, null, null, null, null, null, null );
            }

            @Override
            protected LocalDateTimeValue constructCalendarDate( AnyValue year, AnyValue month, AnyValue day )
            {
                return constructCalendarDateWithConstructedTime( year, month, day, null, null, null, null, null, null );
            }

            @Override
            protected LocalDateTimeValue constructCalendarDateWithSelectedTime(
                    AnyValue year, AnyValue month, AnyValue day, AnyValue time )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected LocalDateTimeValue constructCalendarDateWithConstructedTime(
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
                return localDateTime(
                        (int) safeCastIntegral( "year", year, 0 ),
                        (int) safeCastIntegral( "month", month, 1 ),
                        (int) safeCastIntegral( "day", day, 1 ),
                        (int) safeCastIntegral( "hour", hour, 0 ),
                        (int) safeCastIntegral( "minute", minute, 0 ),
                        (int) safeCastIntegral( "second", second, 0 ),
                        validNano( millisecond, microsecond, nanosecond ) );
            }

            @Override
            protected LocalDateTimeValue constructWeekDate( AnyValue year, AnyValue week, AnyValue dayOfWeek )
            {
                return constructWeekDateWithConstructedTime( year, week, dayOfWeek, null, null, null, null, null, null );
            }

            @Override
            protected LocalDateTimeValue constructWeekDateWithSelectedTime(
                    AnyValue year, AnyValue week, AnyValue dayOfWeek, AnyValue time )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected LocalDateTimeValue constructWeekDateWithConstructedTime(
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
                return localDateTime(
                        LocalDateTime.now()
                                .withYear( (int) safeCastIntegral( "year", year, 0 ) )
                                .with( IsoFields.WEEK_OF_WEEK_BASED_YEAR, (int) safeCastIntegral( "week", week, 1 ) )
                                .with( ChronoField.DAY_OF_WEEK, (int) safeCastIntegral( "dayOfWeek", dayOfWeek, 1 ) )
                                .withHour( (int) safeCastIntegral( "hour", hour, 0 ) )
                                .withMinute( (int) safeCastIntegral( "minute", minute, 0 ) )
                                .withSecond( (int) safeCastIntegral( "second", second, 0 ) )
                                .withNano( validNano( millisecond, microsecond, nanosecond ) )
                         );
            }

            @Override
            protected LocalDateTimeValue constructQuarterDate(
                    AnyValue year, AnyValue quarter, AnyValue dayOfQuarter )
            {
                return constructQuarterDateWithConstructedTime( year, quarter, dayOfQuarter, null, null, null, null, null, null );
            }

            @Override
            protected LocalDateTimeValue constructQuarterDateWithSelectedTime(
                    AnyValue year, AnyValue quarter, AnyValue dayOfQuarter, AnyValue time )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected LocalDateTimeValue constructQuarterDateWithConstructedTime(
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
                return localDateTime(
                        LocalDateTime.now()
                                .withYear( (int) safeCastIntegral( "year", year, 0 ) )
                                .with( IsoFields.QUARTER_OF_YEAR, (int) safeCastIntegral( "quarter", quarter, 1 ) )
                                .with( IsoFields.DAY_OF_QUARTER, (int) safeCastIntegral( "dayOfQuarter", dayOfQuarter, 1 ) )
                                .withHour( (int) safeCastIntegral( "hour", hour, 0 ) )
                                .withMinute( (int) safeCastIntegral( "minute", minute, 0 ) )
                                .withSecond( (int) safeCastIntegral( "second", second, 0 ) )
                                .withNano( validNano( millisecond, microsecond, nanosecond ) )
                );
            }

            @Override
            protected LocalDateTimeValue constructOrdinalDate( AnyValue year, AnyValue ordinalDay )
            {
                return constructOrdinalDateWithConstructedTime( year, ordinalDay, null, null, null, null, null, null );
            }

            @Override
            protected LocalDateTimeValue constructOrdinalDateWithSelectedTime(
                    AnyValue year, AnyValue ordinalDay, AnyValue time )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected LocalDateTimeValue constructOrdinalDateWithConstructedTime(
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
                return localDateTime(
                        LocalDateTime.now()
                                .withYear( (int) safeCastIntegral( "year", year, 0 ) )
                                .withDayOfYear( (int) safeCastIntegral( "ordinalDay", ordinalDay, 1 ) )
                                .withHour( (int) safeCastIntegral( "hour", hour, 0 ) )
                                .withMinute( (int) safeCastIntegral( "minute", minute, 0 ) )
                                .withSecond( (int) safeCastIntegral( "second", second, 0 ) )
                                .withNano( validNano( millisecond, microsecond, nanosecond ) )
                );
            }
        };
    }

    public abstract static class Compiler<Input> extends DateTimeValue.DateTimeBuilder<Input,MethodHandle>
    {
    }

    private final LocalDateTime value;

    private LocalDateTimeValue( LocalDateTime value )
    {
        this.value = value;
    }

    public int compareTo( LocalDateTimeValue other )
    {

        return value.compareTo( other.value );
    }

    @Override
    LocalDateTime temporal()
    {
        return value;
    }

    @Override
    public boolean equals( Value other )
    {
        return other instanceof LocalDateTimeValue && value.equals( ((LocalDateTimeValue) other).value );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeLocalDateTime( value.toEpochSecond( UTC ), value.getNano() );
    }

    @Override
    public String prettyPrint()
    {
        return value.format( DateTimeFormatter.ISO_LOCAL_DATE_TIME );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.LOCAL_DATE_TIME;
    }

    @Override
    protected int computeHash()
    {
        return value.toInstant( UTC ).hashCode();
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapLocalDateTime( this );
    }

    @Override
    public LocalDateTimeValue add( DurationValue duration )
    {
        return replacement( value.plus( duration ) );
    }

    @Override
    public LocalDateTimeValue sub( DurationValue duration )
    {
        return replacement( value.minus( duration ) );
    }

    @Override
    LocalDateTimeValue replacement( LocalDateTime dateTime )
    {
        return dateTime == value ? this : new LocalDateTimeValue( dateTime );
    }

    private static final Pattern PATTERN = Pattern.compile(
            DATE_PATTERN + "(?<time>T" + TIME_PATTERN + ")?",
            Pattern.CASE_INSENSITIVE );

    private static LocalDateTimeValue parse( Matcher matcher )
    {
        return localDateTime( LocalDateTime.of( parseDate( matcher ), optTime( matcher ) ) );
    }

    static LocalTime optTime( Matcher matcher )
    {
        return matcher.group( "time" ) != null ? parseTime( matcher ) : LocalTime.MIN;
    }
}
