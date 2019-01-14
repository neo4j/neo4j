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
package org.neo4j.values.storable;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.values.AnyValue;
import org.neo4j.values.StructureBuilder;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.utils.InvalidValuesArgumentException;
import org.neo4j.values.utils.UnsupportedTemporalUnitException;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static org.neo4j.values.storable.DateTimeValue.parseZoneName;

public final class LocalTimeValue extends TemporalValue<LocalTime,LocalTimeValue>
{
    public static final LocalTimeValue MIN_VALUE = new LocalTimeValue( LocalTime.MIN );
    public static final LocalTimeValue MAX_VALUE = new LocalTimeValue( LocalTime.MAX );

    public static LocalTimeValue localTime( LocalTime value )
    {
        return new LocalTimeValue( requireNonNull( value, "LocalTime" ) );
    }

    public static LocalTimeValue localTime( int hour, int minute, int second, int nanosOfSecond )
    {
        return new LocalTimeValue( assertValidArgument( () -> LocalTime.of( hour, minute, second, nanosOfSecond ) ) );
    }

    public static LocalTimeValue localTime( long nanoOfDay )
    {
        return new LocalTimeValue( assertValidArgument( () -> LocalTime.ofNanoOfDay( nanoOfDay ) ) );
    }

    public static LocalTimeValue parse( CharSequence text )
    {
        return parse( LocalTimeValue.class, PATTERN, LocalTimeValue::parse, text );
    }

    public static LocalTimeValue parse( TextValue text )
    {
        return parse( LocalTimeValue.class, PATTERN, LocalTimeValue::parse, text );
    }

    public static LocalTimeValue now( Clock clock )
    {
        return new LocalTimeValue( LocalTime.now( clock ) );
    }

    public static LocalTimeValue now( Clock clock, String timezone )
    {
        return now( clock.withZone( parseZoneName( timezone ) ) );
    }

    public static LocalTimeValue now( Clock clock, Supplier<ZoneId> defaultZone )
    {
        return now( clock.withZone( defaultZone.get() ) );
    }

    public static LocalTimeValue build( MapValue map, Supplier<ZoneId> defaultZone )
    {
        return StructureBuilder.build( builder( defaultZone ), map );
    }

    public static LocalTimeValue select( AnyValue from, Supplier<ZoneId> defaultZone )
    {
        return builder( defaultZone ).selectTime( from );
    }

    public static LocalTimeValue truncate(
            TemporalUnit unit,
            TemporalValue input,
            MapValue fields,
            Supplier<ZoneId> defaultZone )
    {
        LocalTime localTime = input.getLocalTimePart();
        LocalTime truncatedLT = assertValidUnit( () -> localTime.truncatedTo( unit ) );
        if ( fields.size() == 0 )
        {
            return localTime( truncatedLT );
        }
        else
        {
            Map<String,AnyValue> updatedFields = fields.getMapCopy();
            truncatedLT = updateFieldMapWithConflictingSubseconds( updatedFields, unit, truncatedLT );
            if ( updatedFields.size() == 0 )
            {
                return localTime( truncatedLT );
            }
            updatedFields.put( "time", localTime( truncatedLT ) );
            return build( VirtualValues.map( updatedFields ), defaultZone );
        }
    }

    static final LocalTime DEFAULT_LOCAL_TIME = LocalTime.of( TemporalFields.hour.defaultValue, TemporalFields.minute.defaultValue );

    static TimeValue.TimeBuilder<LocalTimeValue> builder( Supplier<ZoneId> defaultZone )
    {
        return new TimeValue.TimeBuilder<LocalTimeValue>( defaultZone )
        {
            @Override
            protected boolean supportsTimeZone()
            {
                return false;
            }

            @Override
            public LocalTimeValue buildInternal()
            {
                LocalTime result;
                if ( fields.containsKey( TemporalFields.time ) )
                {
                    AnyValue time = fields.get( TemporalFields.time );
                    if ( !(time instanceof TemporalValue) )
                    {
                        throw new InvalidValuesArgumentException( String.format( "Cannot construct local time from: %s", time ) );
                    }
                    result = ((TemporalValue) time).getLocalTimePart();
                }
                else
                {
                    result = DEFAULT_LOCAL_TIME;
                }

                result = assignAllFields( result );
                return localTime( result );
            }

            @Override
            protected LocalTimeValue selectTime(
                    AnyValue time )
            {

                if ( !(time instanceof TemporalValue) )
                {
                    throw new InvalidValuesArgumentException( String.format( "Cannot construct local time from: %s", time ) );
                }
                TemporalValue v = (TemporalValue) time;
                LocalTime lt = v.getLocalTimePart();
                return localTime( lt );
            }
        };
    }

    private final LocalTime value;

    private LocalTimeValue( LocalTime value )
    {
        this.value = value;
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        LocalTimeValue other = (LocalTimeValue) otherValue;
        return value.compareTo( other.value );
    }

    @Override
    LocalTime temporal()
    {
        return value;
    }

    @Override
    LocalDate getDatePart()
    {
        throw new UnsupportedTemporalUnitException( String.format( "Cannot get the date of: %s", this ) );
    }

    @Override
    public String getTypeName()
    {
        return "LocalTime";
    }

    @Override
    LocalTime getLocalTimePart()
    {
        return value;
    }

    @Override
    OffsetTime getTimePart( Supplier<ZoneId> defaultZone )
    {
        ZoneOffset currentOffset = assertValidArgument( () ->  ZonedDateTime.ofInstant( Instant.now(), defaultZone.get() ) ).getOffset();
        return OffsetTime.of( value, currentOffset );
    }

    @Override
    ZoneId getZoneId( Supplier<ZoneId> defaultZone )
    {
        return defaultZone.get();
    }

    @Override
    ZoneId getZoneId()
    {
        throw new UnsupportedTemporalUnitException( String.format( "Cannot get the timezone of: %s", this ) );
    }

    @Override
    ZoneOffset getZoneOffset()
    {
        throw new UnsupportedTemporalUnitException( String.format( "Cannot get the offset of: %s", this ) );
    }

    @Override
    public boolean supportsTimeZone()
    {
        return false;
    }

    @Override
    boolean hasTime()
    {
        return true;
    }

    @Override
    public boolean equals( Value other )
    {
        return other instanceof LocalTimeValue && value.equals( ((LocalTimeValue) other).value );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeLocalTime( value );
    }

    @Override
    public String prettyPrint()
    {
        return assertPrintable( () -> value.format( DateTimeFormatter.ISO_LOCAL_TIME ) );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.LOCAL_TIME;
    }

    @Override
    protected int computeHash()
    {
        return value.hashCode();
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapLocalTime( this );
    }

    @Override
    public LocalTimeValue add( DurationValue duration )
    {
        return replacement( assertValidArithmetic( () -> value.plusNanos( duration.nanosOfDay() ) ) );
    }

    @Override
    public LocalTimeValue sub( DurationValue duration )
    {
        return replacement( assertValidArithmetic( () -> value.minusNanos( duration.nanosOfDay() ) ) );
    }

    @Override
    LocalTimeValue replacement( LocalTime time )
    {
        return time == value ? this : new LocalTimeValue( time );
    }

    static final String TIME_PATTERN = "(?:(?:(?<longHour>[0-9]{1,2})(?::(?<longMinute>[0-9]{1,2})"
            + "(?::(?<longSecond>[0-9]{1,2})(?:\\.(?<longFraction>[0-9]{1,9}))?)?)?)|"
            + "(?:(?<shortHour>[0-9]{2})(?:(?<shortMinute>[0-9]{2})"
            + "(?:(?<shortSecond>[0-9]{2})(?:\\.(?<shortFraction>[0-9]{1,9}))?)?)?))";
    private static final Pattern PATTERN = Pattern.compile( "(?:T)?" + TIME_PATTERN );

    private static LocalTimeValue parse( Matcher matcher )
    {
        return new LocalTimeValue( parseTime( matcher ) );
    }

    static LocalTime parseTime( Matcher matcher )
    {
        int hour;
        int minute;
        int second;
        int fraction;
        String longHour = matcher.group( "longHour" );
        if ( longHour != null )
        {
            hour = parseInt( longHour );
            minute = optInt( matcher.group( "longMinute" ) );
            second = optInt( matcher.group( "longSecond" ) );
            fraction = parseNanos( matcher.group( "longFraction" ) );
        }
        else
        {
            String shortHour = matcher.group( "shortHour" );
            hour = parseInt( shortHour );
            minute = optInt( matcher.group( "shortMinute" ) );
            second = optInt( matcher.group( "shortSecond" ) );
            fraction = parseNanos( matcher.group( "shortFraction" ) );
        }
        return assertParsable( () -> LocalTime.of( hour, minute, second, fraction ) );
    }

    private static int parseNanos( String value )
    {
        if ( value == null )
        {
            return 0;
        }
        int nanos = parseInt( value );
        if ( nanos != 0 )
        {
            for ( int i = value.length(); i < 9; i++ )
            {
                nanos *= 10;
            }
        }
        return nanos;
    }

    static int optInt( String value )
    {
        return value == null ? 0 : parseInt( value );
    }
}
