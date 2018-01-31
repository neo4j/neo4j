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
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.values.AnyValue;
import org.neo4j.values.StructureBuilder;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.MapValue;

import static java.lang.Integer.parseInt;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.neo4j.values.storable.DateTimeValue.parseZoneName;

public final class LocalTimeValue extends TemporalValue<LocalTime,LocalTimeValue>
{
    public static LocalTimeValue localTime( LocalTime value )
    {
        return new LocalTimeValue( requireNonNull( value, "LocalTime" ) );
    }

    public static LocalTimeValue localTime( int hour, int minute, int second, int nanosOfSecond )
    {
        return new LocalTimeValue( LocalTime.of( hour, minute, second, nanosOfSecond ) );
    }

    public static LocalTimeValue localTime( long nanoOfDay )
    {
        return new LocalTimeValue( LocalTime.ofNanoOfDay( nanoOfDay ) );
    }

    public static LocalTimeValue inUTC( TimeValue time )
    {
        return new LocalTimeValue( time.temporal().withOffsetSameInstant( UTC ).toLocalTime() );
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

    public static LocalTimeValue build( MapValue map, Supplier<ZoneId> defaultZone )
    {
        return StructureBuilder.build( builder( defaultZone ), map );
    }

    public static LocalTimeValue truncate(
            TemporalUnit unit,
            TemporalValue input,
            MapValue fields,
            Supplier<ZoneId> defaultZone )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    static StructureBuilder<AnyValue,LocalTimeValue> builder( Supplier<ZoneId> defaultZone )
    {
        return new TimeValue.TimeBuilder<AnyValue,LocalTimeValue>()
        {
            @Override
            protected ZoneId timezone( AnyValue timezone )
            {
                return timezone == null ? defaultZone.get() : timezoneOf( timezone );
            }

            @Override
            protected LocalTimeValue selectTime( AnyValue temporal )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            protected LocalTimeValue constructTime(
                    AnyValue hour,
                    AnyValue minute,
                    AnyValue second,
                    AnyValue millisecond,
                    AnyValue microsecond,
                    AnyValue nanosecond )
            {
                throw new UnsupportedOperationException( "not implemented" );
            }
        };
    }

    public abstract static class Compiler<Input> extends TimeValue.TimeBuilder<Input,MethodHandle>
    {
    }

    private final LocalTime value;

    private LocalTimeValue( LocalTime value )
    {
        this.value = value;
    }

    @Override
    LocalTime temporal()
    {
        return value;
    }

    @Override
    public boolean equals( Value other )
    {
        return other instanceof LocalTimeValue && value.equals( ((LocalTimeValue) other).value );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeLocalTime( value.getLong( ChronoField.NANO_OF_DAY ) );
    }

    @Override
    public String prettyPrint()
    {
        return value.format( DateTimeFormatter.ISO_LOCAL_TIME );
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
        return replacement( value.plusNanos( duration.nanosOfDay() ) );
    }

    @Override
    public LocalTimeValue sub( DurationValue duration )
    {
        return replacement( value.minusNanos( duration.nanosOfDay() ) );
    }

    @Override
    LocalTimeValue replacement( LocalTime time )
    {
        return time == value ? this : new LocalTimeValue( time );
    }

    static final String TIME_PATTERN = "(?:(?:(?<longHour>[0-9]{1,2})(?::(?<longMinute>[0-9]{1,2})"
            + "(?::(?<longSecond>[0-9]{1,2})(?:.(?<longFraction>[0-9]{1,9}))?)?)?)|"
            + "(?:(?<shortHour>[0-9]{2})(?:(?<shortMinute>[0-9]{2})"
            + "(?:(?<shortSecond>[0-9]{2})(?:.(?<shortFraction>[0-9]{1,9}))?)?)?))";
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
        return LocalTime.of( hour, minute, second, fraction );
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
