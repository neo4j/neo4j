/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.UnsupportedTemporalUnitException;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.Comparison;
import org.neo4j.values.Equality;
import org.neo4j.values.StructureBuilder;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.MapValue;

import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.time.temporal.ChronoField.EPOCH_DAY;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.OFFSET_SECONDS;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.storable.NumberType.NO_NUMBER;
import static org.neo4j.values.storable.NumberValue.safeCastFloatingPoint;
import static org.neo4j.values.utils.TemporalUtil.AVG_NANOS_PER_MONTH;
import static org.neo4j.values.utils.TemporalUtil.AVG_SECONDS_PER_MONTH;
import static org.neo4j.values.utils.TemporalUtil.NANOS_PER_SECOND;
import static org.neo4j.values.utils.TemporalUtil.SECONDS_PER_DAY;

/**
 * We use our own implementation because neither {@link java.time.Duration} nor {@link java.time.Period} fits our needs.
 * {@link java.time.Duration} only works with seconds, assumes 24H days, and is unable to handle larger units than days.
 * {@link java.time.Period} only works with units from days or larger, and does not deal with time.
 */
public final class DurationValue extends ScalarValue implements TemporalAmount, Comparable<DurationValue>
{
    static final long SHALLOW_SIZE = shallowSizeOfInstance( DurationValue.class );

    public static final DurationValue MIN_VALUE = duration( 0, 0, Long.MIN_VALUE, 0 );
    public static final DurationValue MAX_VALUE = duration( 0, 0, Long.MAX_VALUE, 999_999_999 );

    public static final DurationValue ZERO = new DurationValue( 0, 0, 0, 0 );
    private static final List<TemporalUnit> UNITS = List.of( MONTHS, DAYS, SECONDS, NANOS );
    // This comparator is safe until 292,271,023,045 years. After that, we have an overflow.
    private static final Comparator<DurationValue> COMPARATOR =
            Comparator.comparingLong( DurationValue::getAverageLengthInSeconds )
                    .thenComparingLong( d -> d.nanos ) // nanos are guaranteed to be smaller than NANOS_PER_SECOND
                    .thenComparingLong( d -> d.months )// At this point, the durations have the same length and we compare by the individual fields.
                    .thenComparingLong( d -> d.days )
                    .thenComparingLong( d -> d.seconds );
    private final long months;
    private final long days;
    private final long seconds;
    private final int nanos;

    private DurationValue( long months, long days, long seconds, long nanos )
    {
        assertNoOverflow( months, days, seconds, nanos );
        seconds = secondsWithNanos( seconds, nanos );
        nanos %= NANOS_PER_SECOND;
        // normalize nanos to be between 0 and NANOS_PER_SECOND-1
        if ( nanos < 0 )
        {
            seconds -= 1;
            nanos += NANOS_PER_SECOND;
        }
        this.months = months;
        this.days = days;
        this.seconds = seconds;
        this.nanos = (int) nanos;
    }

    private static DurationValue newDuration( long months, long days, long seconds, long nanos )
    {
        return seconds == 0 && days == 0 && months == 0 && nanos == 0 // ordered by probability of non-zero
                ? ZERO : new DurationValue( months, days, seconds, nanos );
    }

    public static DurationValue duration( Duration value )
    {
        requireNonNull( value, "Duration" );
        return newDuration( 0, 0, value.getSeconds(), value.getNano() );
    }

    public static DurationValue duration( Period value )
    {
        requireNonNull( value, "Period" );
        return newDuration( value.toTotalMonths(), value.getDays(), 0, 0 );
    }

    public static DurationValue duration( long months, long days, long seconds, long nanos )
    {
        return newDuration( months, days, seconds, nanos );
    }

    public static DurationValue parse( CharSequence text )
    {
        return TemporalValue.parse( DurationValue.class, PATTERN, DurationValue::parse, text );
    }

    public static DurationValue parse( TextValue text )
    {
        return TemporalValue.parse( DurationValue.class, PATTERN, DurationValue::parse, text );
    }

    static DurationValue build( Map<String,? extends AnyValue> input )
    {
        StructureBuilder<AnyValue,DurationValue> builder = builder();
        for ( Map.Entry<String,? extends AnyValue> entry : input.entrySet() )
        {
            builder.add( entry.getKey(), entry.getValue() );
        }
        return builder.build();
    }

    public static DurationValue build( MapValue map )
    {
        return StructureBuilder.build( builder(), map );
    }

    public static DurationValue between( TemporalUnit unit, Temporal from, Temporal to )
    {
        if ( unit == null )
        {
            return durationBetween( from, to );
        }
        else if ( unit instanceof ChronoUnit )
        {
            switch ( (ChronoUnit) unit )
            {
            case MONTHS:
                return newDuration( assertValidUntil( from, to ,unit ), 0, 0, 0 );
            case DAYS:
                return newDuration( 0, assertValidUntil( from, to ,unit ), 0, 0 );
            case SECONDS:
                return durationInSecondsAndNanos( from, to );
            default:
                throw new UnsupportedTemporalUnitException( "Unsupported unit: " + unit );
            }
        }
        else
        {
            throw new UnsupportedTemporalUnitException( "Unsupported unit: " + unit );
        }
    }

    private static StructureBuilder<AnyValue,DurationValue> builder()
    {
        return new DurationBuilder<>()
        {
            @Override
            DurationValue create(
                    AnyValue years,
                    AnyValue months,
                    AnyValue weeks,
                    AnyValue days,
                    AnyValue hours,
                    AnyValue minutes,
                    AnyValue seconds,
                    AnyValue milliseconds,
                    AnyValue microseconds,
                    AnyValue nanoseconds )
            {
                return approximate(
                        safeCastFloatingPoint( "years", years, 0 ) * 12 +
                        safeCastFloatingPoint( "months", months, 0 ),
                        safeCastFloatingPoint( "weeks", weeks, 0 ) * 7 +
                        safeCastFloatingPoint( "days", days, 0 ),
                        safeCastFloatingPoint( "hours", hours, 0 ) * 3600 +
                        safeCastFloatingPoint( "minutes", minutes, 0 ) * 60 +
                        safeCastFloatingPoint( "seconds", seconds, 0 ),
                        safeCastFloatingPoint( "milliseconds", milliseconds, 0 ) * 1_000_000 +
                        safeCastFloatingPoint( "microseconds", microseconds, 0 ) * 1_000 +
                        safeCastFloatingPoint( "nanoseconds", nanoseconds, 0 )
                );
            }
        };
    }

    @Override
    public int compareTo( DurationValue other )
    {
        return COMPARATOR.compare( this, other );
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        return compareTo( (DurationValue) otherValue );
    }

    @Override
    Comparison unsafeTernaryCompareTo( Value other )
    {

        if ( ternaryEquals( other ) == Equality.TRUE )
        {
            return Comparison.EQUAL;
        }
        else
        {
            return Comparison.UNDEFINED;
        }
    }

    @Override
    public long estimatedHeapUsage()
    {
        return SHALLOW_SIZE;
    }

    private long getAverageLengthInSeconds()
    {
        return calcAverageLengthInSeconds( this.months, this.days, this.seconds );
    }

    private long calcAverageLengthInSeconds( long months, long days, long seconds )
    {
        long daysInSeconds = Math.multiplyExact( days, SECONDS_PER_DAY );
        long monthsInSeconds = Math.multiplyExact( months, AVG_SECONDS_PER_MONTH );
        return Math.addExact( seconds, Math.addExact( daysInSeconds, monthsInSeconds ) );
    }

    private long secondsWithNanos( long seconds, long nanos )
    {
        return Math.addExact( seconds, nanos / NANOS_PER_SECOND );
    }

    private void assertNoOverflow( long months, long days, long seconds, long nanos )
    {
        try
        {
            calcAverageLengthInSeconds( months, days, seconds );
            //noinspection ResultOfMethodCallIgnored
            secondsWithNanos( seconds, nanos );
        }
        catch ( java.lang.ArithmeticException | org.neo4j.exceptions.ArithmeticException e )
        {
            throw invalidDuration( months, days, seconds, nanos, e );
        }
    }

    long nanosOfDay()
    {
        return (seconds % SECONDS_PER_DAY) * NANOS_PER_SECOND + nanos;
    }

    long totalMonths()
    {
        return months;
    }

    /**
     * The number of days of this duration, as computed by the days and the whole days made up of seconds. This
     * excludes the days contributed by the months.
     *
     * @return the total number of days of this duration.
     */
    long totalDays()
    {
        return days + (seconds / SECONDS_PER_DAY);
    }

    private static final String UNIT_BASED_PATTERN = "(?:(?<years>[-+]?[0-9]+(?:[.,][0-9]+)?)Y)?"
            + "(?:(?<months>[-+]?[0-9]+(?:[.,][0-9]+)?)M)?"
            + "(?:(?<weeks>[-+]?[0-9]+(?:[.,][0-9]+)?)W)?"
            + "(?:(?<days>[-+]?[0-9]+(?:[.,][0-9]+)?)D)?"
            + "(?<T>T"
            + "(?:(?<hours>[-+]?[0-9]+(?:[.,][0-9]+)?)H)?"
            + "(?:(?<minutes>[-+]?[0-9]+(?:[.,][0-9]+)?)M)?"
            + "(?:(?<seconds>[-+]?[0-9]+)(?:[.,](?<subseconds>[0-9]{1,9}))?S)?)?";
    private static final String DATE_BASED_PATTERN = "(?:"
            + "(?<year>[0-9]{4})(?:"
            + "-(?<longMonth>[0-9]{2})-(?<longDay>[0-9]{2})|"
            + "(?<shortMonth>[0-9]{2})(?<shortDay>[0-9]{2}))"
            + ")?(?<time>T"
            + "(?:(?<shortHour>[0-9]{2})(?:(?<shortMinute>[0-9]{2})"
            + "(?:(?<shortSecond>[0-9]{2})(?:[.,](?<shortSub>[0-9]{1,9}))?)?)?|"
            + "(?<longHour>[0-9]{2}):(?<longMinute>[0-9]{2})"
            + "(?::(?<longSecond>[0-9]{2})(?:[.,](?<longSub>[0-9]{1,9}))?)?))?";
    private static final Pattern PATTERN = Pattern.compile(
            "(?<sign>[-+]?)P(?:" + UNIT_BASED_PATTERN + "|" + DATE_BASED_PATTERN + ")",
            CASE_INSENSITIVE );

    private static DurationValue parse( Matcher matcher )
    {
        String year = matcher.group( "year" );
        String time = matcher.group( "time" );
        if ( year != null || time != null )
        {
            return parseDateDuration( year, matcher, time != null );
        }
        else
        {
            return parseDuration( matcher );
        }
    }

    private static DurationValue parseDuration( Matcher matcher )
    {
        int sign = "-".equals( matcher.group( "sign" ) ) ? -1 : 1;
        String y = matcher.group( "years" );
        String m = matcher.group( "months" );
        String w = matcher.group( "weeks" );
        String d = matcher.group( "days" );
        String t = matcher.group( "T" );
        if ( (y == null && m == null && w == null && d == null && t == null) || "T".equalsIgnoreCase( t ) )
        {
            return null;
        }
        int pos;
        if ( (pos = fractionPoint( y )) >= 0 )
        {
            if ( m != null || w != null || d != null || t != null )
            {
                return null;
            }
            return approximate( parseFractional( y, pos ) * 12, 0, 0, 0 );
        }
        long months = optLong( y ) * 12;
        if ( (pos = fractionPoint( m )) >= 0 )
        {
            if ( w != null || d != null || t != null )
            {
                return null;
            }
            return approximate( months + parseFractional( m, pos ), 0, 0, 0 );
        }
        months += optLong( m );
        if ( (pos = fractionPoint( w )) >= 0 )
        {
            if ( d != null || t != null )
            {
                return null;
            }
            return approximate( months, parseFractional( w, pos ) * 7, 0, 0 );
        }
        long days = optLong( w ) * 7;
        if ( (pos = fractionPoint( d )) >= 0 )
        {
            if ( t != null )
            {
                return null;
            }
            return approximate( months, days + parseFractional( d, pos ), 0, 0 );
        }
        days += optLong( d );
        return parseDuration( sign, months, days, matcher, false, "hours", "minutes", "seconds", "subseconds" );
    }

    private static DurationValue parseDateDuration( String year, Matcher matcher, boolean time )
    {
        long months = 0;
        long days = 0;
        if ( year != null )
        {
            String month = matcher.group( "longMonth" );
            String day;
            if ( month == null )
            {
                month = matcher.group( "shortMonth" );
                day = matcher.group( "shortDay" );
            }
            else
            {
                day = matcher.group( "longDay" );
            }
            months = parseLong( month );
            if ( months > 12 )
            {
                throw new InvalidArgumentException( "months is out of range: " + month );
            }
            months += parseLong( year ) * 12;
            days = parseLong( day );
            if ( days > 31 )
            {
                throw new InvalidArgumentException( "days is out of range: " + day );
            }
        }
        int sign = "-".equals( matcher.group( "sign" ) ) ? -1 : 1;
        if ( time )
        {
            if ( matcher.group( "longHour" ) != null )
            {
                return parseDuration(
                        sign, months, days, matcher, true, "longHour", "longMinute", "longSecond", "longSub" );
            }
            else
            {
                return parseDuration(
                        sign, months, days, matcher, true, "shortHour", "shortMinute", "shortSecond", "shortSub" );
            }
        }
        else
        {
            return duration( sign * months, sign * days, 0, 0 );
        }
    }

    private static DurationValue parseDuration(
            int sign, long months, long days, Matcher matcher, boolean strict,
            String hour, String min, String sec, String sub )
    {
        String h = matcher.group( hour );
        String m = matcher.group( min );
        String s = matcher.group( sec );
        String n = matcher.group( sub );
        if ( !strict )
        {
            int pos;
            if ( (pos = fractionPoint( h )) >= 0 )
            {
                if ( m != null || s != null )
                {
                    return null;
                }
                return approximate( months, days, parseFractional( h, pos ) * 3600, 0 );
            }
            if ( (pos = fractionPoint( m )) >= 0 )
            {
                if ( s != null )
                {
                    return null;
                }
                return approximate( months, days, parseFractional( m, pos ) * 60, 0 );
            }
        }
        long hours = optLong( h );
        long minutes = optLong( m );
        long seconds = optLong( s );
        if ( strict )
        {
            if ( hours > 24 )
            {
                throw new InvalidArgumentException( "hours out of range: " + hours );
            }
            if ( minutes > 60 )
            {
                throw new InvalidArgumentException( "minutes out of range: " + minutes );
            }
            if ( seconds > 60 )
            {
                throw new InvalidArgumentException( "seconds out of range: " + seconds );
            }
        }
        seconds += hours * 3600 + minutes * 60;
        long nanos = optLong( n );
        if ( nanos != 0 )
        {
            for ( int i = n.length(); i < 9; i++ )
            {
                nanos *= 10;
            }
            if ( s.startsWith( "-" ) )
            {
                nanos = -nanos;
            }
        }
        return duration( sign * months, sign * days, sign * seconds, sign * nanos );
    }

    private static double parseFractional( String input, int pos )
    {
        return parseDouble( input.charAt( pos ) == '.' ? input :
                (input.substring( 0, pos ) + "." + input.substring( pos + 1 )) );
    }

    private static int fractionPoint( String field )
    {
        if ( field == null )
        {
            return -1;
        }
        int fractionPoint = field.indexOf( '.' );
        if ( fractionPoint < 0 )
        {
            fractionPoint = field.indexOf( ',' );
        }
        return fractionPoint;
    }

    private static long optLong( String value )
    {
        return value == null ? 0 : parseLong( value );
    }

    static DurationValue durationBetween( Temporal from, Temporal to )
    {
        long months = 0;
        long days = 0;
        if ( from.isSupported( EPOCH_DAY ) && to.isSupported( EPOCH_DAY ) )
        {
            months = assertValidUntil( from, to, ChronoUnit.MONTHS );
            try
            {
                from = from.plus( months, ChronoUnit.MONTHS );
            }
            catch ( DateTimeException | java.lang.ArithmeticException e )
            {
                throw new org.neo4j.exceptions.ArithmeticException( e.getMessage(), e );
            }

            days = assertValidUntil( from, to, ChronoUnit.DAYS );
            try
            {
                from = from.plus( days, ChronoUnit.DAYS );
            }
            catch ( DateTimeException | java.lang.ArithmeticException e )
            {
                throw new org.neo4j.exceptions.ArithmeticException( e.getMessage(), e );
            }
        }
        long nanos = assertValidUntil( from, to, NANOS );
        return newDuration( months, days, nanos / NANOS_PER_SECOND, nanos % NANOS_PER_SECOND );
    }

    private static DurationValue durationInSecondsAndNanos( Temporal from, Temporal to )
    {
        long seconds;
        long nanos;
        boolean negate = false;
        if ( from.isSupported( OFFSET_SECONDS ) && !to.isSupported( OFFSET_SECONDS ) )
        {
            negate = true;
            Temporal tmp = from;
            from = to;
            to = tmp;
        }
        seconds = assertValidUntil( from, to, SECONDS );
        int fromNanos = from.isSupported( NANO_OF_SECOND ) ? from.get( NANO_OF_SECOND ) : 0;
        int toNanos = to.isSupported( NANO_OF_SECOND ) ? to.get( NANO_OF_SECOND ) : 0;
        nanos = toNanos - fromNanos;

        boolean differenceIsLessThanOneSecond = seconds == 0
                && from.isSupported( SECOND_OF_MINUTE )
                && to.isSupported( SECOND_OF_MINUTE )
                && from.get( SECOND_OF_MINUTE ) != to.get( SECOND_OF_MINUTE );

        if ( nanos < 0 && ( seconds > 0 || differenceIsLessThanOneSecond ) )
        {
            nanos = NANOS_PER_SECOND + nanos;
        }
        else if ( nanos > 0 && ( seconds < 0 || differenceIsLessThanOneSecond ) )
        {
            nanos = nanos - NANOS_PER_SECOND;
        }
        if ( negate )
        {
            seconds = -seconds;
            nanos = -nanos;
        }
        return duration( 0, 0, seconds, nanos );
    }

    @Override
    public boolean equals( Value other )
    {
        if ( other instanceof DurationValue )
        {
            DurationValue that = (DurationValue) other;
            return that.months == this.months &&
                    that.days == this.days &&
                    that.seconds == this.seconds &&
                    that.nanos == this.nanos;
        }
        else
        {
            return false;
        }
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeDuration( months, days, seconds, nanos );
    }

    @Override
    public TemporalAmount asObjectCopy()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return prettyPrint();
    }

    @Override
    public String getTypeName()
    {
        return "Duration";
    }

    @Override
    public String prettyPrint()
    {
        if ( this == ZERO )
        {
            return "PT0S"; // no need to allocate a string builder if we know the result
        }
        StringBuilder str = new StringBuilder().append( "P" );
        append( str, months / 12, 'Y' );
        append( str, months % 12, 'M' );
        append( str, days, 'D' );
        if ( seconds != 0 || nanos != 0 )
        {
            boolean negative = seconds < 0;
            long s = seconds;
            int n = nanos;
            if ( negative && nanos != 0 )
            {
                s++;
                n -= NANOS_PER_SECOND;
            }
            str.append( 'T' );
            append( str, s / 3600, 'H' );
            s %= 3600;
            append( str, s / 60, 'M' );
            s %= 60;
            if ( s != 0 )
            {
                if ( negative && s >= 0 && n != 0 )
                {
                    str.append( '-' );
                }
                str.append( s );
                if ( n != 0 )
                {
                    nanos( str, n );
                }
                str.append( 'S' );
            }
            else if ( n != 0 )
            {
                if ( negative )
                {
                    str.append( '-' );
                }
                str.append( '0' );
                nanos( str, n );
                str.append( 'S' );
            }
        }
        if ( str.length() == 1 )
        { // this was all zeros (but not ZERO for some reason), ensure well formed output:
            str.append( "T0S" );
        }
        return str.toString();
    }

    private static void nanos( StringBuilder str, int nanos )
    {
        str.append( '.' );
        int n = nanos < 0 ? -nanos : nanos;
        for ( int mod = (int)NANOS_PER_SECOND; mod > 1 && n > 0; n %= mod )
        {
            mod /= 10;
            str.append( n / mod );
        }
    }

    private static void append( StringBuilder str, long quantity, char unit )
    {
        if ( quantity != 0 )
        {
            str.append( quantity ).append( unit );
        }
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.DURATION;
    }

    @Override
    public NumberType numberType()
    {
        return NO_NUMBER;
    }

    @Override
    protected int computeHash()
    {
        int result = (int) (months ^ (months >>> 32));
        result = 31 * result + (int) (days ^ (days >>> 32));
        result = 31 * result + (int) (seconds ^ (seconds >>> 32));
        result = 31 * result + nanos;
        return result;
    }

    @Override
    public long updateHash( HashFunction hashFunction, long hash )
    {
        hash = hashFunction.update( hash, months );
        hash = hashFunction.update( hash, days );
        hash = hashFunction.update( hash, seconds );
        hash = hashFunction.update( hash, nanos );
        return hash;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapDuration( this );
    }

    @Override
    public long get( TemporalUnit unit )
    {
        if ( unit instanceof ChronoUnit )
        {
            switch ( (ChronoUnit) unit )
            {
            case MONTHS:
                return months;
            case DAYS:
                return days;
            case SECONDS:
                return seconds;
            case NANOS:
                return nanos;
            default:
                break;
            }
        }
        throw new UnsupportedTemporalUnitException( "Unsupported unit: " + unit );
    }

    /**
     * In contrast to {@link #get(TemporalUnit)}, this method supports more units, namely:
     *
     * years, hours, minutes, milliseconds, microseconds,
     * monthsOfYear, minutesOfHour, secondsOfMinute, millisecondsOfSecond, microsecondsOfSecond, nanosecondsOfSecond
     */
    public LongValue get( String fieldName )
    {
        long val = DurationFields.fromName( fieldName ).asTimeStamp( months, days, seconds, nanos );
        return Values.longValue( val );
    }

    @Override
    public List<TemporalUnit> getUnits()
    {
        return UNITS;
    }

    public DurationValue plus( long amount, TemporalUnit unit )
    {
        if ( unit instanceof ChronoUnit )
        {
            switch ( (ChronoUnit) unit )
            {
            case NANOS:
                return duration( months, days, seconds, nanos + amount );
            case MICROS:
                return duration( months, days, seconds, nanos + amount * 1000 );
            case MILLIS:
                return duration( months, days, seconds, nanos + amount * 1000_000 );
            case SECONDS:
                return duration( months, days, seconds + amount, nanos );
            case MINUTES:
                return duration( months, days, seconds + amount * 60, nanos );
            case HOURS:
                return duration( months, days, seconds + amount * 3600, nanos );
            case HALF_DAYS:
                return duration( months, days, seconds + amount * 12 * 3600, nanos );
            case DAYS:
                return duration( months, days + amount, seconds, nanos );
            case WEEKS:
                return duration( months, days + amount * 7, seconds, nanos );
            case MONTHS:
                return duration( months + amount, days, seconds, nanos );
            case YEARS:
                return duration( months + amount * 12, days, seconds, nanos );
            case DECADES:
                return duration( months + amount * 120, days, seconds, nanos );
            case CENTURIES:
                return duration( months + amount * 1200, days, seconds, nanos );
            case MILLENNIA:
                return duration( months + amount * 12000, days, seconds, nanos );
            default:
                break;
            }
        }
        throw new UnsupportedOperationException( "Unsupported unit: " + unit );
    }

    @Override
    public Temporal addTo( Temporal temporal )
    {
        if ( months != 0 && temporal.isSupported( MONTHS ) )
        {
            temporal = assertValidPlus( temporal, months, MONTHS );
        }
        if ( days != 0 && temporal.isSupported( DAYS ) )
        {
            temporal = assertValidPlus( temporal, days, DAYS );
        }
        if ( seconds != 0 )
        {
            if ( temporal.isSupported( SECONDS ) )
            {
                temporal = assertValidPlus( temporal, seconds, SECONDS );
            }
            else
            {
                long asDays = seconds / SECONDS_PER_DAY;
                if ( asDays != 0 )
                {
                    temporal = assertValidPlus( temporal, asDays, DAYS );
                }
            }
        }
        if ( nanos != 0 && temporal.isSupported( NANOS ) )
        {
            temporal = assertValidPlus( temporal, nanos, NANOS );
        }
        return temporal;
    }

    @Override
    public Temporal subtractFrom( Temporal temporal )
    {
        if ( months != 0 && temporal.isSupported( MONTHS ) )
        {
            temporal = assertValidMinus( temporal, months, MONTHS );
        }
        if ( days != 0 && temporal.isSupported( DAYS ) )
        {
            temporal = assertValidMinus( temporal, days, DAYS );
        }
        if ( seconds != 0 )
        {
            if ( temporal.isSupported( SECONDS ) )
            {
                temporal = assertValidMinus( temporal, seconds, SECONDS );
            }
            else if ( temporal.isSupported( DAYS ) )
            {
                long asDays = seconds / SECONDS_PER_DAY;
                if ( asDays != 0 )
                {
                    temporal = assertValidMinus( temporal, asDays, DAYS );
                }
            }
        }
        if ( nanos != 0 && temporal.isSupported( NANOS ) )
        {
            temporal = assertValidMinus( temporal, nanos, NANOS );
        }
        return temporal;
    }

    public DurationValue add( DurationValue that )
    {
        try
        {
            return duration(
                    Math.addExact( this.months, that.months ),
                    Math.addExact( this.days, that.days ),
                    Math.addExact( this.seconds, that.seconds ),
                    Math.addExact( this.nanos, that.nanos ) );
        }
        catch ( java.lang.ArithmeticException | org.neo4j.exceptions.ArithmeticException e )
        {
            throw invalidDurationAdd( this, that, e );
        }
    }

    public DurationValue sub( DurationValue that )
    {
        try
        {
            return duration(
                    Math.subtractExact( this.months, that.months ),
                    Math.subtractExact( this.days, that.days ),
                    Math.subtractExact( this.seconds, that.seconds ),
                    Math.subtractExact( this.nanos, that.nanos ) );
        }
        catch ( java.lang.ArithmeticException | org.neo4j.exceptions.ArithmeticException e )
        {
            throw invalidDurationSubtract( this, that, e );
        }
    }

    public DurationValue mul( NumberValue number )
    {
        try
        {
            if ( number instanceof IntegralValue )
            {
                long factor = number.longValue();
                return duration(
                        Math.multiplyExact( months, factor ),
                        Math.multiplyExact( days, factor ),
                        Math.multiplyExact( seconds, factor ),
                        Math.multiplyExact( nanos, factor ) );
            }
            if ( number instanceof FloatingPointValue )
            {
                double factor = number.doubleValue();
                return approximate( months * factor, days * factor, seconds * factor, nanos * factor );
            }
        }
        catch ( java.lang.ArithmeticException | org.neo4j.exceptions.ArithmeticException e )
        {
            throw invalidDurationMultiply( this, number, e );
        }
        throw new InvalidArgumentException( "Factor must be either integer of floating point number." );
    }

    public DurationValue div( NumberValue number )
    {
        double divisor = number.doubleValue();
        try
        {
            return approximate( months / divisor, days / divisor, seconds / divisor, nanos / divisor );
        }
        catch ( java.lang.ArithmeticException | org.neo4j.exceptions.ArithmeticException e )
        {
            throw invalidDurationDivision( this, number, e );
        }
    }

    /**
     * Returns an approximation of the provided values by rounding to whole units and recalculating
     * the remainder into the smaller units.
     */
    public static DurationValue approximate( double months, double days, double seconds, double nanos )
    {
        long monthsAsLong = safeDoubleToLong(months);

        double monthDiffInNanos = AVG_NANOS_PER_MONTH * months - AVG_NANOS_PER_MONTH * monthsAsLong;
        days += monthDiffInNanos / (NANOS_PER_SECOND * SECONDS_PER_DAY);
        long daysAsLong = safeDoubleToLong(days);

        double daysDiffInNanos = NANOS_PER_SECOND * SECONDS_PER_DAY * days - NANOS_PER_SECOND * SECONDS_PER_DAY * daysAsLong;
        seconds += daysDiffInNanos / NANOS_PER_SECOND;
        long secondsAsLong = safeDoubleToLong(seconds);

        double secondsDiffInNanos = NANOS_PER_SECOND * seconds - NANOS_PER_SECOND * secondsAsLong;
        nanos += secondsDiffInNanos;
        long nanosAsLong = safeDoubleToLong(nanos);

        return duration( monthsAsLong, daysAsLong, secondsAsLong, nanosAsLong );
    }

    /**
     * Normalize parts so that
     *
     *   nanos < NANOS_PER_SECOND
     *   seconds < SECONDS_PER_DAY
     *
     * Days are not normalized to months because the big variance in days/month.
     */
    public DurationValue normalize()
    {
        long d = days;
        long s = seconds;
        long n = nanos;

        if ( n < NANOS_PER_SECOND && s < SECONDS_PER_DAY )
        {
            return this;
        }

        if ( n >= NANOS_PER_SECOND )
        {
            long extraSeconds = n / NANOS_PER_SECOND;
            s += extraSeconds;
            n -= extraSeconds * NANOS_PER_SECOND;
        }

        if ( s >= SECONDS_PER_DAY )
        {
            long extraDays = s / SECONDS_PER_DAY;
            d += extraDays;
            s -= extraDays * SECONDS_PER_DAY;
        }

        return duration( months, d, s, n );
    }

    /**
     * Will cast a double to a long, but only if it is inside the limits of [Long.MIN_VALUE, LONG.MAX_VALUE]
     * We need this to detect overflow errors, whereas normal truncation is OK while approximating.
     */
    private static long safeDoubleToLong( double d )
    {
        if ( d > Long.MAX_VALUE || d < Long.MIN_VALUE )
        {
            throw new org.neo4j.exceptions.ArithmeticException( "long overflow" );
        }
        return (long) d;
    }

    private static Temporal assertValidPlus( Temporal temporal, long amountToAdd, TemporalUnit unit )
    {
        try
        {
            return temporal.plus(amountToAdd,  unit);
        }
        catch ( DateTimeException | java.lang.ArithmeticException e )
        {
            throw new org.neo4j.exceptions.ArithmeticException( e.getMessage(), e );
        }
    }

    private static Temporal assertValidMinus( Temporal temporal, long amountToAdd, TemporalUnit unit )
    {
        try
        {
            return temporal.minus(amountToAdd,  unit);
        }
        catch ( DateTimeException | java.lang.ArithmeticException e )
        {
            throw new org.neo4j.exceptions.ArithmeticException( e.getMessage(), e );
        }
    }

    private static long assertValidUntil( Temporal from, Temporal to, TemporalUnit unit )
    {
        try
        {
            return from.until( to, unit );
        }
        catch ( UnsupportedTemporalTypeException e )
        {
            throw new UnsupportedTemporalUnitException( e.getMessage(), e );
        }
        catch ( DateTimeException e )
        {
            throw new InvalidArgumentException( e.getMessage(), e );
        }
    }

    private InvalidArgumentException invalidDuration( long months, long days, long seconds, long nanos, Exception e )
    {
        return new InvalidArgumentException(
                String.format( "Invalid value for duration, will cause overflow. Value was months=%d, days=%d, seconds=%d, nanos=%d",
                        months, days, seconds, nanos ), e );
    }

    private InvalidArgumentException invalidDurationAdd( DurationValue o1, DurationValue o2, Exception e )
    {
        return new InvalidArgumentException(
                String.format( "Can not add duration %s and %s without causing overflow.", o1, o2 ), e );
    }

    private InvalidArgumentException invalidDurationSubtract( DurationValue o1, DurationValue o2, Exception e )
    {
        return new InvalidArgumentException(
                String.format( "Can not subtract duration %s and %s without causing overflow.", o1, o2 ), e );
    }

    private InvalidArgumentException invalidDurationMultiply( DurationValue o1, NumberValue numberValue, Exception e )
    {
        return new InvalidArgumentException(
                String.format( "Can not multiply duration %s with %s without causing overflow.", o1, numberValue ), e );
    }

    private InvalidArgumentException invalidDurationDivision( DurationValue o1, NumberValue numberValue, Exception e )
    {
        return new InvalidArgumentException(
                String.format( "Can not divide duration %s with %s without causing overflow.", o1, numberValue ), e );
    }
}
