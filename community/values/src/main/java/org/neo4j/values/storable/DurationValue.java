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
import java.time.Duration;
import java.time.LocalDate;
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

import org.neo4j.values.AnyValue;
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
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static org.neo4j.values.storable.NumberType.NO_NUMBER;
import static org.neo4j.values.storable.NumberValue.safeCastFloatingPoint;
import static org.neo4j.values.storable.TimeUtil.AVG_DAYS_PER_MONTH;
import static org.neo4j.values.storable.TimeUtil.AVG_SECONDS_PER_MONTH;
import static org.neo4j.values.storable.TimeUtil.NANOS_PER_SECOND;
import static org.neo4j.values.storable.TimeUtil.SECONDS_PER_DAY;

/**
 * We use our own implementation because neither {@link java.time.Duration} nor {@link java.time.Period} fits our needs.
 * {@link java.time.Duration} only works with seconds, assumes 24H days, and is unable to handle larger units than days.
 * {@link java.time.Period} only works with units from days or larger, and does not deal with time.
 */
public final class DurationValue extends ScalarValue implements TemporalAmount, Comparable<DurationValue>
{
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
        seconds += nanos / NANOS_PER_SECOND;
        nanos %= NANOS_PER_SECOND;
        if ( seconds < 0 && nanos > 0 )
        {
            seconds += 1;
            nanos -= NANOS_PER_SECOND;
        }
        else if ( seconds > 0 && nanos < 0 )
        {
            seconds -= 1;
            nanos += NANOS_PER_SECOND;
        }
        return newDuration( months, days, seconds, (int) nanos );
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
                return newDuration( from.until( to, unit ), 0, 0, 0 );
            case DAYS:
                return newDuration( 0, from.until( to, unit ), 0, 0 );
            case SECONDS:
                return durationInSecondsAndNanos( from, to );
            default:
                throw new IllegalArgumentException( "Unsupported unit: " + unit );
            }
        }
        else
        {
            throw new IllegalArgumentException( "Unsupported unit: " + unit );
        }
    }

    static StructureBuilder<AnyValue,DurationValue> builder()
    {
        return new DurationBuilder<AnyValue,DurationValue>()
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

    public abstract static class Compiler<Input> extends DurationBuilder<Input,MethodHandle>
    {
    }

    private static final DurationValue ZERO = new DurationValue( 0, 0, 0, 0 );
    private static final List<TemporalUnit> UNITS = unmodifiableList( asList( MONTHS, DAYS, SECONDS, NANOS ) );
    // This comparator is safe until 292,271,023,045 years. After that, we have an overflow.
    private static final Comparator<DurationValue> COMPARATOR =
            Comparator.comparingLong( DurationValue::averageLengthInSeconds )
                    // nanos are guaranteed to be smaller than NANOS_PER_SECOND
                    .thenComparingLong( d -> d.nanos )
                    // At this point, the durations have the same length and we compare by the individual fields.
                    .thenComparingLong( d -> d.months )
                    .thenComparingLong( d -> d.days )
                    .thenComparingLong( d -> d.seconds );
    private final long months;
    private final long days;
    private final long seconds;
    private final int nanos;

    private static DurationValue newDuration( long months, long days, long seconds, int nanos )
    {
        return seconds == 0 && days == 0 && months == 0 && nanos == 0 // ordered by probability of non-zero
                ? ZERO : new DurationValue( months, days, seconds, nanos );
    }

    private DurationValue( long months, long days, long seconds, long nanos )
    {
        seconds += nanos / NANOS_PER_SECOND;
        nanos %= NANOS_PER_SECOND;
        if ( seconds < 0 && nanos > 0 )
        {
            seconds += 1;
            nanos -= NANOS_PER_SECOND;
        }
        else if ( seconds > 0 && nanos < 0 )
        {
            seconds -= 1;
            nanos += NANOS_PER_SECOND;
        }
        this.months = months;
        this.days = days;
        this.seconds = seconds;
        this.nanos = (int) nanos;
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

    private long averageLengthInSeconds()
    {
        return this.seconds + this.days * SECONDS_PER_DAY + this.months * AVG_SECONDS_PER_MONTH;
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
        int sign = "-".equals( matcher.group( "sign" ) ) ? -1 : 1;
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
                throw new IllegalArgumentException( "months is out of range: " + month );
            }
            months += parseLong( year ) * 12;
            days = parseLong( day );
            if ( days > 31 )
            {
                throw new IllegalArgumentException( "days is out of range: " + day );
            }
        }
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
                throw new IllegalArgumentException( "hours out of range: " + hours );
            }
            if ( minutes > 60 )
            {
                throw new IllegalArgumentException( "minutes out of range: " + minutes );
            }
            if ( seconds > 60 )
            {
                throw new IllegalArgumentException( "seconds out of range: " + seconds );
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
            Period period = Period.between( LocalDate.from( from ), LocalDate.from( to ) );
            months = period.getYears() * 12L + period.getMonths();
            days = period.getDays();
            if ( months != 0 || days != 0 )
            {
                // Adjust in order to get to a point where we can compute the time difference,
                // without having to bother with the length of days (which might differ due to timezone)
                from = from.plus( period );
            }
        }
        // Compute the time difference - which is simple at this point
        // NANOS of a day will never overflow a long
        long nanos = from.until( to, NANOS );
        return newDuration( months, days, nanos / NANOS_PER_SECOND, (int) (nanos % NANOS_PER_SECOND) );
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
        seconds = from.until( to, SECONDS );
        int fromNanos = from.isSupported( NANO_OF_SECOND ) ? from.get( NANO_OF_SECOND ) : 0;
        int toNanos = to.isSupported( NANO_OF_SECOND ) ? to.get( NANO_OF_SECOND ) : 0;
        nanos = toNanos - fromNanos;

        boolean specialZeroSecondCase = seconds == 0 && from.get( SECOND_OF_MINUTE ) != to.get( SECOND_OF_MINUTE );

        if ( nanos < 0 && ( seconds > 0 || specialZeroSecondCase ) )
        {
            nanos = NANOS_PER_SECOND + nanos;
        }
        else if ( nanos > 0 && ( seconds < 0 || specialZeroSecondCase ) )
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
            str.append( 'T' );
            long s = seconds % 3600;
            append( str, seconds / 3600, 'H' );
            append( str, s / 60, 'M' );
            s %= 60;
            if ( s != 0 )
            {
                str.append( s );
                if ( nanos != 0 )
                {
                    nanos( str );
                }
                str.append( 'S' );
            }
            else if ( nanos != 0 )
            {
                if ( nanos < 0 )
                {
                    str.append( '-' );
                }
                str.append( '0' );
                nanos( str );
                str.append( 'S' );
            }
        }
        if ( str.length() == 1 )
        { // this was all zeros (but not ZERO for some reason), ensure well formed output:
            str.append( "T0S" );
        }
        return str.toString();
    }

    private void nanos( StringBuilder str )
    {
        str.append( '.' );
        int n = nanos < 0 ? -nanos : nanos;
        for ( int mod = (int)NANOS_PER_SECOND; mod > 1 && n > 0; n %= mod )
        {
            str.append( n / (mod /= 10) );
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
        throw new UnsupportedTemporalTypeException( "Unsupported unit: " + unit );
    }

    /**
     * In contrast to {@link #get(TemporalUnit)}, this method supports more units, namely:
     *
     * years, hours, minutes, milliseconds, microseconds,
     * monthsOfYear, minutesOfHour, secondsOfMinute, millisecondsOfSecond, microsecondsOfSecond, nanosecondsOfSecond
     */
    public LongValue get( String fieldName )
    {
        long val;
        switch ( fieldName.toLowerCase() )
        {
        case "years":
            val = months / 12;
            break;
        case "months":
            val = months;
            break;
        case "monthsofyear":
            val = months % 12;
            break;
        case "days":
            val = days;
            break;
        case "hours":
            val = seconds / 3600;
            break;
        case "minutesofhour":
            val = (seconds / 60) % 60;
            break;
        case "minutes":
            val = seconds / 60;
            break;
        case "secondsofminute":
            val = seconds % 60;
            break;
        case "seconds":
            val = seconds;
            break;
        case "millisecondsofsecond":
            val = nanos / 1000_000;
            break;
        case "milliseconds":
            val = seconds * 1000 + nanos / 1000_000;
            break;
        case "microsecondsofsecond":
            val = nanos / 1000;
            break;
        case "microseconds":
            val = seconds * 1000_000 + nanos / 1000;
            break;
        case "nanosecondsofsecond":
            val = nanos;
            break;
        case "nanoseconds":
            val = seconds * NANOS_PER_SECOND + nanos;
            break;
        default:
            throw new UnsupportedTemporalTypeException( "No such field: " + fieldName );
        }

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
            temporal = temporal.plus( months, MONTHS );
        }
        if ( days != 0 && temporal.isSupported( DAYS ) )
        {
            temporal = temporal.plus( days, DAYS );
        }
        if ( seconds != 0 )
        {
            if ( temporal.isSupported( SECONDS ) )
            {
                temporal = temporal.plus( seconds, SECONDS );
            }
            else
            {
                long asDays = seconds / SECONDS_PER_DAY;
                if ( asDays != 0 )
                {
                    temporal = temporal.plus( asDays, DAYS );
                }
            }
        }
        if ( nanos != 0 && temporal.isSupported( NANOS ) )
        {
            temporal = temporal.plus( nanos, NANOS );
        }
        return temporal;
    }

    @Override
    public Temporal subtractFrom( Temporal temporal )
    {
        if ( months != 0 && temporal.isSupported( MONTHS ) )
        {
            temporal = temporal.minus( months, MONTHS );
        }
        if ( days != 0 && temporal.isSupported( DAYS ) )
        {
            temporal = temporal.minus( days, DAYS );
        }
        if ( seconds != 0 )
        {
            if ( temporal.isSupported( SECONDS ) )
            {
                temporal = temporal.minus( seconds, SECONDS );
            }
            else if ( temporal.isSupported( DAYS ) )
            {
                long asDays = seconds / SECONDS_PER_DAY;
                if ( asDays != 0 )
                {
                    temporal = temporal.minus( asDays, DAYS );
                }
            }
        }
        if ( nanos != 0 && temporal.isSupported( NANOS ) )
        {
            temporal = temporal.minus( nanos, NANOS );
        }
        return temporal;
    }

    public DurationValue add( DurationValue that )
    {
        return duration(
                this.months + that.months,
                this.days + that.days,
                this.seconds + that.seconds,
                this.nanos + that.nanos );
    }

    public DurationValue sub( DurationValue that )
    {
        return duration(
                this.months - that.months,
                this.days - that.days,
                this.seconds - that.seconds,
                this.nanos - that.nanos );
    }

    public DurationValue mul( NumberValue number )
    {
        if ( number instanceof IntegralValue )
        {
            long factor = number.longValue();
            return duration( months * factor, days * factor, seconds * factor, nanos * factor );
        }
        if ( number instanceof FloatingPointValue )
        {
            double factor = number.doubleValue();
            return approximate( months * factor, days * factor, seconds * factor, nanos * factor );
        }
        throw new IllegalArgumentException( "Factor must be either integer of floating point number." );
    }

    public DurationValue div( NumberValue number )
    {
        double divisor = number.doubleValue();
        return approximate( months / divisor, days / divisor, seconds / divisor, nanos / divisor );
    }

    private static DurationValue approximate( double months, double days, double seconds, double nanos )
    {
        long m = (long) months;
        days += AVG_DAYS_PER_MONTH * (months - m);
        long d = (long) days;
        seconds += SECONDS_PER_DAY * (days - d);
        long s = (long) seconds;
        nanos += NANOS_PER_SECOND * (seconds - s);
        long n = (long) nanos;
        return duration( m, d, s, n );
    }
}
