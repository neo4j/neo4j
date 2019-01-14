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
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.values.StructureBuilder;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.utils.InvalidValuesArgumentException;
import org.neo4j.values.utils.UnsupportedTemporalUnitException;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static org.neo4j.util.FeatureToggles.flag;
import static org.neo4j.values.storable.DateTimeValue.parseZoneName;
import static org.neo4j.values.storable.IntegralValue.safeCastIntegral;

public final class DateValue extends TemporalValue<LocalDate,DateValue>
{
    public static final DateValue MIN_VALUE = new DateValue( LocalDate.MIN );
    public static final DateValue MAX_VALUE = new DateValue( LocalDate.MAX );

    public static DateValue date( LocalDate value )
    {
        return new DateValue( requireNonNull( value, "LocalDate" ) );
    }

    public static DateValue date( int year, int month, int day )
    {
        return new DateValue( assertValidArgument( () -> LocalDate.of( year, month, day ) ) );
    }

    public static DateValue weekDate( int year, int week, int dayOfWeek )
    {
        return new DateValue( assertValidArgument( () -> localWeekDate( year, week, dayOfWeek ) ) );
    }

    public static DateValue quarterDate( int year, int quarter, int dayOfQuarter )
    {
        return new DateValue( assertValidArgument( () -> localQuarterDate( year, quarter, dayOfQuarter ) ) );
    }

    public static DateValue ordinalDate( int year, int dayOfYear )
    {
        return new DateValue( assertValidArgument( () -> LocalDate.ofYearDay( year, dayOfYear ) ) );
    }

    public static DateValue epochDate( long epochDay )
    {
        return new DateValue( assertValidArgument( () -> LocalDate.ofEpochDay( epochDay ) ) );
    }

    public static DateValue parse( CharSequence text )
    {
        return parse( DateValue.class, PATTERN, DateValue::parse, text );
    }

    public static DateValue parse( TextValue text )
    {
        return parse( DateValue.class, PATTERN, DateValue::parse, text );
    }

    public static DateValue now( Clock clock )
    {
        return new DateValue( LocalDate.now( clock ) );
    }

    public static DateValue now( Clock clock, String timezone )
    {
        return now( clock.withZone( parseZoneName( timezone ) ) );
    }

    public static DateValue now( Clock clock, Supplier<ZoneId> defaultZone )
    {
        return now( clock.withZone( defaultZone.get() ) );
    }

    public static DateValue build( MapValue map, Supplier<ZoneId> defaultZone )
    {
        return StructureBuilder.build( builder( defaultZone ), map );
    }

    public static DateValue select( org.neo4j.values.AnyValue from, Supplier<ZoneId> defaultZone )
    {
        return builder( defaultZone ).selectDate( from );
    }

    public static DateValue truncate(
            TemporalUnit unit,
            TemporalValue input,
            MapValue fields,
            Supplier<ZoneId> defaultZone )
    {
        LocalDate localDate = input.getDatePart();
        DateValue truncated = date( truncateTo( localDate, unit ) );
        if ( fields.size() == 0 )
        {
            return truncated;
        }
        else
        {
            MapValue updatedFields = VirtualValues.copy( fields, Pair.of( "date", truncated ) );
            return build( updatedFields, defaultZone );
        }
    }

    static LocalDate truncateTo( LocalDate value, TemporalUnit unit )
    {
        if ( unit == ChronoUnit.MILLENNIA )
        {
            return value.with( Neo4JTemporalField.YEAR_OF_MILLENNIUM, 0 );
        }
        else if ( unit == ChronoUnit.CENTURIES )
        {
            return value.with( Neo4JTemporalField.YEAR_OF_CENTURY, 0 );
        }
        else if ( unit == ChronoUnit.DECADES )
        {
            return value.with( Neo4JTemporalField.YEAR_OF_DECADE, 0 );
        }
        else if ( unit == ChronoUnit.YEARS )
        {
            return value.with( TemporalAdjusters.firstDayOfYear() );
        }
        else if ( unit == IsoFields.WEEK_BASED_YEARS )
        {
            return value.with( IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1 ).with( ChronoField.DAY_OF_WEEK, 1 );
        }
        else if ( unit == IsoFields.QUARTER_YEARS )
        {
            return value.with( IsoFields.DAY_OF_QUARTER, 1 );
        }
        else if ( unit == ChronoUnit.MONTHS )
        {
            return value.with( TemporalAdjusters.firstDayOfMonth() );
        }
        else if ( unit == ChronoUnit.WEEKS )
        {
            return value.with( TemporalAdjusters.previousOrSame( DayOfWeek.MONDAY ) );
        }
        else if ( unit == ChronoUnit.DAYS )
        {
            return value;
        }
        else
        {
            throw new UnsupportedTemporalUnitException( "Unit too small for truncation: " + unit );
        }
    }

    static DateBuilder builder( Supplier<ZoneId> defaultZone )
    {
        return new DateBuilder( defaultZone );
    }

    private final LocalDate value;

    private DateValue( LocalDate value )
    {
        this.value = value;
    }

    @Override
    int unsafeCompareTo( Value otherValue )
    {
        DateValue other = (DateValue) otherValue;
        return value.compareTo( other.value );
    }

    @Override
    public String getTypeName()
    {
        return "Date";
    }

    @Override
    LocalDate temporal()
    {
        return value;
    }

    @Override
    LocalDate getDatePart()
    {
        return value;
    }

    @Override
    LocalTime getLocalTimePart()
    {
        throw new UnsupportedTemporalUnitException( String.format( "Cannot get the time of: %s", this ) );
    }

    @Override
    OffsetTime getTimePart( Supplier<ZoneId> defaultZone )
    {
        throw new UnsupportedTemporalUnitException( String.format( "Cannot get the time of: %s", this ) );
    }

    @Override
    ZoneId getZoneId( Supplier<ZoneId> defaultZone )
    {
        throw new UnsupportedTemporalUnitException( String.format( "Cannot get the time zone of: %s", this ) );
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
        return false;
    }

    @Override
    public boolean equals( Value other )
    {
        return other instanceof DateValue && value.equals( ((DateValue) other).value );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeDate( value );
    }

    @Override
    public String prettyPrint()
    {
        return assertPrintable( () -> value.format( DateTimeFormatter.ISO_DATE ) );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.DATE;
    }

    @Override
    protected int computeHash()
    {
        return Long.hashCode( value.toEpochDay() );
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapDate( this );
    }

    @Override
    public DateValue add( DurationValue duration )
    {
        return replacement( assertValidArithmetic(
                () -> value.plusMonths( duration.totalMonths() ).plusDays( duration.totalDays() ) ) );
    }

    @Override
    public DateValue sub( DurationValue duration )
    {
        return replacement( assertValidArithmetic(
                () -> value.minusMonths( duration.totalMonths() ).minusDays( duration.totalDays() ) ) );
    }

    @Override
    DateValue replacement( LocalDate date )
    {
        return date == value ? this : new DateValue( date );
    }

    static final boolean QUARTER_DATES = flag( DateValue.class, "QUARTER_DATES", true );
    /**
     * The regular expression pattern for parsing dates. All fields come in two versions - long and short form, the
     * long form is for formats containing dashes, the short form is for formats without dashes. The long format is
     * the only one that handles signed years, since that is how the only case that supports years with other than 4
     * numbers, and not having dashes then would make the format ambiguous. In order to not have two cases that can
     * parse only the year, we let the long case handle that.
     * <p/>
     * Valid formats:
     * <ul>
     * <li>Year:<ul>
     * <li>{@code [0-9]{4}} - unique without dashes, since it is the only one 4 numbers long<br>
     * Parsing: {@code longYear}</li>
     * <li>{@code [+-] [0-9]{1,9}}<br>
     * Parsing: {@code longYear}</li>
     * </ul></li>
     * <li>Year & Month:<ul>
     * <li>{@code [0-9]{4} [0-9]{2}} - unique without dashes, since it is the only one 6 numbers long<br>
     * Parsing: {@code shortYear, shortMonth}</li>
     * <li>{@code [0-9]{4} - [0-9]{1,2}}<br>
     * Parsing: {@code longYear, longMonth}</li>
     * <li>{@code [+-] [0-9]{1,9} - [0-9]{1,2}}<br>
     * Parsing: {@code longYear, longMonth}</li>
     * </ul></li>
     * <li>Calendar date (Year & Month & Day):<ul>
     * <li>{@code [0-9]{4} [0-9]{2} [0-9]{2}} - unique without dashes, since it is the only one 8 numbers long<br>
     * Parsing: {@code shortYear, shortMonth, shortDay}</li>
     * <li>{@code [0-9]{4} - [0-9]{1,2} - [0-9]{1,2}}<br>
     * Parsing: {@code longYear, longMonth, longDay}</li>
     * <li>{@code [+-] [0-9]{1,9} - [0-9]{1,2} - [0-9]{1,2}}<br>
     * Parsing: {@code longYear, longMonth, longDay}</li>
     * </ul></li>
     * <li>Year & Week:<ul>
     * <li>{@code [0-9]{4} W [0-9]{2}}<br>
     * Parsing: {@code shortYear, shortWeek}</li>
     * <li>{@code [0-9]{4} - W [0-9]{2}}<br>
     * Parsing: {@code longYear, longWeek}</li>
     * <li>{@code [+-] [0-9]{1,9} - W [0-9]{2}}<br>
     * Parsing: {@code longYear, longWeek}</li>
     * </ul></li>
     * <li>Week date (year & week & day of week):<ul>
     * <li>{@code [0-9]{4} W [0-9]{2} [0-9]} - unique without dashes, contains W followed by 2 numbers<br>
     * Parsing: {@code shortYear, shortWeek, shortDOW}</li>
     * <li>{@code [0-9]{4} - W [0-9]{1,2} - [0-9]}<br>
     * Parsing: {@code longYear, longWeek, longDOW}</li>
     * <li>{@code [+-] [0-9]{1,9} - W [0-9]{2} - [0-9]}<br>
     * Parsing: {@code longYear, longWeek, longDOW}</li>
     * </ul></li>
     * <li>Ordinal date (year & day of year):<ul>
     * <li>{@code [0-9]{4} [0-9]{3}} - unique without dashes, since it is the only one 7 number long<br>
     * Parsing: {@code shortYear, shortDOY}</li>
     * <li>{@code [0-9]{4} - [0-9]{3}} - needs to be exactly 3 numbers long to distinguish from Year & Month<br>
     * Parsing: {@code longYear, longDOY}</li>
     * <li>{@code [+-] [0-9]{1,9} - [0-9]{3}} - needs to be exactly 3 numbers long to distinguish from Year & Month<br>
     * Parsing: {@code longYear, longDOY}</li>
     * </ul></li>
     * </ul>
     */
    static final String DATE_PATTERN = "(?:"
                                       // short formats - without dashes:
                                       + "(?<shortYear>[0-9]{4})(?:"
                                       + "(?<shortMonth>[0-9]{2})(?<shortDay>[0-9]{2})?|" // calendar date
                                       + "W(?<shortWeek>[0-9]{2})(?<shortDOW>[0-9])?|" // week date
                                       + (QUARTER_DATES ? "Q(?<shortQuarter>[0-9])(?<shortDOQ>[0-9]{2})?|" : "")
                                       // quarter date
                                       + "(?<shortDOY>[0-9]{3}))" + "|" // ordinal date
                                       // long formats - includes dashes:
                                       + "(?<longYear>(?:[0-9]{4}|[+-][0-9]{1,9}))(?:"
                                       + "-(?<longMonth>[0-9]{1,2})(?:-(?<longDay>[0-9]{1,2}))?|" // calendar date
                                       + "-W(?<longWeek>[0-9]{1,2})(?:-(?<longDOW>[0-9]))?|" // week date
                                       + (QUARTER_DATES ? "-Q(?<longQuarter>[0-9])(?:-(?<longDOQ>[0-9]{1,2}))?|" : "")
                                       // quarter date
                                       + "-(?<longDOY>[0-9]{3}))?" + ")"; // ordinal date
    private static final Pattern PATTERN = Pattern.compile( DATE_PATTERN );

    /**
     * Creates a {@link LocalDate} from a {@link Matcher} that matches the regular expression defined by
     * {@link #DATE_PATTERN}. The decision tree in the implementation of this method is guided by the parsing notes
     * for {@link #DATE_PATTERN}.
     *
     * @param matcher a {@link Matcher} that matches the regular expression defined in {@link #DATE_PATTERN}.
     * @return a {@link LocalDate} parsed from the given {@link Matcher}.
     */
    static LocalDate parseDate( Matcher matcher )
    {
        String longYear = matcher.group( "longYear" );
        if ( longYear != null )
        {
            return parse( matcher, parseInt( longYear ),
                    "longMonth", "longDay", "longWeek", "longDOW", "longQuarter", "longDOQ", "longDOY" );
        }
        else
        {
            return parse( matcher, parseInt( matcher.group( "shortYear" ) ),
                    "shortMonth", "shortDay", "shortWeek", "shortDOW", "shortQuarter", "shortDOQ", "shortDOY" );
        }
    }

    private static LocalDate parse(
            Matcher matcher, int year,
            String MONTH, String DAY, String WEEK, String DOW, String QUARTER, String DOQ, String DOY )
    {
        String month = matcher.group( MONTH );
        if ( month != null )
        {
            return assertParsable( () -> LocalDate.of( year, parseInt( month ), optInt( matcher.group( DAY ) ) ) );
        }
        String week = matcher.group( WEEK );
        if ( week != null )
        {
            return assertParsable( () -> localWeekDate( year, parseInt( week ), optInt( matcher.group( DOW ) ) ) );
        }
        String quarter = matcher.group( QUARTER );
        if ( quarter != null )
        {
            return assertParsable(
                    () -> localQuarterDate( year, parseInt( quarter ), optInt( matcher.group( DOQ ) ) ) );
        }
        String doy = matcher.group( DOY );
        if ( doy != null )
        {
            return assertParsable( () -> LocalDate.ofYearDay( year, parseInt( doy ) ) );
        }
        return assertParsable( () -> LocalDate.of( year, 1, 1 ) );
    }

    private static DateValue parse( Matcher matcher )
    {
        return new DateValue( parseDate( matcher ) );
    }

    private static int optInt( String value )
    {
        return value == null ? 1 : parseInt( value );
    }

    private static LocalDate localWeekDate( int year, int week, int dayOfWeek )
    {
        LocalDate weekOne = LocalDate.of( year, 1, 4 ); // the fourth is guaranteed to be in week 1 by definition
        LocalDate withWeek = weekOne.with( IsoFields.WEEK_OF_WEEK_BASED_YEAR, week );
        // the implementation of WEEK_OF_WEEK_BASED_YEAR uses addition to adjust the date, this means that it accepts
        // week 53 of years that don't have 53 weeks, so we have to guard for this:
        if ( week == 53 && withWeek.get( IsoFields.WEEK_BASED_YEAR ) != year )
        {
            throw new InvalidValuesArgumentException(
                    String.format( "Year %d does not contain %d weeks.", year, week ) );
        }
        return withWeek.with( ChronoField.DAY_OF_WEEK, dayOfWeek );
    }

    private static LocalDate localQuarterDate( int year, int quarter, int dayOfQuarter )
    {
        // special handling for the range of Q1 and Q2, since they are shorter than Q3 and Q4
        if ( quarter == 2 && dayOfQuarter == 92 )
        {
            throw new InvalidValuesArgumentException( "Quarter 2 only has 91 days." );
        }
        // instantiate the yearDate now, because we use it to know if it is a leap year
        LocalDate yearDate = LocalDate.ofYearDay( year, dayOfQuarter ); // guess on the day
        if ( quarter == 1 && dayOfQuarter > 90 && (!yearDate.isLeapYear() || dayOfQuarter == 92) )
        {
            throw new InvalidValuesArgumentException( String.format(
                    "Quarter 1 of %d only has %d days.", year, yearDate.isLeapYear() ? 91 : 90 ) );
        }
        return yearDate
                .with( IsoFields.QUARTER_OF_YEAR, quarter )
                .with( IsoFields.DAY_OF_QUARTER, dayOfQuarter );
    }

    static final LocalDate DEFAULT_CALENDER_DATE = LocalDate
            .of( TemporalFields.year.defaultValue, TemporalFields.month.defaultValue, TemporalFields.day.defaultValue );

    private static class DateBuilder extends Builder<DateValue>
    {
        @Override
        protected boolean supportsTimeZone()
        {
            return false;
        }

        @Override
        protected boolean supportsEpoch()
        {
            return false;
        }

        DateBuilder( Supplier<ZoneId> defaultZone )
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
            return false;
        }

        private LocalDate getDateOf( org.neo4j.values.AnyValue temporal )
        {
            if ( temporal instanceof TemporalValue )
            {
                TemporalValue v = (TemporalValue) temporal;
                return v.getDatePart();
            }
            throw new InvalidValuesArgumentException( String.format( "Cannot construct date from: %s", temporal ) );
        }

        @Override
        public DateValue buildInternal()
        {
            LocalDate result;
            if ( fields.containsKey( TemporalFields.date ) )
            {
                result = getDateOf( fields.get( TemporalFields.date ) );
            }
            else if ( fields.containsKey( TemporalFields.week ) )
            {
                // Be sure to be in the start of the week based year (which can be later than 1st Jan)
                result = DEFAULT_CALENDER_DATE
                        .with( IsoFields.WEEK_BASED_YEAR,
                                safeCastIntegral( TemporalFields.year.name(), fields.get( TemporalFields.year ),
                                        TemporalFields.year.defaultValue ) )
                        .with( IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1 )
                        .with( ChronoField.DAY_OF_WEEK, 1 );
            }
            else
            {
                result = DEFAULT_CALENDER_DATE;
            }
            result = assignAllFields( result );
            return date( result );
        }

        DateValue selectDate( org.neo4j.values.AnyValue date )
        {
            if ( date instanceof DateValue )
            {
                return (DateValue) date;
            }
            return date( getDateOf( date ) );
        }
    }
}
