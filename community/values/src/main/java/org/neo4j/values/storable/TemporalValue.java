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

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;
import java.time.temporal.ValueRange;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.values.AnyValue;
import org.neo4j.values.StructureBuilder;

import static org.neo4j.values.storable.DateTimeValue.parseZoneName;
import static org.neo4j.values.storable.IntegralValue.safeCastIntegral;
import static org.neo4j.values.storable.NumberType.NO_NUMBER;

public abstract class TemporalValue<T extends Temporal, V extends TemporalValue<T,V>>
        extends ScalarValue implements Temporal
{
    TemporalValue()
    {
        // subclasses are confined to this package,
        // but type-checking is valuable to be able to do outside
        // (therefore the type itself is public)
    }

    private static final String DEFAULT_WHEN = "statement";

    public abstract TemporalValue add( DurationValue duration );

    public abstract TemporalValue sub( DurationValue duration );

    abstract T temporal();

    abstract LocalDate getDatePart();

    abstract LocalTime getLocalTimePart();

    abstract OffsetTime getTimePart( Supplier<ZoneId> defaultZone );

    abstract ZoneId getZoneId( Supplier<ZoneId> defaultZone );

    abstract boolean hasTimeZone();

    abstract V replacement( T temporal );

    @Override
    public final T asObjectCopy()
    {
        return temporal();
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V with( TemporalAdjuster adjuster )
    {
        return replacement( (T) temporal().with( adjuster ) );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V plus( TemporalAmount amount )
    {
        return replacement( (T) temporal().plus( amount ) );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V minus( TemporalAmount amount )
    {
        return replacement( (T) temporal().minus( amount ) );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V minus( long amountToSubtract, TemporalUnit unit )
    {
        return replacement( (T) temporal().minus( amountToSubtract, unit ) );
    }

    @Override
    public final boolean isSupported( TemporalUnit unit )
    {
        return temporal().isSupported( unit );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V with( TemporalField field, long newValue )
    {
        return replacement( (T) temporal().with( field, newValue ) );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public final V plus( long amountToAdd, TemporalUnit unit )
    {
        return replacement( (T) temporal().plus( amountToAdd, unit ) );
    }

    @Override
    public final long until( Temporal endExclusive, TemporalUnit unit )
    {
        return temporal().until( endExclusive, unit );
    }

    @Override
    public final ValueRange range( TemporalField field )
    {
        return temporal().range( field );
    }

    @Override
    public final int get( TemporalField field )
    {
        return temporal().get( field );
    }

    @Override
    public <R> R query( TemporalQuery<R> query )
    {
        return temporal().query( query );
    }

    @Override
    public final boolean isSupported( TemporalField field )
    {
        return temporal().isSupported( field );
    }

    @Override
    public final long getLong( TemporalField field )
    {
        return temporal().getLong( field );
    }

    @Override
    public final NumberType numberType()
    {
        return NO_NUMBER;
    }

    @Override
    public final String toString()
    {
        return getClass().getSimpleName() + "<" + prettyPrint() + ">";
    }

    @Override
    public final boolean equals( boolean x )
    {
        return false;
    }

    @Override
    public final boolean equals( long x )
    {
        return false;
    }

    @Override
    public final boolean equals( double x )
    {
        return false;
    }

    @Override
    public final boolean equals( char x )
    {
        return false;
    }

    @Override
    public final boolean equals( String x )
    {
        return false;
    }

    static <VALUE> VALUE parse( Class<VALUE> type, Pattern pattern, Function<Matcher,VALUE> parser, CharSequence text )
    {
        Matcher matcher = pattern.matcher( text );
        VALUE result = matcher.matches() ? parser.apply( matcher ) : null;
        if ( result == null )
        {
            throw new DateTimeParseException(
                    "Text cannot be parsed to a " + valueName( type ), text, 0 );
        }
        return result;
    }

    static <VALUE> VALUE parse( Class<VALUE> type, Pattern pattern, Function<Matcher,VALUE> parser, TextValue text )
    {
        Matcher matcher = text.matcher( pattern );
        VALUE result = matcher != null && matcher.matches() ? parser.apply( matcher ) : null;
        if ( result == null )
        {
            throw new DateTimeParseException(
                    "Text cannot be parsed to a " + valueName( type ), text.stringValue(), 0 );
        }
        return result;
    }

    static <VALUE> VALUE parse(
            Class<VALUE> type,
            Pattern pattern,
            BiFunction<Matcher,Supplier<ZoneId>,VALUE> parser,
            CharSequence text,
            Supplier<ZoneId> defaultZone )
    {
        Matcher matcher = pattern.matcher( text );
        VALUE result = matcher.matches() ? parser.apply( matcher, defaultZone ) : null;
        if ( result == null )
        {
            throw new DateTimeParseException(
                    "Text cannot be parsed to a " + valueName( type ), text, 0 );
        }
        return result;
    }

    static <VALUE> VALUE parse(
            Class<VALUE> type,
            Pattern pattern,
            BiFunction<Matcher,Supplier<ZoneId>,VALUE> parser,
            TextValue text,
            Supplier<ZoneId> defaultZone )
    {
        Matcher matcher = text.matcher( pattern );
        VALUE result = matcher != null && matcher.matches() ? parser.apply( matcher, defaultZone ) : null;
        if ( result == null )
        {
            throw new DateTimeParseException(
                    "Text cannot be parsed to a " + valueName( type ), text.stringValue(), 0 );
        }
        return result;
    }

    private static <VALUE> String valueName( Class<VALUE> type )
    {
        String name = type.getSimpleName();
        return name.substring( 0, name.length() - /*"Value" is*/5/*characters*/ );
    }

    abstract static class Builder<Result> implements StructureBuilder<AnyValue,Result>
    {
        private final Supplier<ZoneId> defaultZone;
        private DateTimeBuilder state;
        protected AnyValue timezone;

        protected Map<Field,AnyValue> fields = new EnumMap<>( Field.class );

        Builder( Supplier<ZoneId> defaultZone )
        {
            this.defaultZone = defaultZone;
        }

        @Override
        public final Result build()
        {
            if ( state == null )
            {
                throw new IllegalArgumentException( "Builder state empty" );
            }
            state.checkAssignments( this.supportsDate() );
            return buildInternal();
        }

        <Temp extends Temporal> Temp assignAllFields( Temp temp )
        {
            Temp result = temp;
            for ( Map.Entry<Field,AnyValue> entry : fields.entrySet() )
            {
                Field f = entry.getKey();
                if ( !f.isGroupSelector() && f != Field.timezone && f != Field.millisecond && f != Field.microsecond && f != Field.nanosecond )
                {
                    TemporalField temporalField = f.field;
                    result = (Temp) result.with( temporalField, safeCastIntegral( f.name(), entry.getValue(), f.defaultValue ) );
                }
            }
            // Assign all sub-second parts in one step
            if ( supportsTime() &&
                    (fields.containsKey( Field.millisecond ) || fields.containsKey( Field.microsecond ) || fields.containsKey( Field.nanosecond )) )
            {
                result = (Temp) result.with( Field.nanosecond.field,
                        validNano( fields.get( Field.millisecond ), fields.get( Field.microsecond ), fields.get( Field.nanosecond ) ) );
            }
            return result;
        }

        static int validNano( AnyValue millisecond, AnyValue microsecond, AnyValue nanosecond )
        {
            long ms = safeCastIntegral( "millisecond", millisecond, Field.millisecond.defaultValue );
            long us = safeCastIntegral( "microsecond", microsecond, Field.microsecond.defaultValue );
            long ns = safeCastIntegral( "nanosecond", nanosecond, Field.nanosecond.defaultValue );
            if ( ms < 0 || ms >= 1000 )
            {
                throw new IllegalArgumentException( "Invalid millisecond: " + ms );
            }
            if ( us < 0 || us >= (millisecond != null || nanosecond != null ? 1000 : 1000_000) )
            {
                throw new IllegalArgumentException( "Invalid microsecond: " + us );
            }
            if ( ns < 0 || ns >= ((millisecond != null || microsecond != null) ? 1000 : 1000_000_000) )
            {
                throw new IllegalArgumentException( "Invalid nanosecond: " + ns );
            }
            return (int) (ms * 1000_000 + us * 1000 + ns);
        }

        @Override
        public final StructureBuilder<AnyValue,Result> add( String fieldName, AnyValue value )
        {
            Field field = Field.fields.get( fieldName.toLowerCase() );
            if ( field == null )
            {
                throw new IllegalArgumentException( "No such field: " + fieldName );
            }
            // Change state
            field.assign( this, value );

            // Set field for this builder
            fields.put( field, value );
            return this;
        }

        @SuppressWarnings( "BooleanMethodIsAlwaysInverted" )
        private boolean supports( TemporalField field )
        {
            if ( field.isDateBased() )
            {
                return supportsDate();
            }
            if ( field.isTimeBased() )
            {
                return supportsTime();
            }
            throw new IllegalStateException( "Fields should be either date based or time based" );
        }

        protected abstract boolean supportsDate();

        protected abstract boolean supportsTime();

        protected abstract boolean supportsTimeZone();

        protected abstract boolean supportsEpoch();

        protected ZoneId timezone( AnyValue timezone )
        {
            return timezone == null ? defaultZone.get() : timezoneOf( timezone );
        }

        // Construction

        protected abstract Result buildInternal();

        // Timezone utilities

        protected final ZoneId optionalTimezone()
        {
            return timezone == null ? null : timezone();
        }

        protected final ZoneId timezone()
        {
            return timezone( timezone );
        }

        protected final ZoneId timezoneOf( AnyValue timezone )
        {
            if ( timezone instanceof TextValue )
            {
                return parseZoneName( ((TextValue) timezone).stringValue() );
            }
            throw new UnsupportedOperationException( "Cannot convert to ZoneId: " + timezone );
        }

    }

    protected enum Field
    {
        year( ChronoField.YEAR, 0 ),
        quarter( IsoFields.QUARTER_OF_YEAR, 1 ),
        month( ChronoField.MONTH_OF_YEAR, 1 ),
        week( IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1 ),
        ordinalDay( ChronoField.DAY_OF_YEAR, 1 ),
        dayOfQuarter( IsoFields.DAY_OF_QUARTER, 1 ),
        dayOfWeek( ChronoField.DAY_OF_WEEK, 1 ),
        day( ChronoField.DAY_OF_MONTH, 1 ),
        hour( ChronoField.HOUR_OF_DAY, 0 ),
        minute( ChronoField.MINUTE_OF_HOUR, 0 ),
        second( ChronoField.SECOND_OF_MINUTE, 0 ),
        millisecond( ChronoField.MILLI_OF_SECOND, 0 ),
        microsecond( ChronoField.MICRO_OF_SECOND, 0 ),
        nanosecond( ChronoField.NANO_OF_SECOND, 0 ),
        timezone//<pre>
        { //</pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                if ( !builder.supportsTimeZone() )
                {
                    throw new IllegalArgumentException( "Cannot assign time zone if also assigning other fields." );
                }
                if ( builder.timezone != null )
                {
                    throw new IllegalArgumentException( "Cannot assign timezone twice." );
                }
                builder.timezone = value;
            }
        },
        // group selectors
        date//<pre>
        { //</pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                if ( !builder.supportsDate() )
                {
                    throw new IllegalArgumentException( "Not supported: " + name() );
                }
                if ( builder.state == null )
                {
                    builder.state = new DateTimeBuilder();
                }
                builder.state = builder.state.assign( this, value );
            }

            @Override
            boolean isGroupSelector()
            {
                return true;
            }
        },
        time//<pre>
        { //</pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                if ( !builder.supportsTime() )
                {
                    throw new IllegalArgumentException( "Not supported: " + name() );
                }
                if ( builder.state == null )
                {
                    builder.state = new DateTimeBuilder();
                }
                builder.state = builder.state.assign( this, value );
            }

            @Override
            boolean isGroupSelector()
            {
                return true;
            }
        },
        datetime//<pre>
        { //</pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                if ( !builder.supportsDate() || !builder.supportsTime() )
                {
                    throw new IllegalArgumentException( "Not supported: " + name() );
                }
                if ( builder.state == null )
                {
                    builder.state = new DateTimeBuilder( );
                }
                builder.state = builder.state.assign( this, value );
            }

            @Override
            boolean isGroupSelector()
            {
                return true;
            }
        },
        epoch//<pre>
        { //<pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                if ( !builder.supportsEpoch() )
                {
                    throw new IllegalArgumentException( "Not supported: " + name() );
                }
                if ( builder.state == null )
                {
                    builder.state = new DateTimeBuilder( );
                }
                builder.state = builder.state.assign( this, value );
            }

            @Override
            boolean isGroupSelector()
            {
                return true;
            }
        };
        private static final Map<String,Field> fields = new HashMap<>();

        static
        {
            for ( Field field : values() )
            {
                fields.put( field.name().toLowerCase(), field );
            }
            // aliases
            fields.put( "weekday", dayOfWeek );
            fields.put( "quarterday", dayOfQuarter );
        }

        final TemporalField field;
        final int defaultValue;

        Field( TemporalField field, int defaultValue )
        {
            this.field = field;
            this.defaultValue = defaultValue;
        }

        Field()
        {
            this.field = null;
            this.defaultValue = -1;
        }

        boolean isGroupSelector()
        {
            return false;
        }

        void assign( Builder<?> builder, AnyValue value )
        {
            assert field != null : "method should have been overridden";
            if ( !builder.supports( field ) )
            {
                throw new IllegalArgumentException( "Not supported: " + name() );
            }
            if ( builder.state == null )
            {
                builder.state = new DateTimeBuilder();
            }
            builder.state = builder.state.assign( this, value );
        }
    }

    private static class DateTimeBuilder
    {
        protected DateBuilder date;
        protected ConstructTime time;

        DateTimeBuilder()
        {
        }

        DateTimeBuilder( DateBuilder date, ConstructTime time )
        {
            this.date = date;
            this.time = time;
        }

        void checkAssignments( boolean requiresDate )
        {
            if ( date != null )
            {
                date.checkAssignments();
            }
            if ( time != null )
            {
                if ( requiresDate )
                {
                    if ( date != null )
                    {
                        date.assertFullyAssigned();
                    }
                    else
                    {
                        throw new IllegalArgumentException( Field.year.name() + " must be specified" );
                    }
                }
                time.checkAssignments();
            }
        }

        DateTimeBuilder assign( Field field, AnyValue value )
        {
            if ( field == Field.datetime || field == Field.epoch )
            {
                return new SelectDateTimeDTBuilder( date, time ).assign( field, value );
            }
            else if ( field == Field.time || field == Field.date )
            {
                return new SelectDateOrTimeDTBuilder( date, time ).assign( field, value );
            }
            else
            {
                return assignToSubBuilders( field, value );
            }
        }

        DateTimeBuilder assignToSubBuilders( Field field, AnyValue value )
        {
            if ( field == Field.date || field.field != null && field.field.isDateBased() )
            {
                if ( date == null )
                {
                    date = new ConstructDate();
                }
                date = date.assign( field, value );
            }
            else if ( field == Field.time || field.field != null && field.field.isTimeBased() )
            {
                if ( time == null )
                {
                    time = new ConstructTime();
                }
                time.assign( field, value );
            }
            else
            {
                throw new IllegalStateException( "This method should not be used for any fields the DateBuilder or TimeBuilder can't handle" );
            }
            return this;
        }
    }

    private static class SelectDateTimeDTBuilder extends DateTimeBuilder
    {
        private AnyValue datetime;
        private AnyValue epoch;

        SelectDateTimeDTBuilder( DateBuilder date, ConstructTime time )
        {
            super( date, time );
        }

        @Override
        void checkAssignments( boolean requiresDate )
        {
            // Nothing to do
        }

        @Override
        DateTimeBuilder assign( Field field, AnyValue value )
        {
            if ( field == Field.date || field == Field.time )
            {
                throw new IllegalArgumentException( field.name() + " cannot be selected together with datetime or epoch." );
            }
            else if ( field == Field.datetime )
            {
                if ( epoch != null )
                {
                    throw new IllegalArgumentException( field.name() + " cannot be selected together with epoch." );
                }
                datetime = assignment( Field.datetime, datetime, value );
            }
            else if ( field == Field.epoch )
            {
                if ( datetime != null )
                {
                    throw new IllegalArgumentException( field.name() + " cannot be selected together with datetime." );
                }
                epoch = assignment( Field.epoch, epoch, value );
            }
            else
            {
                return assignToSubBuilders( field, value );
            }
            return this;
        }
    }

    private static class SelectDateOrTimeDTBuilder extends DateTimeBuilder
    {
        SelectDateOrTimeDTBuilder( DateBuilder date, ConstructTime time )
        {
            super( date, time );
        }

        @Override
        DateTimeBuilder assign( Field field, AnyValue value )
        {
            if ( field == Field.datetime || field == Field.epoch )
            {
                throw new IllegalArgumentException( field.name() + " cannot be selected together with date or time." );
            }
            else
            {
                return assignToSubBuilders( field, value );
            }
        }
    }

    private abstract static class DateBuilder
    {
        abstract DateBuilder assign( Field field, AnyValue value );

        abstract void checkAssignments();

        abstract void assertFullyAssigned();
    }

    private static final class ConstructTime
    {
        private AnyValue hour;
        private AnyValue minute;
        private AnyValue second;
        private AnyValue millisecond;
        private AnyValue microsecond;
        private AnyValue nanosecond;
        private AnyValue time;

        ConstructTime()
        {
        }

        void assign( Field field, AnyValue value )
        {
            switch ( field )
            {
            case hour:
                hour = assignment( field, hour, value );
                break;
            case minute:
                minute = assignment( field, minute, value );
                break;
            case second:
                second = assignment( field, second, value );
                break;
            case millisecond:
                millisecond = assignment( field, millisecond, value );
                break;
            case microsecond:
                microsecond = assignment( field, microsecond, value );
                break;
            case nanosecond:
                nanosecond = assignment( field, nanosecond, value );
                break;
            case time:
            case datetime:
                time = assignment( field, time, value );
                break;
            default:
                throw new IllegalStateException( "Not a time field: " + field );
            }
        }

        void checkAssignments()
        {
            if ( time == null )
            {
                assertDefinedInOrder( Pair.of( hour, "hour" ), Pair.of( minute, "minute" ), Pair.of( second, "second" ),
                        Pair.of( oneOf( millisecond, microsecond, nanosecond ), "subsecond" ) );
            }
        }
    }

    private static class ConstructDate extends DateBuilder
    {
        AnyValue year;
        AnyValue date;

        ConstructDate()
        {
        }

        ConstructDate( AnyValue date )
        {
            this.date = date;
        }

        @Override
        ConstructDate assign( Field field, AnyValue value )
        {
            switch ( field )
            {
            case year:
                year = assignment( field, year, value );
                return this;
            case quarter:
            case dayOfQuarter:
                return new QuarterDate( year, date ).assign( field, value );
            case month:
            case day:
                return new CalendarDate( year, date ).assign( field, value );
            case week:
            case dayOfWeek:
                return new WeekDate( year, date ).assign( field, value );
            case ordinalDay:
                return new OrdinalDate( year, date ).assign( field, value );
            case date:
            case datetime:
                date = assignment( field, date, value );
                return this;
            default:
                throw new IllegalStateException( "Not a date field: " + field );
            }
        }

        @Override
        void checkAssignments()
        {
            // Nothing to do
        }

        @Override
        void assertFullyAssigned()
        {
            if ( date == null )
            {
                throw new IllegalArgumentException( Field.month.name() + " must be specified"  );
            }
        }
    }

    private static final class CalendarDate extends ConstructDate
    {
        private AnyValue month;
        private AnyValue day;

        CalendarDate( AnyValue year, AnyValue date )
        {
            this.year = year;
            this.date = date;
        }

        @Override
        ConstructDate assign( Field field, AnyValue value )
        {
            switch ( field )
            {
            case year:
                year = assignment( field, year, value );
                return this;
            case month:
                month = assignment( field, month, value );
                return this;
            case day:
                day = assignment( field, day, value );
                return this;
            case date:
            case datetime:
                date = assignment( field, date, value );
                return this;
            default:
                throw new IllegalArgumentException( "Cannot assign " + field + " to calendar date." );
            }
        }

        @Override
        void checkAssignments()
        {
            if ( date == null )
            {
                assertDefinedInOrder( Pair.of( year, "year" ), Pair.of( month, "month" ), Pair.of( day, "day" ) );
            }
        }

        @Override
        void assertFullyAssigned()
        {
            if ( date == null )
            {
                assertAllDefined( Pair.of( year, "year" ), Pair.of( month, "month" ), Pair.of( day, "day" ) );
            }
        }
    }

    private static final class WeekDate extends ConstructDate
    {
        private AnyValue week;
        private AnyValue dayOfWeek;

        WeekDate( AnyValue year, AnyValue date )
        {
            this.year = year;
            this.date = date;
        }

        @Override
        ConstructDate assign( Field field, AnyValue value )
        {
            switch ( field )
            {
            case year:
                year = assignment( field, year, value );
                return this;
            case week:
                week = assignment( field, week, value );
                return this;
            case dayOfWeek:
                dayOfWeek = assignment( field, dayOfWeek, value );
                return this;
            case date:
            case datetime:
                date = assignment( field, date, value );
                return this;
            default:
                throw new IllegalArgumentException( "Cannot assign " + field + " to week date." );
            }
        }

        @Override
        void checkAssignments()
        {
            if ( date == null )
            {
                assertDefinedInOrder( Pair.of( year, "year" ), Pair.of( week, "week" ), Pair.of( dayOfWeek, "dayOfWeek" ) );
            }
        }

        @Override
        void assertFullyAssigned()
        {
            if ( date == null )
            {
                assertAllDefined( Pair.of( year, "year" ), Pair.of( week, "week" ), Pair.of( dayOfWeek, "dayOfWeek" ) );
            }
        }
    }

    private static final class QuarterDate extends ConstructDate
    {
        private AnyValue quarter;
        private AnyValue dayOfQuarter;

        QuarterDate( AnyValue year, AnyValue date )
        {
            this.year = year;
            this.date = date;
        }

        @Override
        ConstructDate assign( Field field, AnyValue value )
        {
            switch ( field )
            {
            case year:
                year = assignment( field, year, value );
                return this;
            case quarter:
                quarter = assignment( field, quarter, value );
                return this;
            case dayOfQuarter:
                dayOfQuarter = assignment( field, dayOfQuarter, value );
                return this;
            case date:
            case datetime:
                date = assignment( field, date, value );
                return this;
            default:
                throw new IllegalArgumentException( "Cannot assign " + field + " to quarter date." );
            }
        }

        @Override
        void checkAssignments()
        {
            if ( date == null )
            {
                assertDefinedInOrder( Pair.of( year, "year" ), Pair.of( quarter, "quarter" ), Pair.of( dayOfQuarter, "dayOfQuarter" ) );
            }
        }

        @Override
        void assertFullyAssigned()
        {
            if ( date == null )
            {
                assertAllDefined( Pair.of( year, "year" ), Pair.of( quarter, "quarter" ), Pair.of( dayOfQuarter, "dayOfQuarter" ) );
            }
        }
    }

    private static final class OrdinalDate extends ConstructDate
    {
        private AnyValue ordinalDay;

        OrdinalDate( AnyValue year, AnyValue date )
        {
            this.year = year;
            this.date = date;
        }

        @Override
        ConstructDate assign( Field field, AnyValue value )
        {
            switch ( field )
            {
            case year:
                year = assignment( field, year, value );
                return this;
            case ordinalDay:
                ordinalDay = assignment( field, ordinalDay, value );
                return this;
            case date:
            case datetime:
                date = assignment( field, date, value );
                return this;
            default:
                throw new IllegalArgumentException( "Cannot assign " + field + " to ordinal date." );
            }
        }

        @Override
        void assertFullyAssigned()
        {
            if ( date == null )
            {
                assertAllDefined( Pair.of( year, "year" ), Pair.of( ordinalDay, "ordinalDay" ) );
            }
        }
    }

    private static AnyValue assignment( Field field, AnyValue oldValue, AnyValue newValue )
    {
        if ( oldValue != null )
        {
            throw new IllegalArgumentException( "cannot re-assign " + field );
        }
        return newValue;
    }

    @SafeVarargs
    static void assertDefinedInOrder( Pair<org.neo4j.values.AnyValue, String>... values )
    {
        if ( values[0].first() == null )
        {
            throw new IllegalArgumentException( values[0].other() + " must be specified" );
        }

        String firstNotAssigned = null;

        for ( Pair<org.neo4j.values.AnyValue,String> value : values )
        {
            if ( value.first() == null )
            {
                if ( firstNotAssigned == null )
                {
                    firstNotAssigned = value.other();
                }
            }
            else if ( firstNotAssigned != null )
            {
                throw new IllegalArgumentException( value.other() + " cannot be specified without " + firstNotAssigned );
            }
        }
    }

    @SafeVarargs
    static void assertAllDefined( Pair<org.neo4j.values.AnyValue, String>... values )
    {
        for ( Pair<org.neo4j.values.AnyValue,String> value : values )
        {
            if ( value.first() == null )
            {
                throw new IllegalArgumentException( value.other() + " must be specified" );
            }
        }
    }

    static org.neo4j.values.AnyValue oneOf( org.neo4j.values.AnyValue a, org.neo4j.values.AnyValue b, org.neo4j.values.AnyValue c )
    {
        return a != null ? a : b != null ? b : c;
    }
}
