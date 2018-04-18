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

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.chrono.ChronoZonedDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
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
import org.neo4j.values.utils.InvalidValuesArgumentException;
import org.neo4j.values.utils.TemporalArithmeticException;
import org.neo4j.values.utils.TemporalParseException;
import org.neo4j.values.utils.UnsupportedTemporalUnitException;

import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.storable.DateTimeValue.parseZoneName;
import static org.neo4j.values.storable.IntegralValue.safeCastIntegral;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.NumberType.NO_NUMBER;
import static org.neo4j.values.storable.TimeValue.time;

public abstract class TemporalValue<T extends Temporal, V extends TemporalValue<T,V>>
        extends ScalarValue implements Temporal
{
    TemporalValue()
    {
        // subclasses are confined to this package,
        // but type-checking is valuable to be able to do outside
        // (therefore the type itself is public)
    }

    public abstract TemporalValue add( DurationValue duration );

    public abstract TemporalValue sub( DurationValue duration );

    abstract T temporal();

    /**
     * @return the date part of this temporal, if date is supported.
     */
    abstract LocalDate getDatePart();

    /**
     * @return the local time part of this temporal, if time is supported.
     */
    abstract LocalTime getLocalTimePart();

    /**
     * @return the time part of this temporal, if time is supported.
     */
    abstract OffsetTime getTimePart( Supplier<ZoneId> defaultZone );

    /**
     * @return the zone id, if time is supported. If time is supported, but no timezone, the defaultZone will be used.
     * @throws UnsupportedTemporalUnitException if time is not supported
     */
    abstract ZoneId getZoneId( Supplier<ZoneId> defaultZone );

    /**
     * @return the zone id, if this temporal has a timezone.
     * @throws UnsupportedTemporalUnitException if this does not have a timezone
     */
    abstract ZoneId getZoneId();

    /**
     * @return the zone offset, if this temporal has a zone offset.
     * @throws UnsupportedTemporalUnitException if this does not have a offset
     */
    abstract ZoneOffset getZoneOffset();

    abstract boolean supportsTimeZone();

    abstract boolean hasTime();

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
        if (  !(endExclusive instanceof TemporalValue) )
        {
            throw new InvalidValuesArgumentException( "Can only compute durations between TemporalValues." );
        }
        TemporalValue from = this;
        TemporalValue to = (TemporalValue) endExclusive;

        from = attachTime( from );
        to = attachTime( to );

        if ( from.isSupported( ChronoField.MONTH_OF_YEAR ) && !to.isSupported( ChronoField.MONTH_OF_YEAR ) )
        {
            to = attachDate( to, from.getDatePart() );
        }
        else if ( to.isSupported( ChronoField.MONTH_OF_YEAR ) && !from.isSupported( ChronoField.MONTH_OF_YEAR ) )
        {
            from = attachDate( from, to.getDatePart() );
        }

        if ( from.supportsTimeZone() && !to.supportsTimeZone() )
        {
            to = attachTimeZone( to, from.getZoneId( from::getZoneOffset ) );
        }
        else if ( to.supportsTimeZone() && !from.supportsTimeZone() )
        {
            from = attachTimeZone( from, to.getZoneId( to::getZoneOffset ) );
        }
        long until;
        try
        {
            until = from.temporal().until( to, unit );
        }
        catch ( UnsupportedTemporalTypeException e )
        {
            throw new UnsupportedTemporalUnitException( e.getMessage(), e );
        }
        return until;
    }

    private TemporalValue attachTime( TemporalValue temporal )
    {
        boolean supportsTime = temporal.isSupported( ChronoField.SECOND_OF_DAY );

        if ( supportsTime )
        {
            return temporal;
        }
        else
        {
            LocalDate datePart = temporal.getDatePart();
            LocalTime timePart = LocalTimeValue.DEFAULT_LOCAL_TIME;
            return localDateTime( LocalDateTime.of( datePart, timePart ) );
        }
    }

    private TemporalValue attachDate( TemporalValue temporal, LocalDate dateToAttach )
    {
        LocalTime timePart = temporal.getLocalTimePart();

        if ( temporal.supportsTimeZone() )
        {
            // turn time into date time
            return datetime( ZonedDateTime.of( dateToAttach, timePart, temporal.getZoneOffset() ) );
        }
        else
        {
            // turn local time into local date time
            return localDateTime( LocalDateTime.of( dateToAttach, timePart ) );
        }
    }

    private TemporalValue attachTimeZone( TemporalValue temporal, ZoneId zoneIdToAttach )
    {
        if ( temporal.isSupported( ChronoField.MONTH_OF_YEAR ) )
        {
            // turn local date time into date time
            return datetime( ZonedDateTime.of( temporal.getDatePart(), temporal.getLocalTimePart(), zoneIdToAttach ) );
        }
        else
        {
            // turn local time into time
            if ( zoneIdToAttach instanceof ZoneOffset )
            {
                return time( OffsetTime.of( temporal.getLocalTimePart(), (ZoneOffset) zoneIdToAttach ) );
            }
            else
            {
                throw new IllegalStateException( "Should only attach offsets to local times, not zone ids." );
            }
        }
    }

    @Override
    public final ValueRange range( TemporalField field )
    {
        return temporal().range( field );
    }

    @Override
    public final int get( TemporalField field )
    {
        int accessor;
        try
        {
         accessor = temporal().get( field );
        }
        catch ( UnsupportedTemporalTypeException e )
        {
            throw new UnsupportedTemporalUnitException( e.getMessage(), e );
        }
        return accessor;
    }

    public final AnyValue get( String fieldName )
    {
        Field field = Field.fields.get( fieldName.toLowerCase() );
        if ( field == Field.epochSeconds || field == Field.epochMillis )
        {
            T temp = temporal();
            if ( temp instanceof ChronoZonedDateTime )
            {
                ChronoZonedDateTime zdt = (ChronoZonedDateTime) temp;
                if ( field == Field.epochSeconds )
                {
                    return Values.longValue( zdt.toInstant().toEpochMilli() / 1000 );
                }
                else
                {
                    return Values.longValue( zdt.toInstant().toEpochMilli() );
                }
            }
            else
            {
                throw new UnsupportedTemporalUnitException( "Epoch not supported." );
            }
        }
        if ( field == Field.timezone )
        {
            return Values.stringValue( getZoneId(this::getZoneOffset).toString() );
        }
        if ( field == Field.offset )
        {
            return Values.stringValue( getZoneOffset().toString() );
        }
        if ( field == Field.offsetMinutes )
        {
            return Values.intValue( getZoneOffset().getTotalSeconds() / 60 );
        }
        if ( field == Field.offsetSeconds )
        {
            return Values.intValue( getZoneOffset().getTotalSeconds() );
        }
        if ( field == null || field.field == null )
        {
            throw new UnsupportedTemporalUnitException( "No such field: " + fieldName );
        }
        return Values.intValue( get( field.field ) );
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

    @Override
    public String toString()
    {
        return prettyPrint();
    }

    static <VALUE> VALUE parse( Class<VALUE> type, Pattern pattern, Function<Matcher,VALUE> parser, CharSequence text )
    {
        Matcher matcher = pattern.matcher( text );
        VALUE result = matcher.matches() ? parser.apply( matcher ) : null;
        if ( result == null )
        {
            throw new TemporalParseException(
                    "Text cannot be parsed to a " + valueName( type ), text.toString(), 0 );
        }
        return result;
    }

    static <VALUE> VALUE parse( Class<VALUE> type, Pattern pattern, Function<Matcher,VALUE> parser, TextValue text )
    {
        Matcher matcher = text.matcher( pattern );
        VALUE result = matcher != null && matcher.matches() ? parser.apply( matcher ) : null;
        if ( result == null )
        {
            throw new TemporalParseException(
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
            throw new TemporalParseException(
                    "Text cannot be parsed to a " + valueName( type ), text.toString(), 0 );
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
            throw new TemporalParseException(
                    "Text cannot be parsed to a " + valueName( type ), text.stringValue(), 0 );
        }
        return result;
    }

    private static <VALUE> String valueName( Class<VALUE> type )
    {
        String name = type.getSimpleName();
        return name.substring( 0, name.length() - /*"Value" is*/5/*characters*/ );
    }

    public static TimeCSVHeaderInformation parseHeaderInformation( String text )
    {
        TimeCSVHeaderInformation fields = new TimeCSVHeaderInformation();
        Value.parseHeaderInformation( text, "time/datetime", fields );
        return fields;
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
                throw new InvalidValuesArgumentException( "Builder state empty" );
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

        @Override
        public final StructureBuilder<AnyValue,Result> add( String fieldName, AnyValue value )
        {
            Field field = Field.fields.get( fieldName.toLowerCase() );
            if ( field == null )
            {
                throw new InvalidValuesArgumentException( "No such field: " + fieldName );
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
        // Read only accessors (not assignable)
        weekYear( IsoFields.WEEK_BASED_YEAR, 0 )//<pre>
        { //</pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                throw new UnsupportedTemporalUnitException( "Not supported: " + name() );
            }
        },
        offset//<pre>
        { //</pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                throw new UnsupportedTemporalUnitException( "Not supported: " + name() );
            }
        },
        offsetMinutes//<pre>
        { //</pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                throw new UnsupportedTemporalUnitException( "Not supported: " + name() );
            }
        },
        offsetSeconds//<pre>
        { //</pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                throw new UnsupportedTemporalUnitException( "Not supported: " + name() );
            }
        },
        // time zone
        timezone//<pre>
        { //</pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                if ( !builder.supportsTimeZone() )
                {
                    throw new UnsupportedTemporalUnitException( "Cannot assign time zone if also assigning other fields." );
                }
                if ( builder.timezone != null )
                {
                    throw new InvalidValuesArgumentException( "Cannot assign timezone twice." );
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
                    throw new UnsupportedTemporalUnitException( "Not supported: " + name() );
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
                    throw new UnsupportedTemporalUnitException( "Not supported: " + name() );
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
                    throw new UnsupportedTemporalUnitException( "Not supported: " + name() );
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
        epochSeconds//<pre>
        { //<pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                if ( !builder.supportsEpoch() )
                {
                    throw new UnsupportedTemporalUnitException( "Not supported: " + name() );
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
        epochMillis//<pre>
        { //<pre>

            @Override
            void assign( Builder<?> builder, AnyValue value )
            {
                if ( !builder.supportsEpoch() )
                {
                    throw new UnsupportedTemporalUnitException( "Not supported: " + name() );
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
                throw new UnsupportedTemporalUnitException( "Not supported: " + name() );
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
                        throw new InvalidValuesArgumentException( Field.year.name() + " must be specified" );
                    }
                }
                time.checkAssignments();
            }
        }

        DateTimeBuilder assign( Field field, AnyValue value )
        {
            if ( field == Field.datetime || field == Field.epochSeconds || field == Field.epochMillis )
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
        private AnyValue epochSeconds;
        private AnyValue epochMillis;

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
                throw new InvalidValuesArgumentException( field.name() + " cannot be selected together with datetime or epochSeconds or epochMillis." );
            }
            else if ( field == Field.datetime )
            {
                if ( epochSeconds != null )
                {
                    throw new InvalidValuesArgumentException( field.name() + " cannot be selected together with epochSeconds." );
                }
                else if ( epochMillis != null )
                {
                    throw new InvalidValuesArgumentException( field.name() + " cannot be selected together with epochMillis." );
                }
                datetime = assignment( Field.datetime, datetime, value );
            }
            else if ( field == Field.epochSeconds )
            {
                if ( epochMillis != null )
                {
                    throw new InvalidValuesArgumentException( field.name() + " cannot be selected together with epochMillis." );
                }
                else if ( datetime != null )
                {
                    throw new InvalidValuesArgumentException( field.name() + " cannot be selected together with datetime." );
                }
                epochSeconds = assignment( Field.epochSeconds, epochSeconds, value );
            }
            else if ( field == Field.epochMillis )
            {
                if ( epochSeconds != null )
                {
                    throw new InvalidValuesArgumentException( field.name() + " cannot be selected together with epochSeconds." );
                }
                else if ( datetime != null )
                {
                    throw new InvalidValuesArgumentException( field.name() + " cannot be selected together with datetime." );
                }
                epochMillis = assignment( Field.epochMillis, epochMillis, value );
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
            if ( field == Field.datetime || field == Field.epochSeconds || field == Field.epochMillis )
            {
                throw new InvalidValuesArgumentException( field.name() + " cannot be selected together with date or time." );
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
                throw new InvalidValuesArgumentException( Field.month.name() + " must be specified"  );
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
                throw new UnsupportedTemporalUnitException( "Cannot assign " + field + " to calendar date." );
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
                throw new UnsupportedTemporalUnitException( "Cannot assign " + field + " to week date." );
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
                throw new UnsupportedTemporalUnitException( "Cannot assign " + field + " to quarter date." );
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
                throw new UnsupportedTemporalUnitException( "Cannot assign " + field + " to ordinal date." );
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
            throw new InvalidValuesArgumentException( "cannot re-assign " + field );
        }
        return newValue;
    }

    @SafeVarargs
    static void assertDefinedInOrder( Pair<org.neo4j.values.AnyValue, String>... values )
    {
        if ( values[0].first() == null )
        {
            throw new InvalidValuesArgumentException( values[0].other() + " must be specified" );
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
                throw new InvalidValuesArgumentException( value.other() + " cannot be specified without " + firstNotAssigned );
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
                throw new InvalidValuesArgumentException( value.other() + " must be specified" );
            }
        }
    }

    static org.neo4j.values.AnyValue oneOf( org.neo4j.values.AnyValue a, org.neo4j.values.AnyValue b, org.neo4j.values.AnyValue c )
    {
        return a != null ? a : b != null ? b : c;
    }

    static ZoneId timezoneOf( AnyValue timezone )
    {
        if ( timezone instanceof TextValue )
        {
            return parseZoneName( ((TextValue) timezone).stringValue() );
        }
        throw new UnsupportedOperationException( "Cannot convert to ZoneId: " + timezone );
    }

    static int validNano( AnyValue millisecond, AnyValue microsecond, AnyValue nanosecond )
    {
        long ms = safeCastIntegral( "millisecond", millisecond, Field.millisecond.defaultValue );
        long us = safeCastIntegral( "microsecond", microsecond, Field.microsecond.defaultValue );
        long ns = safeCastIntegral( "nanosecond", nanosecond, Field.nanosecond.defaultValue );
        if ( ms < 0 || ms >= 1000 )
        {
            throw new InvalidValuesArgumentException( "Invalid millisecond: " + ms );
        }
        if ( us < 0 || us >= (millisecond != null ? 1000 : 1000_000) )
        {
            throw new InvalidValuesArgumentException( "Invalid microsecond: " + us );
        }
        if ( ns < 0 || ns >= ( microsecond != null ? 1000 : millisecond != null ? 1000_000 : 1000_000_000 ) )
        {
            throw new InvalidValuesArgumentException( "Invalid nanosecond: " + ns );
        }
        return (int) (ms * 1000_000 + us * 1000 + ns);
    }

    static <TEMP extends Temporal> TEMP updateFieldMapWithConflictingSubseconds( Map<String,AnyValue> fields, TemporalUnit unit, TEMP truncated )
    {
        boolean conflictingMilliSeconds = false;
        boolean conflictingMicroSeconds = false;

        for ( Map.Entry<String,AnyValue> entry : fields.entrySet() )
        {
            if ( unit == ChronoUnit.MILLIS && ( "microsecond".equals( entry.getKey() ) || "nanosecond".equals( entry.getKey() ) ) )
            {
                conflictingMilliSeconds = true;
            }
            else if ( unit == ChronoUnit.MICROS && "nanosecond".equals( entry.getKey() ) )
            {
                conflictingMicroSeconds = true;
            }
        }

        if ( conflictingMilliSeconds )
        {
            AnyValue millis = Values.intValue( truncated.get( ChronoField.MILLI_OF_SECOND ) );
            AnyValue micros = fields.remove( "microsecond" );
            AnyValue nanos = fields.remove( "nanosecond" );
            int newNanos = validNano( millis, micros, nanos );
            truncated = (TEMP) truncated.with( ChronoField.NANO_OF_SECOND, newNanos );
        }
        else if ( conflictingMicroSeconds )
        {
            AnyValue micros = Values.intValue( truncated.get( ChronoField.MICRO_OF_SECOND ) );
            AnyValue nanos = fields.remove( "nanosecond" );
            int newNanos = validNano( null,  micros, nanos );
            truncated = (TEMP) truncated.with( ChronoField.NANO_OF_SECOND, newNanos );
        }
        return truncated;
    }

    static <TEMP extends Temporal> TEMP assertValidArgument( Supplier<TEMP> func )
    {
        try
        {
            return func.get();
        }
        catch ( DateTimeException e )
        {
            throw new InvalidValuesArgumentException( e.getMessage(), e );
        }
    }

    static <TEMP extends Temporal> TEMP assertValidUnit( Supplier<TEMP> func )
    {
        try
        {
            return func.get();
        }
        catch ( DateTimeException e )
        {
            throw new UnsupportedTemporalUnitException( e.getMessage(), e );
        }
    }

    static <OFFSET extends ZoneId> OFFSET assertValidZone( Supplier<OFFSET> func )
    {
        try
        {
            return func.get();
        }
        catch ( DateTimeException e )
        {
            throw new InvalidValuesArgumentException( e.getMessage(), e );
        }
    }

    static <TEMP extends Temporal> TEMP assertParsable( Supplier<TEMP> func )
    {
        try
        {
            return func.get();
        }
        catch ( DateTimeException e )
        {
            throw new TemporalParseException( e.getMessage(), e );
        }
    }

    static String assertPrintable( Supplier<String> func )
    {
        try
        {
            return func.get();
        }
        catch ( DateTimeException e )
        {
            throw new TemporalParseException( e.getMessage(), e );
        }
    }

    static <TEMP extends Temporal> TEMP assertValidArithmetic( Supplier<TEMP> func )
    {
        try
        {
            return func.get();
        }
        catch ( DateTimeException | ArithmeticException e )
        {
            throw new TemporalArithmeticException( e.getMessage(), e );
        }
    }

    static Pair<LocalDate,LocalTime> getTruncatedDateAndTime( TemporalUnit unit, TemporalValue input, String type )
    {
        if ( unit.isTimeBased() && !(input instanceof DateTimeValue || input instanceof LocalDateTimeValue) )
        {
            throw new UnsupportedTemporalUnitException( String.format( "Cannot truncate %s to %s with a time based unit.", input, type ) );
        }
        LocalDate localDate = input.getDatePart();
        LocalTime localTime = input.hasTime() ? input.getLocalTimePart() : LocalTimeValue.DEFAULT_LOCAL_TIME;

        LocalTime truncatedTime;
        LocalDate truncatedDate;
        if ( unit.isDateBased() )
        {
            truncatedDate = DateValue.truncateTo( localDate, unit );
            truncatedTime = LocalTimeValue.DEFAULT_LOCAL_TIME;
        }
        else
        {
            truncatedDate = localDate;
            truncatedTime = localTime.truncatedTo( unit );
        }
        return Pair.of( truncatedDate, truncatedTime );
    }

    static class TimeCSVHeaderInformation implements CSVHeaderInformation
    {
        String timezone;

        @Override
        public void assign( String key, Object valueObj )
        {
            if ( !(valueObj instanceof String) )
            {
                throw new InvalidValuesArgumentException( String.format( "Cannot assign %s to field %s", valueObj, key ) );
            }
            String value = (String) valueObj;
            if ( "timezone".equals( key.toLowerCase() ) )
            {
                if ( timezone == null )
                {
                    timezone = value;
                }
                else
                {
                    throw new InvalidValuesArgumentException( "Cannot set timezone twice" );
                }
            }
            else
            {
                throw new InvalidValuesArgumentException( "Unsupported header field: " + value );
            }
        }

        Supplier<ZoneId> zoneSupplier( Supplier<ZoneId> defaultSupplier )
        {
            if ( timezone != null )
            {
                ZoneId tz = DateTimeValue.parseZoneName( timezone );
                // Override defaultZone
                return () -> tz;
            }
            return defaultSupplier;
        }
    }
}
