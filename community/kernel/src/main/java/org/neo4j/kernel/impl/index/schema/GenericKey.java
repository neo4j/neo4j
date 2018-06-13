package org.neo4j.kernel.impl.index.schema;

import java.time.ZoneId;
import java.time.ZoneOffset;

import org.neo4j.kernel.impl.index.schema.GenericLayout.Type;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.DurationIndexKey.AVG_DAY_SECONDS;
import static org.neo4j.kernel.impl.index.schema.DurationIndexKey.AVG_MONTH_SECONDS;
import static org.neo4j.kernel.impl.index.schema.GenericLayout.HIGHEST_TYPE_BY_VALUE_GROUP;
import static org.neo4j.kernel.impl.index.schema.GenericLayout.LOWEST_TYPE_BY_VALUE_GROUP;
import static org.neo4j.kernel.impl.index.schema.StringIndexKey.unsignedByteArrayCompare;
import static org.neo4j.values.storable.Values.NO_VALUE;

public class GenericKey extends NativeIndexKey<GenericKey>
{
    private static final long TRUE = 1;
    private static final long FALSE = 0;

    Type type;

    // zoned date time:       long0 (epochSecondUTC), long1 (nanoOfSecond), long2 (zoneId), long3 (zoneOffsetSeconds)
    // local date time:       long0 (nanoOfSecond), long1 (epochSecond)
    // date:                  long0 (epochDay)
    // zoned time:            long0 (nanosOfDayUTC), long1 (zoneOffsetSeconds)
    // local time:            long0 (nanoOfDay)
    // duration:              long0 (totalAvgSeconds), long1 (nanosOfSecond), long2 (months), long3 (days)
    // text:                  long0 (length), long1 (bytesDereferenced), long2 (ignoreLength), byteArray
    // boolean:               long0
    // number:                long0 (value), long1 (number type)
    // TODO spatial
    // TODO arrays of all types ^^^

    long long0;
    long long1;
    long long2;
    long long3;
    byte[] byteArray;

    @Override
    void initialize( long entityId )
    {
        super.initialize( entityId );
        type = null;
        long0 = 0;
        long1 = 0;
        long2 = 0;
        long3 = 0;
    }

    @Override
    protected Value assertCorrectType( Value value )
    {
        if ( Values.isGeometryValue( value ) || Values.isArrayValue( value ) )
        {
            throw new IllegalArgumentException( "Unsupported value " + value );
        }
        return value;
    }

    @Override
    Value asValue()
    {
        switch ( type )
        {
        case ZONED_DATE_TIME:
            return zonedDateTimeAsValue();
        case LOCAL_DATE_TIME:
            return localDateTimeAsValue();
        case DATE:
            return dateAsValue();
        case ZONED_TIME:
            return zonedTimeAsValue();
        case LOCAL_TIME:
            return localTimeAsValue();
        case DURATION:
            return durationAsValue();
        case TEXT:
            return textAsValue();
        case BOOLEAN:
            return booleanAsValue();
        case NUMBER:
            return numberAsValue();
        default:
            throw new IllegalArgumentException( "Unknown type " + type );
        }
    }

    @Override
    void initValueAsLowest( ValueGroup valueGroup )
    {
        type = valueGroup == ValueGroup.UNKNOWN ? LOWEST_TYPE_BY_VALUE_GROUP : GenericLayout.TYPE_BY_GROUP[valueGroup.ordinal()];
        long0 = Long.MIN_VALUE;
        long1 = Long.MIN_VALUE;
        long2 = Long.MIN_VALUE;
        long3 = Long.MIN_VALUE;
        byteArray = null;
    }

    @Override
    void initValueAsHighest( ValueGroup valueGroup )
    {
        type = valueGroup == ValueGroup.UNKNOWN ? HIGHEST_TYPE_BY_VALUE_GROUP : GenericLayout.TYPE_BY_GROUP[valueGroup.ordinal()];
        long0 = Long.MAX_VALUE;
        long1 = Long.MAX_VALUE;
        long2 = Long.MAX_VALUE;
        long3 = Long.MAX_VALUE;
        byteArray = null;
    }

    @Override
    int compareValueTo( GenericKey other )
    {
        int typeComparison = GenericLayout.TYPE_COMPARATOR.compare( type, other.type );
        if ( typeComparison != 0 )
        {
            return typeComparison;
        }

        switch ( type )
        {
        case ZONED_DATE_TIME:
            return compareZonedDateTime( other );
        case LOCAL_DATE_TIME:
            return compareLocalDateTime( other );
        case DATE:
            return compareDate( other );
        case ZONED_TIME:
            return compareZonedTime( other );
        case LOCAL_TIME:
            return compareLocalTime( other );
        case DURATION:
            return compareDuration( other );
        case TEXT:
            return compareText( other );
        case BOOLEAN:
            return compareBoolean( other );
        case NUMBER:
            return compareNumber( other );
        default:
            throw new IllegalArgumentException( "Unknown type " + type );
        }
    }

    void copyByteArrayFromIfExists( GenericKey key, int targetLength )
    {
        if ( key.type == Type.TEXT )
        {
            setBytesLength( targetLength );
            System.arraycopy( key.byteArray, 0, byteArray, 0, targetLength );
        }
        else
        {
            byteArray = null;
        }
    }

    void setBytesLength( int length )
    {
        if ( booleanOf( long1 ) || byteArray == null || byteArray.length < length )
        {
            long1 = FALSE;

            // allocate a bit more than required so that there's a higher chance that this byte[] instance
            // can be used for more keys than just this one
            byteArray = new byte[length + length / 2];
        }
        long0 = length;
    }

    void initAsPrefixLow( String prefix )
    {
        initialize( Long.MIN_VALUE );
        writeString( prefix );
        long2 = FALSE;
        // Don't set ignoreLength = true here since the "low" a.k.a. left side of the range should care about length.
        // This will make the prefix lower than those that matches the prefix (their length is >= that of the prefix)
    }

    @Override
    protected void writeDate( long epochDay ) throws RuntimeException
    {
        type = Type.DATE;
        long0 = epochDay;
    }

    @Override
    protected void writeLocalTime( long nanoOfDay ) throws RuntimeException
    {
        type = Type.LOCAL_TIME;
        long0 = nanoOfDay;
    }

    @Override
    protected void writeTime( long nanosOfDayUTC, int offsetSeconds ) throws RuntimeException
    {
        type = Type.ZONED_TIME;
        long0 = nanosOfDayUTC;
        long1 = offsetSeconds;
    }

    @Override
    protected void writeLocalDateTime( long epochSecond, int nano ) throws RuntimeException
    {
        type = Type.LOCAL_DATE_TIME;
        long0 = nano;
        long1 = epochSecond;
    }

    @Override
    protected void writeDateTime( long epochSecondUTC, int nano, int offsetSeconds ) throws RuntimeException
    {
        type = Type.ZONED_DATE_TIME;
        long0 = epochSecondUTC;
        long1 = nano;
        long2 = -1;
        long3 = offsetSeconds;
    }

    @Override
    protected void writeDateTime( long epochSecondUTC, int nano, String zoneId ) throws RuntimeException
    {
        type = Type.ZONED_DATE_TIME;
        long0 = epochSecondUTC;
        long1 = nano;
        long2 = TimeZones.map( zoneId );
        long3 = 0;
    }

    @Override
    public void writeBoolean( boolean value ) throws RuntimeException
    {
        type = Type.BOOLEAN;
        long0 = value ? TRUE : FALSE;
    }

    @Override
    public void writeInteger( byte value )
    {
        type = Type.NUMBER;
        long0 = value;
        long1 = RawBits.BYTE;
    }

    @Override
    public void writeInteger( short value )
    {
        type = Type.NUMBER;
        long0 = value;
        long1 = RawBits.SHORT;
    }

    @Override
    public void writeInteger( int value )
    {
        type = Type.NUMBER;
        long0 = value;
        long1 = RawBits.INT;
    }

    @Override
    public void writeInteger( long value )
    {
        type = Type.NUMBER;
        long0 = value;
        long1 = RawBits.LONG;
    }

    @Override
    public void writeFloatingPoint( float value )
    {
        type = Type.NUMBER;
        long0 = Float.floatToIntBits( value );
        long1 = RawBits.FLOAT;
    }

    @Override
    public void writeFloatingPoint( double value )
    {
        type = Type.NUMBER;
        long0 = Double.doubleToLongBits( value );
        long1 = RawBits.DOUBLE;
    }

    @Override
    public void writeString( String value ) throws RuntimeException
    {
        type = Type.TEXT;
        byteArray = UTF8.encode( value );
        long0 = byteArray.length;
        long1 = FALSE;
    }

    @Override
    public void writeString( char value ) throws RuntimeException
    {
        writeString( String.valueOf( value ) );
    }

    @Override
    public void writeDuration( long months, long days, long seconds, int nanos )
    {
        type = Type.DURATION;
        long0 = months * AVG_MONTH_SECONDS + days * AVG_DAY_SECONDS + seconds;
        long1 = nanos;
        long2 = months;
        long3 = days;
    }

    void initAsPrefixHigh( String prefix )
    {
        initialize( Long.MAX_VALUE );
        writeString( prefix );
        long2 = TRUE;
    }

    private NumberValue numberAsValue()
    {
        return RawBits.asNumberValue( long0, (byte) long1 );
    }

    private BooleanValue booleanAsValue()
    {
        return Values.booleanValue( long0 == TRUE );
    }

    private Value textAsValue()
    {
        if ( byteArray == null )
        {
            return Values.NO_VALUE;
        }

        // Dereference our bytes so that we won't overwrite it on next read
        long1 = TRUE;
        return Values.utf8Value( byteArray, 0, (int) long0 );
    }

    private Value durationAsValue()
    {
        long seconds = long0 - long2 * AVG_MONTH_SECONDS - long3 * AVG_DAY_SECONDS;
        return DurationValue.duration( long2, long3, seconds, long1 );
    }

    private LocalTimeValue localTimeAsValue()
    {
        return LocalTimeValue.localTime( long0 );
    }

    private Value zonedTimeAsValue()
    {
        if ( TimeZones.validZoneOffset( (int) long1 ) )
        {
            return TimeValue.time( long0, ZoneOffset.ofTotalSeconds( (int) long1 ) );
        }
        return NO_VALUE;
    }

    private DateValue dateAsValue()
    {
        return DateValue.epochDate( long0 );
    }

    private LocalDateTimeValue localDateTimeAsValue()
    {
        return LocalDateTimeValue.localDateTime( long1, long0 );
    }

    private DateTimeValue zonedDateTimeAsValue()
    {
        return TimeZones.validZoneId( (short) long2 ) ?
               DateTimeValue.datetime( long0, long1, ZoneId.of( TimeZones.map( (short) long2 ) ) ) :
               DateTimeValue.datetime( long0, long1, ZoneOffset.ofTotalSeconds( (int) long3 ) );
    }

    private int compareNumber( GenericKey other )
    {
        return RawBits.compare( long0, (byte) long1, other.long0, (byte) other.long1 );
    }

    private int compareBoolean( GenericKey other )
    {
        return Long.compare( long0, other.long0 );
    }

    private int compareText( GenericKey other )
    {
        if ( byteArray != other.byteArray )
        {
            if ( byteArray == null )
            {
                return isHighestText() ? 1 : -1;
            }
            if ( other.byteArray == null )
            {
                return other.isHighestText() ? -1 : 1;
            }
        }
        else
        {
            return 0;
        }

        return unsignedByteArrayCompare( byteArray, (int) long0, other.byteArray, (int) other.long0, booleanOf( long2 ) | booleanOf( other.long2 ) );
    }

    private boolean isHighestText()
    {
        return getCompareId() && getEntityId() == Long.MAX_VALUE && byteArray == null;
    }

    private boolean booleanOf( long longValue )
    {
        return longValue == TRUE;
    }

    private int compareZonedDateTime( GenericKey other )
    {
        int compare = Long.compare( long0, other.long0 );
        if ( compare == 0 )
        {
            compare = Integer.compare( (int) long1, (int) other.long1 );
            if ( compare == 0 &&
                    // We need to check validity upfront without throwing exceptions, because the PageCursor might give garbage bytes
                    TimeZones.validZoneOffset( (int) long3 ) &&
                    TimeZones.validZoneOffset( (int) other.long3 ) )
            {
                // In the rare case of comparing the same instant in different time zones, we settle for
                // mapping to values and comparing using the general values comparator.
                compare = Values.COMPARATOR.compare( asValue(), other.asValue() );
            }
        }
        return compare;
    }

    private int compareLocalDateTime( GenericKey other )
    {
        int compare = Long.compare( long1, other.long1 );
        if ( compare == 0 )
        {
            compare = Integer.compare( (int) long0, (int) other.long0 );
        }
        return compare;
    }

    private int compareDate( GenericKey other )
    {
        return Long.compare( long0, other.long0 );
    }

    private int compareZonedTime( GenericKey other )
    {
        int compare = Long.compare( long0, other.long0 );
        if ( compare == 0 )
        {
            compare = Integer.compare( (int) long1, (int) other.long1 );
        }
        return compare;
    }

    private int compareLocalTime( GenericKey other )
    {
        return Long.compare( long0, other.long0 );
    }

    private int compareDuration( GenericKey other )
    {
        int comparison = Long.compare( long0, other.long0 );
        if ( comparison == 0 )
        {
            comparison = Integer.compare( (int) long1, (int) other.long1 );
            if ( comparison == 0 )
            {
                comparison = Long.compare( long2, other.long2 );
                if ( comparison == 0 )
                {
                    comparison = Long.compare( long3, other.long3 );
                }
            }
        }
        return comparison;
    }
}
