/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import java.time.ZoneId;
import java.time.ZoneOffset;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.index.schema.GenericLayout.Type;
import org.neo4j.kernel.impl.store.TemporalValueWriterAdapter;
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
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.HIGH;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.LOW;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;
import static org.neo4j.kernel.impl.index.schema.StringIndexKey.unsignedByteArrayCompare;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.ZONE_ID_FLAG;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.ZONE_ID_MASK;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.asZoneId;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.asZoneOffset;
import static org.neo4j.kernel.impl.index.schema.ZonedDateTimeLayout.isZoneId;
import static org.neo4j.values.storable.Values.NO_VALUE;

class GenericKeyState extends TemporalValueWriterAdapter<RuntimeException>
{
    private static final long TRUE = 1;
    private static final long FALSE = 0;
    private static final int TYPE_ID_SIZE = Byte.BYTES;

    Type type;

    // zoned date time:       long0 (epochSecondUTC), long1 (nanoOfSecond), long2 (zoneId), long3 (zoneOffsetSeconds)
    // local date time:       long0 (nanoOfSecond), long1 (epochSecond)
    // date:                  long0 (epochDay)
    // zoned time:            long0 (nanosOfDayUTC), long1 (zoneOffsetSeconds)
    // local time:            long0 (nanoOfDay)
    // duration:              long0 (totalAvgSeconds), long1 (nanosOfSecond), long2 (months), long3 (days)
    // text:                  long0 (length), long1 (bytesDereferenced), long2 (ignoreLength), long3 (isHighest), byteArray
    // boolean:               long0
    // number:                long0 (value), long1 (number type)
    // TODO spatial
    // TODO arrays of all types ^^^

    private long long0;
    private long long1;
    private long long2;
    private long long3;
    private byte[] byteArray;
    private NativeIndexKey.Inclusion inclusion;

    void clear()
    {
        type = null;
        long0 = 0;
        long1 = 0;
        long2 = 0;
        long3 = 0;
        inclusion = NEUTRAL;
    }

    Value assertCorrectType( Value value )
    {
        if ( Values.isGeometryValue( value ) || Values.isArrayValue( value ) )
        {
            throw new IllegalArgumentException( "Unsupported value " + value );
        }
        return value;
    }

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

    void initValueAsLowest( ValueGroup valueGroup )
    {
        type = valueGroup == ValueGroup.UNKNOWN ? LOWEST_TYPE_BY_VALUE_GROUP : GenericLayout.TYPE_BY_GROUP[valueGroup.ordinal()];
        long0 = Long.MIN_VALUE;
        long1 = Long.MIN_VALUE;
        long2 = Long.MIN_VALUE;
        long3 = Long.MIN_VALUE;
        byteArray = null;
        if ( type == Type.TEXT )
        {
            long3 = FALSE;
        }
        inclusion = LOW;
    }

    void initValueAsHighest( ValueGroup valueGroup )
    {
        type = valueGroup == ValueGroup.UNKNOWN ? HIGHEST_TYPE_BY_VALUE_GROUP : GenericLayout.TYPE_BY_GROUP[valueGroup.ordinal()];
        long0 = Long.MAX_VALUE;
        long1 = Long.MAX_VALUE;
        long2 = Long.MAX_VALUE;
        long3 = Long.MAX_VALUE;
        byteArray = null;
        if ( type == Type.TEXT )
        {
            long3 = TRUE;
        }
        inclusion = HIGH;
    }

    int compareValueTo( GenericKeyState other )
    {
        int typeComparison = GenericLayout.TYPE_COMPARATOR.compare( type, other.type );
        if ( typeComparison != 0 )
        {
            return typeComparison;
        }

        int valueComparison = internalCompareValueTo( other );
        if ( valueComparison != 0 )
        {
            return valueComparison;
        }

        return inclusion.compareTo( other.inclusion );
    }

    private int internalCompareValueTo( GenericKeyState other )
    {
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

    private void copyByteArrayFromIfExists( GenericKeyState key, int targetLength )
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

    private void setBytesLength( int length )
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
        clear();
        writeString( prefix );
        long2 = FALSE;
        inclusion = LOW;
        // Don't set ignoreLength = true here since the "low" a.k.a. left side of the range should care about length.
        // This will make the prefix lower than those that matches the prefix (their length is >= that of the prefix)
    }

    void initAsPrefixHigh( String prefix )
    {
        clear();
        writeString( prefix );
        long2 = TRUE;
        inclusion = HIGH;
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

    private int compareNumber( GenericKeyState other )
    {
        return RawBits.compare( long0, (byte) long1, other.long0, (byte) other.long1 );
    }

    private int compareBoolean( GenericKeyState other )
    {
        return Long.compare( long0, other.long0 );
    }

    private int compareText( GenericKeyState other )
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
        return long3 == TRUE;
    }

    private boolean booleanOf( long longValue )
    {
        return longValue == TRUE;
    }

    private int compareZonedDateTime( GenericKeyState other )
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

    private int compareLocalDateTime( GenericKeyState other )
    {
        int compare = Long.compare( long1, other.long1 );
        if ( compare == 0 )
        {
            compare = Integer.compare( (int) long0, (int) other.long0 );
        }
        return compare;
    }

    private int compareDate( GenericKeyState other )
    {
        return Long.compare( long0, other.long0 );
    }

    private int compareZonedTime( GenericKeyState other )
    {
        int compare = Long.compare( long0, other.long0 );
        if ( compare == 0 )
        {
            compare = Integer.compare( (int) long1, (int) other.long1 );
        }
        return compare;
    }

    private int compareLocalTime( GenericKeyState other )
    {
        return Long.compare( long0, other.long0 );
    }

    private int compareDuration( GenericKeyState other )
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

    void copyFrom( GenericKeyState key )
    {
        this.type = key.type;
        this.long0 = key.long0;
        this.long1 = key.long1;
        this.long2 = key.long2;
        this.long3 = key.long3;
        this.copyByteArrayFromIfExists( key, (int) key.long0 );
        this.inclusion = key.inclusion;
    }

    int size()
    {
        // TODO copy-pasted from individual keys
        // TODO also put this in Type enum
        switch ( type )
        {
        case ZONED_DATE_TIME:
            return Long.BYTES +    /* epochSecond */
                   Integer.BYTES + /* nanoOfSecond */
                   Integer.BYTES;  /* timeZone */
        case LOCAL_DATE_TIME:
            return Long.BYTES +    /* epochSecond */
                   Integer.BYTES;  /* nanoOfSecond */
        case DATE:
            return Long.BYTES;     /* epochDay */
        case ZONED_TIME:
            return Long.BYTES +    /* nanosOfDayUTC */
                   Integer.BYTES;  /* zoneOffsetSeconds */
        case LOCAL_TIME:
            return Long.BYTES;     /* nanoOfDay */
        case DURATION:
            return Long.BYTES +    /* totalAvgSeconds */
                   Integer.BYTES + /* nanosOfSecond */
                   Long.BYTES +    /* months */
                   Long.BYTES;     /* days */
        case TEXT:
            return Short.SIZE +    /* short field with bytesLength value */
                   (int) long0;    /* bytesLength */
        case BOOLEAN:
            return Byte.BYTES;     /* byte for this boolean value */
        case NUMBER:
            return Byte.BYTES +    /* type of value */
                   Long.BYTES;     /* raw value bits */
        default:
            throw new IllegalArgumentException( "Unknown type " + type );
        }
    }

    void write( PageCursor cursor )
    {
        cursor.putByte( type.typeId );
        switch ( type )
        {
        case ZONED_DATE_TIME:
            writeZonedDateTime( cursor );
            break;
        case LOCAL_DATE_TIME:
            writeLocalDateTime( cursor );
            break;
        case DATE:
            writeDate( cursor );
            break;
        case ZONED_TIME:
            writeZonedTime( cursor );
            break;
        case LOCAL_TIME:
            writeLocalTime( cursor );
            break;
        case DURATION:
            writeDuration( cursor );
            break;
        case TEXT:
            writeText( cursor );
            break;
        case BOOLEAN:
            writeBoolean( cursor );
            break;
        case NUMBER:
            writeNumber( cursor );
            break;
        default:
            throw new IllegalArgumentException( "Unknown type " + type );
        }
    }

    private void writeNumber( PageCursor cursor )
    {
        cursor.putByte( (byte) long1 );
        cursor.putLong( long0 );
    }

    private void writeBoolean( PageCursor cursor )
    {
        cursor.putByte( (byte) long0 );
    }

    private void writeText( PageCursor cursor )
    {
        // TODO short/int weird asymmetry ey?
        cursor.putShort( (short) long0 );
        cursor.putBytes( byteArray, 0, (int) long0 );
    }

    private void writeDuration( PageCursor cursor )
    {
        cursor.putLong( long0 );
        cursor.putInt( (int) long1 );
        cursor.putLong( long2 );
        cursor.putLong( long3 );
    }

    private void writeLocalTime( PageCursor cursor )
    {
        cursor.putLong( long0 );
    }

    private void writeZonedTime( PageCursor cursor )
    {
        cursor.putLong( long0 );
        cursor.putInt( (int) long1 );
    }

    private void writeDate( PageCursor cursor )
    {
        cursor.putLong( long0 );
    }

    private void writeLocalDateTime( PageCursor cursor )
    {
        cursor.putLong( long1 );
        cursor.putInt( (int) long0 );
    }

    private void writeZonedDateTime( PageCursor cursor )
    {
        cursor.putLong( long0 );
        cursor.putInt( (int) long1 );
        if ( long2 >= 0 )
        {
            cursor.putInt( (int) long2 | ZONE_ID_FLAG );
        }
        else
        {
            cursor.putInt( (int) long3 & ZONE_ID_MASK );
        }
    }

    void read( PageCursor cursor, int size )
    {
        if ( size <= TYPE_ID_SIZE )
        {
            initializeToDummyValue();
            return;
        }

        byte typeId = cursor.getByte();
        if ( typeId < 0 || typeId >= GenericLayout.TYPES.length )
        {
            initializeToDummyValue();
            return;
        }

        size -= TYPE_ID_SIZE;
        type = GenericLayout.TYPE_BY_ID[typeId];
        inclusion = NEUTRAL;
        switch ( type )
        {
        case ZONED_DATE_TIME:
            readZonedDateTime( cursor );
            break;
        case LOCAL_DATE_TIME:
            readLocalDateTime( cursor );
            break;
        case DATE:
            readDate( cursor );
            break;
        case ZONED_TIME:
            readZonedTime( cursor );
            break;
        case LOCAL_TIME:
            readLocalTime( cursor );
            break;
        case DURATION:
            readDuration( cursor );
            break;
        case TEXT:
            readText( cursor, size );
            break;
        case BOOLEAN:
            readBoolean( cursor );
            break;
        case NUMBER:
            readNumber( cursor );
            break;
        default:
            throw new IllegalArgumentException( "Unknown type " + type );
        }
    }

    void initializeToDummyValue()
    {
        type = Type.NUMBER;
        long0 = 0;
        long1 = 0;
        inclusion = NEUTRAL;
    }

    private void readNumber( PageCursor cursor )
    {
        long1 = cursor.getByte();
        long0 = cursor.getLong();
    }

    private void readBoolean( PageCursor cursor )
    {
        long0 = cursor.getByte();
    }

    private void readText( PageCursor cursor, int maxSize )
    {
        short bytesLength = cursor.getShort();
        if ( bytesLength <= 0 || bytesLength > maxSize )
        {
            initializeToDummyValue();
            return;
        }
        setBytesLength( bytesLength );
        cursor.getBytes( byteArray, 0, bytesLength );
    }

    private void readDuration( PageCursor cursor )
    {
        long0 = cursor.getLong();
        long1 = cursor.getInt();
        long2 = cursor.getLong();
        long3 = cursor.getLong();
    }

    private void readLocalTime( PageCursor cursor )
    {
        long0 = cursor.getLong();
    }

    private void readZonedTime( PageCursor cursor )
    {
        long0 = cursor.getLong();
        long1 = cursor.getInt();
    }

    private void readDate( PageCursor cursor )
    {
        long0 = cursor.getLong();
    }

    private void readLocalDateTime( PageCursor cursor )
    {
        long1 = cursor.getLong();
        long0 = cursor.getInt();
    }

    private void readZonedDateTime( PageCursor cursor )
    {
        long0 = cursor.getLong();
        long1 = cursor.getInt();
        int encodedZone = cursor.getInt();
        if ( isZoneId( encodedZone ) )
        {
            long2 = asZoneId( encodedZone );
            long3 = 0;
        }
        else
        {
            long2 = -1;
            long3 = asZoneOffset( encodedZone );
        }
    }

    void writeValue( Value value, NativeIndexKey.Inclusion inclusion )
    {
        value.writeTo( this );
        this.inclusion = inclusion;
    }
}
