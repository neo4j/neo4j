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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PrimitiveArrayWriting;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.String.format;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.kernel.impl.index.schema.DurationIndexKey.AVG_DAY_SECONDS;
import static org.neo4j.kernel.impl.index.schema.DurationIndexKey.AVG_MONTH_SECONDS;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.HIGH;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.LOW;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;
import static org.neo4j.kernel.impl.index.schema.Type.booleanOf;
import static org.neo4j.kernel.impl.index.schema.Types.HIGHEST_BY_VALUE_GROUP;
import static org.neo4j.kernel.impl.index.schema.Types.LOWEST_BY_VALUE_GROUP;

/**
 * A key instance which can handle all types of single values, i.e. not composite keys, but all value types.
 * See {@link CompositeGenericKey} for implementation which supports composite keys.
 *
 * Regarding why the "internal" versions of some methods which are overridden by the CompositeGenericKey sub-class. Example:
 * - Consider a method a() which is used by some part of the implementation of the generic index provider.
 * - Sometimes the instance the method is called on will be a CompositeGenericKey.
 * - CompositeGenericKey overrides a() to loop over multiple state slots. Each slot is a GenericKey too.
 * - Simply overriding a() and call slot[i].a() would result in StackOverflowError since it would be calling itself.
 * This is why aInternal() exists and GenericKey#a() is implemented by simply forwarding to aInternal().
 * CompositeGenericKey#a() is implemented by looping over multiple GenericKey instances, also calling aInternal() in each of those, instead of a().
 */
public class GenericKey extends NativeIndexKey<GenericKey>
{
    /**
     * This is the biggest size a static (as in non-dynamic, like string), non-array value can have.
     */
    static final int BIGGEST_STATIC_SIZE = Long.BYTES * 4; // long0, long1, long2, long3

    // TODO copy-pasted from individual keys
    // TODO also put this in Type enum
    public static final int SIZE_GEOMETRY_HEADER = 3;              /* 2b tableId and 22b code */
    public static final int SIZE_GEOMETRY =        Long.BYTES;     /* rawValueBits */
    static final int SIZE_GEOMETRY_COORDINATE =    Long.BYTES;     /* one coordinate */
    public static final int SIZE_ZONED_DATE_TIME = Long.BYTES +    /* epochSecond */
                                                   Integer.BYTES + /* nanoOfSecond */
                                                   Integer.BYTES;  /* timeZone */
    public static final int SIZE_LOCAL_DATE_TIME = Long.BYTES +    /* epochSecond */
                                                   Integer.BYTES;  /* nanoOfSecond */
    public static final int SIZE_DATE =            Long.BYTES;     /* epochDay */
    public static final int SIZE_ZONED_TIME =      Long.BYTES +    /* nanosOfDayUTC */
                                                   Integer.BYTES;  /* zoneOffsetSeconds */
    public static final int SIZE_LOCAL_TIME =      Long.BYTES;     /* nanoOfDay */
    public static final int SIZE_DURATION =        Long.BYTES +    /* totalAvgSeconds */
                                                   Integer.BYTES + /* nanosOfSecond */
                                                   Long.BYTES +    /* months */
                                                   Long.BYTES;     /* days */
    public static final int SIZE_STRING_LENGTH =   Short.BYTES;    /* length of string byte array */
    public static final int SIZE_BOOLEAN =         Byte.BYTES;     /* byte for this boolean value */
    public static final int SIZE_NUMBER_TYPE =     Byte.BYTES;     /* type of value */
    public static final int SIZE_NUMBER_BYTE =     Byte.BYTES;     /* raw value bits */
    public static final int SIZE_NUMBER_SHORT =    Short.BYTES;    /* raw value bits */
    public static final int SIZE_NUMBER_INT =      Integer.BYTES;  /* raw value bits */
    public static final int SIZE_NUMBER_LONG =     Long.BYTES;     /* raw value bits */
    public static final int SIZE_NUMBER_FLOAT =    Integer.BYTES;  /* raw value bits */
    public static final int SIZE_NUMBER_DOUBLE =   Long.BYTES;     /* raw value bits */
    public static final int SIZE_ARRAY_LENGTH =    Short.BYTES;
    static final int BIGGEST_REASONABLE_ARRAY_LENGTH = PAGE_SIZE / 2 / SIZE_NUMBER_BYTE;

    static final long TRUE = 1;
    static final long FALSE = 0;
    static final int NO_ENTITY_ID = -1;
    private static final int TYPE_ID_SIZE = Byte.BYTES;
    private static final double[] NO_COORDINATES = new double[0];

    // Immutable
    private final IndexSpecificSpaceFillingCurveSettingsCache settings;

    // Mutable, meta-state
    Type type;
    NativeIndexKey.Inclusion inclusion;
    boolean isArray;

    // Mutable, non-array values
    long long0;
    long long1;
    long long2;
    long long3;
    byte[] byteArray;

    // Mutable, array values
    long[] long0Array;
    long[] long1Array;
    long[] long2Array;
    long[] long3Array;
    byte[][] byteArrayArray;
    boolean isHighestArray;
    int arrayLength;
    int currentArrayOffset;

    // Mutable, spatial values
    /*
     * Settings for a specific crs retrieved from #settings using #long1 and #long2.
     */
    SpaceFillingCurve spaceFillingCurve;

    GenericKey( IndexSpecificSpaceFillingCurveSettingsCache settings )
    {
        this.settings = settings;
    }

    /* <initializers> */
    void clear()
    {
        if ( type == Types.TEXT && booleanOf( long1 ) )
        {
            // Clear byteArray if it has been dereferenced
            byteArray = null;
        }
        type = null;
        long0 = 0;
        long1 = 0;
        long2 = 0;
        long3 = 0;
        inclusion = NEUTRAL;
        isArray = false;
        arrayLength = 0;
        isHighestArray = false;
        currentArrayOffset = 0;
        spaceFillingCurve = null;
    }

    void initializeToDummyValue()
    {
        setEntityId( Long.MIN_VALUE );
        initializeToDummyValueInternal();
    }

    void initializeToDummyValueInternal()
    {
        clear();
        writeInteger( 0 );
        inclusion = NEUTRAL;
    }

    void initValueAsLowest( ValueGroup valueGroup )
    {
        clear();
        type = valueGroup == ValueGroup.UNKNOWN ? LOWEST_BY_VALUE_GROUP : Types.BY_GROUP[valueGroup.ordinal()];
        type.initializeAsLowest( this );
    }

    void initValueAsHighest( ValueGroup valueGroup )
    {
        clear();
        type = valueGroup == ValueGroup.UNKNOWN ? HIGHEST_BY_VALUE_GROUP : Types.BY_GROUP[valueGroup.ordinal()];
        type.initializeAsHighest( this );
    }

    void initAsPrefixLow( TextValue prefix )
    {
        prefix.writeTo( this );
        long2 = FALSE;
        inclusion = LOW;
        // Don't set ignoreLength = true here since the "low" a.k.a. left side of the range should care about length.
        // This will make the prefix lower than those that matches the prefix (their length is >= that of the prefix)
    }

    void initAsPrefixHigh( TextValue prefix )
    {
        prefix.writeTo( this );
        long2 = TRUE; // ignoreLength
        inclusion = HIGH;
    }

    /* </initializers> */
    void copyFrom( GenericKey key )
    {
        setEntityId( key.getEntityId() );
        setCompareId( key.getCompareId() );
        copyFromInternal( key );
    }

    void copyFromInternal( GenericKey key )
    {
        copyMetaFrom( key );
        type.copyValue( this, key );
    }

    void copyMetaFrom( GenericKey key )
    {
        this.type = key.type;
        this.inclusion = key.inclusion;
        this.isArray = key.isArray;
        if ( key.isArray )
        {
            this.arrayLength = key.arrayLength;
            this.currentArrayOffset = key.currentArrayOffset;
            this.isHighestArray = key.isHighestArray;
        }
    }

    void writeValue( Value value, NativeIndexKey.Inclusion inclusion )
    {
        isArray = false;
        value.writeTo( this );
        this.inclusion = inclusion;
    }

    @Override
    void writeValue( int stateSlot, Value value, Inclusion inclusion )
    {
        writeValue( value, inclusion );
    }

    @Override
    void assertValidValue( int stateSlot, Value value )
    {
        // No need, we can handle all values
    }

    @Override
    Value[] asValues()
    {
        return new Value[] {asValue()};
    }

    @Override
    void initValueAsLowest( int stateSlot, ValueGroup valueGroup )
    {
        initValueAsLowest( valueGroup );
    }

    @Override
    void initValueAsHighest( int stateSlot, ValueGroup valueGroup )
    {
        initValueAsHighest( valueGroup );
    }

    GenericKey stateSlot( int slot )
    {
        assert slot == 0;
        return this;
    }

    @Override
    int numberOfStateSlots()
    {
        return 1;
    }

    @Override
    int compareValueTo( GenericKey other )
    {
        return compareValueToInternal( other );
    }

    int compareValueToInternal( GenericKey other )
    {
        if ( type != other.type )
        {
            // These null checks guard for inconsistent reading where we're expecting a retry to occur
            // Unfortunately it's the case that SeekCursor calls these methods inside a shouldRetry.
            // Fortunately we only need to do these checks if the types aren't equal, and one of the two
            // are guaranteed to be a "real" state, i.e. not inside a shouldRetry.
            if ( type == null )
            {
                return -1;
            }
            if ( other.type == null )
            {
                return 1;
            }
            return Type.COMPARATOR.compare( type, other.type );
        }

        int valueComparison = type.compareValue( this, other );
        if ( valueComparison != 0 )
        {
            return valueComparison;
        }

        return inclusion.compareTo( other.inclusion );
    }

    void minimalSplitter( GenericKey left, GenericKey right, GenericKey into )
    {
        into.setCompareId( right.getCompareId() );
        if ( left.compareValueTo( right ) != 0 )
        {
            into.setEntityId( NO_ENTITY_ID );
        }
        else
        {
            // There was no minimal splitter to be found so entity id will serve as divider
            into.setEntityId( right.getEntityId() );
        }
        minimalSplitterInternal( left, right, into );
    }

    void minimalSplitterInternal( GenericKey left, GenericKey right, GenericKey into )
    {
        into.clear();
        into.copyMetaFrom( right );
        right.type.minimalSplitter( left, right, into );
    }

    int size()
    {
        return ENTITY_ID_SIZE + sizeInternal();
    }

    int sizeInternal()
    {
        return type.valueSize( this ) + TYPE_ID_SIZE;
    }

    Value asValue()
    {
        return type.asValue( this );
    }

    void put( PageCursor cursor )
    {
        cursor.putLong( getEntityId() );
        putInternal( cursor );
    }

    void putInternal( PageCursor cursor )
    {
        cursor.putByte( type.typeId );
        type.putValue( cursor, this );
    }

    boolean get( PageCursor cursor, int size )
    {
        if ( size < ENTITY_ID_SIZE )
        {
            initializeToDummyValue();
            cursor.setCursorException( format( "Failed to read " + getClass().getSimpleName() +
                    " due to keySize < ENTITY_ID_SIZE, more precisely %d", size ) );
            return false;
        }

        initialize( cursor.getLong() );
        if ( !getInternal( cursor, size ) )
        {
            initializeToDummyValue();
            return false;
        }
        return true;
    }

    boolean getInternal( PageCursor cursor, int size )
    {
        if ( size <= TYPE_ID_SIZE )
        {
            setCursorException( cursor, "slot size less than TYPE_ID_SIZE, " + size );
            return false;
        }

        byte typeId = cursor.getByte();
        if ( typeId < 0 || typeId >= Types.BY_ID.length )
        {
            setCursorException( cursor, "non-valid typeId, " + typeId );
            return false;
        }

        inclusion = NEUTRAL;
        return setType( Types.BY_ID[typeId] ).readValue( cursor, size - TYPE_ID_SIZE, this );
    }

    /* <write> (write to field state from Value or cursor) */

    private <T extends Type> T setType( T type )
    {
        if ( this.type != null && type != this.type )
        {
            clear();
        }
        this.type = type;
        return type;
    }

    @Override
    protected void writeDate( long epochDay )
    {
        if ( !isArray )
        {
            setType( Types.DATE ).write( this, epochDay );
        }
        else
        {
            Types.DATE_ARRAY.write( this, currentArrayOffset++, epochDay );
        }
    }

    @Override
    protected void writeLocalTime( long nanoOfDay )
    {
        if ( !isArray )
        {
            setType( Types.LOCAL_TIME ).write( this, nanoOfDay );
        }
        else
        {
            Types.LOCAL_TIME_ARRAY.write( this, currentArrayOffset++, nanoOfDay );
        }
    }

    @Override
    protected void writeTime( long nanosOfDayUTC, int offsetSeconds )
    {
        if ( !isArray )
        {
            setType( Types.ZONED_TIME ).write( this, nanosOfDayUTC, offsetSeconds );
        }
        else
        {
            Types.ZONED_TIME_ARRAY.write( this, currentArrayOffset++, nanosOfDayUTC, offsetSeconds );
        }
    }

    @Override
    protected void writeLocalDateTime( long epochSecond, int nano )
    {
        if ( !isArray )
        {
            setType( Types.LOCAL_DATE_TIME ).write( this, epochSecond, nano );
        }
        else
        {
            Types.LOCAL_DATE_TIME_ARRAY.write( this, currentArrayOffset++, epochSecond, nano );
        }
    }

    @Override
    protected void writeDateTime( long epochSecondUTC, int nano, int offsetSeconds )
    {
        writeDateTime( epochSecondUTC, nano, (short) -1, offsetSeconds );
    }

    @Override
    protected void writeDateTime( long epochSecondUTC, int nano, String zoneId )
    {
        writeDateTime( epochSecondUTC, nano, TimeZones.map( zoneId ) );
    }

    protected void writeDateTime( long epochSecondUTC, int nano, short zoneId )
    {
        writeDateTime( epochSecondUTC, nano, zoneId, 0 );
    }

    private void writeDateTime( long epochSecondUTC, int nano, short zoneId, int offsetSeconds )
    {
        if ( !isArray )
        {
            setType( Types.ZONED_DATE_TIME ).write( this, epochSecondUTC, nano, zoneId, offsetSeconds );
        }
        else
        {
            Types.ZONED_DATE_TIME_ARRAY.write( this, currentArrayOffset++, epochSecondUTC, nano, zoneId, offsetSeconds );
        }
    }

    @Override
    public void writeBoolean( boolean value )
    {
        if ( !isArray )
        {
            setType( Types.BOOLEAN ).write( this, value );
        }
        else
        {
            Types.BOOLEAN_ARRAY.write( this, currentArrayOffset++, value );
        }
    }

    private void writeNumber( long value, byte numberType )
    {
        if ( !isArray )
        {
            setType( Types.NUMBER ).write( this, value, numberType );
        }
        else
        {
            Types.NUMBER_ARRAY.write( this, currentArrayOffset++, value );
        }
    }

    @Override
    public void writeInteger( byte value )
    {
        writeNumber( value, RawBits.BYTE );
    }

    @Override
    public void writeInteger( short value )
    {
        writeNumber( value, RawBits.SHORT );
    }

    @Override
    public void writeInteger( int value )
    {
        writeNumber( value, RawBits.INT );
    }

    static short toNonNegativeShortExact( long value )
    {
        if ( (value & ~0x7FFF) != 0 )
        {
            throw new IllegalArgumentException( value + " is bigger than maximum for a signed short (2B) " + 0x7FFF );
        }
        return (short) value;
    }

    @Override
    public void writeInteger( long value )
    {
        writeNumber( value, RawBits.LONG );
    }

    @Override
    public void writeFloatingPoint( float value )
    {
        writeNumber( Float.floatToIntBits( value ), RawBits.FLOAT );
    }

    @Override
    public void writeFloatingPoint( double value )
    {
        writeNumber( Double.doubleToLongBits( value ), RawBits.DOUBLE );
    }

    @Override
    public void writeString( String value )
    {
        writeStringBytes( UTF8.encode( value ), false );
    }

    @Override
    public void writeString( char value )
    {
        writeStringBytes( UTF8.encode( String.valueOf( value ) ), true );
    }

    @Override
    public void writeUTF8( byte[] bytes, int offset, int length )
    {
        byte[] dest = new byte[length];
        System.arraycopy( bytes, offset, dest, 0, length );
        writeStringBytes( dest, false );
    }

    private void writeStringBytes( byte[] bytes, boolean isCharType )
    {
        if ( !isArray )
        {
            setType( Types.TEXT ).write( this, bytes, isCharType );
        }
        else
        {
            // in the array case we've already noted the char/string type in beginArray
            Types.TEXT_ARRAY.write( this, currentArrayOffset++, bytes );
        }
        long1 = FALSE; // long1 is dereferenced true/false
    }

    @Override
    public void writeDuration( long months, long days, long seconds, int nanos )
    {
        long totalAvgSeconds = months * AVG_MONTH_SECONDS + days * AVG_DAY_SECONDS + seconds;
        writeDurationWithTotalAvgSeconds( months, days, totalAvgSeconds, nanos );
    }

    @Override
    public void writePoint( CoordinateReferenceSystem crs, double[] coordinate )
    {
        if ( !isArray )
        {
            setType( Types.GEOMETRY );
            updateCurve( crs.getTable().getTableId(), crs.getCode() );
            Types.GEOMETRY.write( this, spaceFillingCurve.derivedValueFor( coordinate ), coordinate );
        }
        else
        {
            if ( currentArrayOffset == 0 )
            {
                updateCurve( crs.getTable().getTableId(), crs.getCode() );
            }
            else if ( this.long1 != crs.getTable().getTableId() || this.long2 != crs.getCode() )
            {
                throw new IllegalStateException( format(
                        "Tried to assign a geometry array containing different coordinate reference systems, first:%s, violating:%s at array position:%d",
                        CoordinateReferenceSystem.get( (int) long1, (int) long2 ), crs, currentArrayOffset ) );
            }
            Types.GEOMETRY_ARRAY.write( this, currentArrayOffset++, spaceFillingCurve.derivedValueFor( coordinate ), coordinate );
        }
    }

    void writePointDerived( CoordinateReferenceSystem crs, long derivedValue, NativeIndexKey.Inclusion inclusion )
    {
        if ( isArray )
        {
            throw new IllegalStateException( "This method is intended to be called when querying, where one or more sub-ranges are derived " +
                    "from a queried range and each sub-range written to separate keys. " +
                    "As such it's unexpected that this key state thinks that it's holds state for an array" );
        }
        updateCurve( crs.getTable().getTableId(), crs.getCode() );
        setType( Types.GEOMETRY ).write( this, derivedValue, NO_COORDINATES );
        this.inclusion = inclusion;
    }

    private void updateCurve( int tableId, int code )
    {
        if ( this.long1 != tableId || this.long2 != code )
        {
            long1 = tableId;
            long2 = code;
            spaceFillingCurve = settings.forCrs( tableId, code, true );
        }
    }

    void writeDurationWithTotalAvgSeconds( long months, long days, long totalAvgSeconds, int nanos )
    {
        if ( !isArray )
        {
            setType( Types.DURATION ).write( this, months, days, totalAvgSeconds, nanos );
        }
        else
        {
            Types.DURATION_ARRAY.write( this, currentArrayOffset++, months, days, totalAvgSeconds, nanos );
        }
    }
    // Write byte array is a special case,

    // instead of calling beginArray and writing the bytes one-by-one
    // writeByteArray is called so that the bytes can be written in batches.
    // We don't care about that though so just delegate.
    @Override
    public void writeByteArray( byte[] value )
    {
        PrimitiveArrayWriting.writeTo( this, value );
    }

    @Override
    public void beginArray( int size, ArrayType arrayType )
    {
        AbstractArrayType<?> arrayValueType = Types.BY_ARRAY_TYPE[arrayType.ordinal()];
        setType( arrayValueType );
        initializeArrayMeta( size );
        arrayValueType.initializeArray( this, size, arrayType );
    }

    void initializeArrayMeta( int size )
    {
        isArray = true;
        arrayLength = size;
        currentArrayOffset = 0;
    }

    @Override
    public void endArray()
    {   // no-op
    }
    /* </write.array> */

    /* </write> */

    @Override
    public String toString()
    {
        return "[" + toStringInternal() + "],entityId=" + getEntityId();
    }

    String toStringInternal()
    {
        return type.toString( this );
    }

    String toDetailedString()
    {
        return "[" + toDetailedStringInternal() + "],entityId=" + getEntityId();
    }

    String toDetailedStringInternal()
    {
        return type.toDetailedString( this );
    }

    static void setCursorException( PageCursor cursor, String reason )
    {
        cursor.setCursorException( format( "Unable to read generic key slot due to %s", reason ) );
    }
}
