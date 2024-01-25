/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.HIGH;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.LOW;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;
import static org.neo4j.kernel.impl.index.schema.Type.booleanOf;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.PrimitiveArrayWriting;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

/**
 * Regarding why the "internal" versions of some methods which are overridden by the composite keys. Example:
 * - Consider a method a() which is used by some part of the implementation of the generic index provider.
 * - Sometimes the instance the method is called on will be a composite key.
 * - Composite keys override a() to loop over multiple state slots. Each slot is a GenericKey, too.
 * - Simply overriding a() and call slot[i].a() would result in StackOverflowError since it would be calling itself.
 * This is why aInternal() exists and GenericKey#a() is implemented by simply forwarding to aInternal().
 * #a() on a composite key is implemented by looping over multiple GenericKey instances, also calling aInternal() in each of those, instead of a().
 */
public abstract class GenericKey<KEY extends GenericKey<KEY>> extends NativeIndexKey<KEY> {
    public static final int TYPE_ID_SIZE = Byte.BYTES;
    /**
     * This is the biggest size a static (as in non-dynamic, like string), non-array value can have.
     */
    static final int BIGGEST_STATIC_SIZE = Long.BYTES * 4; // long0, long1, long2, long3

    static final long TRUE = 1;
    static final long FALSE = 0;
    /**
     * An average month is 30 days, 10 hours and 30 minutes.
     * In seconds this is (((30 * 24) + 10) * 60 + 30) * 60 = 2629800
     */
    static final long AVG_MONTH_SECONDS = 2_629_800;

    static final long AVG_DAY_SECONDS = 86_400;

    // Mutable, meta-state
    Type type;
    Inclusion inclusion;
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
    SpaceFillingCurve spaceFillingCurve;

    abstract KEY stateSlot(int slot);

    abstract Type[] getTypesById();

    abstract AbstractArrayType<?>[] getArrayTypes();

    abstract Type getLowestByValueGroup();

    abstract Type getHighestByValueGroup();

    abstract Type[] getTypesByGroup();

    /* <initializers> */
    void clear() {
        if (type == Types.TEXT && booleanOf(long1)) {
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

    void initializeToDummyValue() {
        setEntityId(Long.MIN_VALUE);
        initializeToDummyValueInternal();
    }

    void initializeToDummyValueInternal() {
        clear();
        writeInteger(0);
        inclusion = NEUTRAL;
    }

    void initValueAsLowest(ValueGroup valueGroup) {
        clear();
        type = valueGroup == ValueGroup.UNKNOWN || valueGroup == ValueGroup.ANYTHING
                ? getLowestByValueGroup()
                : getTypesByGroup()[valueGroup.ordinal()];
        type.initializeAsLowest(this);
    }

    void initValueAsHighest(ValueGroup valueGroup) {
        clear();
        type = valueGroup == ValueGroup.UNKNOWN || valueGroup == ValueGroup.ANYTHING
                ? getHighestByValueGroup()
                : getTypesByGroup()[valueGroup.ordinal()];
        type.initializeAsHighest(this);
    }

    void initAsPrefixLow(TextValue prefix) {
        prefix.writeTo(this);
        long2 = FALSE;
        inclusion = LOW;
        // Don't set ignoreLength = true here since the "low" a.k.a. left side of the range should care about length.
        // This will make the prefix lower than those that matches the prefix (their length is >= that of the prefix)
    }

    void initAsPrefixHigh(TextValue prefix) {
        prefix.writeTo(this);
        long2 = TRUE; // ignoreLength
        inclusion = HIGH;
    }

    /* </initializers> */
    void copyFrom(GenericKey<?> key) {
        setEntityId(key.getEntityId());
        setCompareId(key.getCompareId());
        copyFromInternal(key);
    }

    void copyFromInternal(GenericKey<?> key) {
        copyMetaFrom(key);
        type.copyValue(this, key);
    }

    void copyMetaFrom(GenericKey<?> key) {
        this.type = key.type;
        this.inclusion = key.inclusion;
        this.isArray = key.isArray;
        if (key.isArray) {
            this.arrayLength = key.arrayLength;
            this.currentArrayOffset = key.currentArrayOffset;
            this.isHighestArray = key.isHighestArray;
        }
    }

    void writeValue(Value value, Inclusion inclusion) {
        isArray = false;
        value.writeTo(this);
        this.inclusion = inclusion;
    }

    @Override
    void writeValue(int stateSlot, Value value, Inclusion inclusion) {
        writeValue(value, inclusion);
    }

    @Override
    void assertValidValue(int stateSlot, Value value) {
        // No need, we can handle all values
    }

    @Override
    Value[] asValues() {
        return new Value[] {asValue()};
    }

    @Override
    void initValueAsLowest(int stateSlot, ValueGroup valueGroup) {
        initValueAsLowest(valueGroup);
    }

    @Override
    void initValueAsHighest(int stateSlot, ValueGroup valueGroup) {
        initValueAsHighest(valueGroup);
    }

    static void setCursorException(PageCursor cursor, String reason) {
        cursor.setCursorException("Unable to read generic key slot due to " + reason);
    }

    @Override
    int numberOfStateSlots() {
        return 1;
    }

    @Override
    int compareValueTo(KEY other) {
        return compareValueToInternal(other);
    }

    int compareValueToInternal(KEY other) {
        if (type != other.type) {
            // These null checks guard for inconsistent reading where we're expecting a retry to occur
            // Unfortunately it's the case that SeekCursor calls these methods inside a shouldRetry.
            // Fortunately we only need to do these checks if the types aren't equal, and one of the two
            // are guaranteed to be a "real" state, i.e. not inside a shouldRetry.
            if (type == null) {
                return -1;
            }
            if (other.type == null) {
                return 1;
            }
            return Type.COMPARATOR.compare(type, other.type);
        }

        int valueComparison = type.compareValue(this, other);
        if (valueComparison != 0) {
            return valueComparison;
        }

        return inclusion.compareTo(other.inclusion);
    }

    void minimalSplitter(KEY left, KEY right, KEY into) {
        into.setCompareId(right.getCompareId());
        if (left.compareValueTo(right) != 0) {
            into.setEntityId(NO_ENTITY_ID);
        } else {
            // There was no minimal splitter to be found so entity id will serve as divider
            into.setEntityId(right.getEntityId());
        }
        minimalSplitterInternal(left, right, into);
    }

    void minimalSplitterInternal(KEY left, KEY right, KEY into) {
        into.clear();
        into.copyMetaFrom(right);
        right.type.minimalSplitter(left, right, into);
    }

    int size() {
        return ENTITY_ID_SIZE + sizeInternal();
    }

    int sizeInternal() {
        return type.valueSize(this) + TYPE_ID_SIZE;
    }

    Value asValue() {
        return type.asValue(this);
    }

    void put(PageCursor cursor) {
        cursor.putLong(getEntityId());
        putInternal(cursor);
    }

    void putInternal(PageCursor cursor) {
        cursor.putByte(type.typeId);
        type.putValue(cursor, this);
    }

    boolean get(PageCursor cursor, int size) {
        if (size < ENTITY_ID_SIZE) {
            initializeToDummyValue();
            cursor.setCursorException("Failed to read GenericKey due to keySize < ENTITY_ID_SIZE");
            return false;
        }

        initialize(cursor.getLong());
        if (!getInternal(cursor, size)) {
            initializeToDummyValue();
            return false;
        }
        return true;
    }

    boolean getInternal(PageCursor cursor, int size) {
        if (size <= TYPE_ID_SIZE) {
            GenericKey.setCursorException(cursor, "slot size less than TYPE_ID_SIZE, " + size);
            return false;
        }

        byte typeId = cursor.getByte();
        if (typeId < 0 || typeId >= getTypesById().length) {
            GenericKey.setCursorException(cursor, "non-valid typeId, " + typeId);
            return false;
        }

        inclusion = NEUTRAL;
        return setType(getTypesById()[typeId]).readValue(cursor, size - TYPE_ID_SIZE, this);
    }

    /* <write> (write to field state from Value or cursor) */

    protected <T extends Type> T setType(T type) {
        if (this.type != null && type != this.type) {
            clear();
        }
        this.type = type;
        return type;
    }

    @Override
    protected void writeDate(long epochDay) {
        if (!isArray) {
            setType(Types.DATE);
            DateType.write(this, epochDay);
        } else {
            DateArrayType.write(this, currentArrayOffset++, epochDay);
        }
    }

    @Override
    protected void writeLocalTime(long nanoOfDay) {
        if (!isArray) {
            setType(Types.LOCAL_TIME);
            LocalTimeType.write(this, nanoOfDay);
        } else {
            LocalTimeArrayType.write(this, currentArrayOffset++, nanoOfDay);
        }
    }

    @Override
    protected void writeTime(long nanosOfDayUTC, int offsetSeconds) {
        if (!isArray) {
            setType(Types.ZONED_TIME);
            ZonedTimeType.write(this, nanosOfDayUTC, offsetSeconds);
        } else {
            ZonedTimeArrayType.write(this, currentArrayOffset++, nanosOfDayUTC, offsetSeconds);
        }
    }

    @Override
    protected void writeLocalDateTime(long epochSecond, int nano) {
        if (!isArray) {
            setType(Types.LOCAL_DATE_TIME);
            LocalDateTimeType.write(this, epochSecond, nano);
        } else {
            LocalDateTimeArrayType.write(this, currentArrayOffset++, epochSecond, nano);
        }
    }

    @Override
    protected void writeDateTime(long epochSecondUTC, int nano, int offsetSeconds) {
        writeDateTime(epochSecondUTC, nano, (short) -1, offsetSeconds);
    }

    @Override
    protected void writeDateTime(long epochSecondUTC, int nano, String zoneId) {
        writeDateTime(epochSecondUTC, nano, TimeZones.map(zoneId));
    }

    protected void writeDateTime(long epochSecondUTC, int nano, short zoneId) {
        writeDateTime(epochSecondUTC, nano, zoneId, 0);
    }

    private void writeDateTime(long epochSecondUTC, int nano, short zoneId, int offsetSeconds) {
        if (!isArray) {
            setType(Types.ZONED_DATE_TIME);
            ZonedDateTimeType.write(this, epochSecondUTC, nano, zoneId, offsetSeconds);
        } else {
            ZonedDateTimeArrayType.write(this, currentArrayOffset++, epochSecondUTC, nano, zoneId, offsetSeconds);
        }
    }

    @Override
    public void writeBoolean(boolean value) {
        if (!isArray) {
            setType(Types.BOOLEAN);
            BooleanType.write(this, value);
        } else {
            BooleanArrayType.write(this, currentArrayOffset++, value);
        }
    }

    private void writeNumber(long value, byte numberType) {
        if (!isArray) {
            setType(Types.NUMBER);
            NumberType.write(this, value, numberType);
        } else {
            NumberArrayType.write(this, currentArrayOffset++, value);
        }
    }

    @Override
    public void writeInteger(byte value) {
        writeNumber(value, RawBits.BYTE);
    }

    @Override
    public void writeInteger(short value) {
        writeNumber(value, RawBits.SHORT);
    }

    @Override
    public void writeInteger(int value) {
        writeNumber(value, RawBits.INT);
    }

    @Override
    public void writeInteger(long value) {
        writeNumber(value, RawBits.LONG);
    }

    @Override
    public void writeFloatingPoint(float value) {
        writeNumber(Float.floatToIntBits(value), RawBits.FLOAT);
    }

    @Override
    public void writeFloatingPoint(double value) {
        writeNumber(Double.doubleToLongBits(value), RawBits.DOUBLE);
    }

    @Override
    public void writeString(String value) {
        writeStringBytes(UTF8.encode(value), false);
    }

    @Override
    public void writeString(char value) {
        writeStringBytes(UTF8.encode(String.valueOf(value)), true);
    }

    @Override
    public void writeUTF8(byte[] bytes, int offset, int length) {
        byte[] dest = new byte[length];
        System.arraycopy(bytes, offset, dest, 0, length);
        writeStringBytes(dest, false);
    }

    private void writeStringBytes(byte[] bytes, boolean isCharType) {
        if (!isArray) {
            setType(Types.TEXT);
            TextType.write(this, bytes, isCharType);
        } else {
            // in the array case we've already noted the char/string type in beginArray
            TextArrayType.write(this, currentArrayOffset++, bytes);
        }
        long1 = FALSE; // long1 is dereferenced true/false
    }

    @Override
    public void writeDuration(long months, long days, long seconds, int nanos) {
        long totalAvgSeconds = months * AVG_MONTH_SECONDS + days * AVG_DAY_SECONDS + seconds;
        writeDurationWithTotalAvgSeconds(months, days, totalAvgSeconds, nanos);
    }

    void writeDurationWithTotalAvgSeconds(long months, long days, long totalAvgSeconds, int nanos) {
        if (!isArray) {
            setType(Types.DURATION);
            DurationType.write(this, months, days, totalAvgSeconds, nanos);
        } else {
            DurationArrayType.write(this, currentArrayOffset++, months, days, totalAvgSeconds, nanos);
        }
    }

    // Write byte array is a special case,
    // instead of calling beginArray and writing the bytes one-by-one
    // writeByteArray is called so that the bytes can be written in batches.
    // We don't care about that though so just delegate.
    @Override
    public void writeByteArray(byte[] value) {
        PrimitiveArrayWriting.writeTo(this, value);
    }

    @Override
    public void beginArray(int size, ArrayType arrayType) {
        AbstractArrayType<?> arrayValueType = getArrayTypes()[arrayType.ordinal()];
        setType(arrayValueType);
        initializeArrayMeta(size);
        arrayValueType.initializeArray(this, size, arrayType);
    }

    void initializeArrayMeta(int size) {
        isArray = true;
        arrayLength = size;
        currentArrayOffset = 0;
    }

    @Override
    public void endArray() { // no-op
    }

    /* </write> */

    @Override
    public String toString() {
        return "[" + toStringInternal() + "],entityId=" + getEntityId();
    }

    String toStringInternal() {
        return type == null ? "<null-type>" : type.toString(this);
    }

    String toDetailedString() {
        return "[" + toDetailedStringInternal() + "],entityId=" + getEntityId();
    }

    String toDetailedStringInternal() {
        return type.toDetailedString(this);
    }
}
