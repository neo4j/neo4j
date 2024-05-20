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
package org.neo4j.packstream.io;

import static org.neo4j.packstream.io.Type.BOOLEAN;
import static org.neo4j.packstream.io.Type.INT;
import static org.neo4j.packstream.io.Type.INT16_MAX;
import static org.neo4j.packstream.io.Type.INT16_MIN;
import static org.neo4j.packstream.io.Type.INT32_MAX;
import static org.neo4j.packstream.io.Type.INT32_MIN;
import static org.neo4j.packstream.io.Type.INT8_MIN;
import static org.neo4j.packstream.io.Type.LIST;
import static org.neo4j.packstream.io.Type.MAP;
import static org.neo4j.packstream.io.Type.STRING;
import static org.neo4j.packstream.io.Type.STRING_CHARSET;
import static org.neo4j.packstream.io.Type.STRUCT;
import static org.neo4j.packstream.io.Type.TINY_INT_MAX;
import static org.neo4j.packstream.io.Type.TINY_INT_MIN;
import static org.neo4j.packstream.io.TypeMarker.BYTES16;
import static org.neo4j.packstream.io.TypeMarker.BYTES32;
import static org.neo4j.packstream.io.TypeMarker.BYTES8;
import static org.neo4j.packstream.io.TypeMarker.BYTES_TYPES;
import static org.neo4j.packstream.io.TypeMarker.FALSE;
import static org.neo4j.packstream.io.TypeMarker.FLOAT64;
import static org.neo4j.packstream.io.TypeMarker.INT16;
import static org.neo4j.packstream.io.TypeMarker.INT32;
import static org.neo4j.packstream.io.TypeMarker.INT64;
import static org.neo4j.packstream.io.TypeMarker.INT8;
import static org.neo4j.packstream.io.TypeMarker.LIST16;
import static org.neo4j.packstream.io.TypeMarker.LIST32;
import static org.neo4j.packstream.io.TypeMarker.LIST8;
import static org.neo4j.packstream.io.TypeMarker.LIST_TYPES;
import static org.neo4j.packstream.io.TypeMarker.MAP16;
import static org.neo4j.packstream.io.TypeMarker.MAP32;
import static org.neo4j.packstream.io.TypeMarker.MAP8;
import static org.neo4j.packstream.io.TypeMarker.MAP_TYPES;
import static org.neo4j.packstream.io.TypeMarker.NULL;
import static org.neo4j.packstream.io.TypeMarker.STRING16;
import static org.neo4j.packstream.io.TypeMarker.STRING32;
import static org.neo4j.packstream.io.TypeMarker.STRING8;
import static org.neo4j.packstream.io.TypeMarker.STRING_TYPES;
import static org.neo4j.packstream.io.TypeMarker.STRUCT_TYPES;
import static org.neo4j.packstream.io.TypeMarker.TINY_INT;
import static org.neo4j.packstream.io.TypeMarker.TINY_LIST;
import static org.neo4j.packstream.io.TypeMarker.TINY_MAP;
import static org.neo4j.packstream.io.TypeMarker.TINY_STRING;
import static org.neo4j.packstream.io.TypeMarker.TRUE;
import static org.neo4j.packstream.io.TypeMarker.decodeLengthNibble;
import static org.neo4j.packstream.io.TypeMarker.encodeLengthNibble;
import static org.neo4j.packstream.io.TypeMarker.requireEncodableLength;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.neo4j.packstream.error.reader.LimitExceededException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedStructException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.reader.UnexpectedTypeMarkerException;
import org.neo4j.packstream.io.function.Reader;
import org.neo4j.packstream.io.function.Writer;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructRegistry;

public final class PackstreamBuf implements ReferenceCounted {

    private final ByteBuf delegate;

    private PackstreamBuf(ByteBuf delegate) {
        this.delegate = delegate;
    }

    /**
     * Allocates a new buffer for writing.
     *
     * @param alloc an allocator.
     * @return a buffer.
     */
    public static PackstreamBuf alloc(ByteBufAllocator alloc) {
        if (alloc == null) {
            throw new NullPointerException("alloc cannot be null");
        }

        return wrap(alloc.buffer());
    }

    /**
     * Allocates an unpooled buffer for writing.
     *
     * @return a buffer.
     */
    public static PackstreamBuf allocUnpooled() {
        return wrap(Unpooled.buffer());
    }

    /**
     * Wraps a given netty buffer for reading and writing.
     *
     * @param delegate a target buffer.
     * @return a wrapped buffer.
     */
    public static PackstreamBuf wrap(ByteBuf delegate) {
        if (delegate == null) {
            throw new NullPointerException("delegate cannot be null");
        }

        return new PackstreamBuf(delegate);
    }

    /**
     * Wraps a given netty buffer and increments its reference counter.
     *
     * @param delegate a target buffer.
     * @return a wrapped buffer.
     */
    public static PackstreamBuf wrapRetained(ByteBuf delegate) {
        if (delegate == null) {
            throw new NullPointerException("delegate cannot be null");
        }

        return new PackstreamBuf(delegate.retain());
    }

    public ByteBuf getTarget() {
        return this.delegate;
    }

    /**
     * Performs a given set of operations without advancing the reader index of this buffer.
     *
     * @param consumer a consumer function.
     */
    public void peek(Consumer<PackstreamBuf> consumer) {
        this.delegate.markReaderIndex();

        try {
            consumer.accept(this);
        } finally {
            this.delegate.resetReaderIndex();
        }
    }

    /**
     * Performs a given set of operations without advancing the reader index of this buffer.
     *
     * @param func a function.
     * @param <R>  a return type.
     * @return an arbitrary return value.
     */
    public <R> R peek(Function<PackstreamBuf, R> func) {
        this.delegate.markReaderIndex();

        try {
            return func.apply(this);
        } finally {
            this.delegate.resetReaderIndex();
        }
    }

    /**
     * Skips a value of a given type within this buffer.
     * @param type a desired value type.
     * @return a reference to this buffer.
     * @throws PackstreamReaderException when an unexpected value type is encountered or a limit exceeded.
     */
    public PackstreamBuf skip(Type type) throws PackstreamReaderException {
        switch (type) {
            case NONE -> this.skipNull();
            case BOOLEAN -> this.skipBoolean();
            case BYTES -> this.skipBytes(-1);
            case FLOAT -> this.skipFloat();
            case INT -> this.skipInt();
            case LIST -> this.skipList(-1);
            case MAP -> this.skipMap(-1);
            case STRING -> this.skipString(-1);
            case STRUCT -> this.skipStruct();
            default -> throw new IllegalArgumentException("Unsupported data type: " + type);
        }

        return this;
    }

    /**
     * Skips a single value within this buffer.
     * @return a reference to this buffer.
     * @throws PackstreamReaderException when an unexpected value type is encountered or a limit exceeded.
     */
    public PackstreamBuf skip() throws PackstreamReaderException {
        return this.skip(this.peekType());
    }

    /**
     * Retrieves the next type marker within this buffer.
     *
     * @return a marker byte.
     */
    public short readMarkerByte() {
        return this.delegate.readUnsignedByte();
    }

    /**
     * Retrieves the next type marker within this buffer.
     *
     * @return a value type.
     */
    public TypeMarker readMarker() {
        return TypeMarker.byEncoded(this.readMarkerByte());
    }

    /**
     * Retrieves the next type marker within this buffer and ensures that it matches the given desired marker.
     *
     * @param expected a desired marker.
     * @return the retrieved marker byte.
     * @throws UnexpectedTypeMarkerException when the type markers mismatch.
     */
    public long readExpectedMarker(TypeMarker expected) throws UnexpectedTypeMarkerException {
        var mb = this.readMarkerByte();
        var actual = TypeMarker.byEncoded(mb);

        if (actual != expected) {
            throw new UnexpectedTypeMarkerException(expected, actual);
        }

        if (expected.getLengthPrefix() == LengthPrefix.NIBBLE) {
            return decodeLengthNibble(mb);
        }

        return mb;
    }

    public long readLengthPrefixMarker(Type type, long limit) throws UnexpectedTypeException, LimitExceededException {
        var mb = this.readMarkerByte();
        var marker = TypeMarker.byEncoded(mb);

        if (marker.getType() != type) {
            throw new UnexpectedTypeException(type, marker);
        }

        long length;
        if (marker.isNibbleMarker()) {
            length = decodeLengthNibble(mb);
        } else {
            length = marker.getLengthPrefix().readFrom(this.delegate);
        }

        if (limit > 0 && length > limit) {
            throw new LimitExceededException(limit, length);
        }

        return length;
    }

    public long readLengthPrefixMarker(Type type) throws UnexpectedTypeException, LimitExceededException {
        return this.readLengthPrefixMarker(type, -1);
    }

    /**
     * Retrieves the next type marker within this buffer.
     * <p>
     * Note: This method does not increment the reader index thus leaving the marker and length prefix intact for future read operations.
     *
     * @return a marker byte.
     */
    public short peekMarkerByte() {
        return this.delegate.getUnsignedByte(this.delegate.readerIndex());
    }

    /**
     * Retrieves the next type marker within this buffer.
     * <p>
     * Note: This method does not increment the reader index thus leaving the marker and length prefix intact for future read calls.
     *
     * @return a value type.
     */
    public TypeMarker peekMarker() {
        return TypeMarker.byEncoded(this.peekMarkerByte());
    }

    /**
     * Retrieves the next type marker within this buffer and returns its associated value type.
     *
     * @return a value type.
     * @see #peekMarker() for a detailed description.
     */
    public Type peekType() {
        return this.peekMarker().getType();
    }

    /**
     * Writes a type marker to this buffer.
     *
     * @param marker a marker byte.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeMarkerByte(int marker) {
        this.delegate.writeByte(marker);
        return this;
    }

    /**
     * Writes a standalone type marker to this buffer.
     *
     * @param type a value type.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeMarker(TypeMarker type) {
        if (type.hasLengthPrefix()) {
            throw new IllegalArgumentException("Type " + type + " requires a length");
        }

        this.writeMarkerByte(type.getValue());
        return this;
    }

    /**
     * Writes a type marker along with its respective length prefix to this buffer.
     *
     * @param marker a value type.
     * @param length a structure length or collection size.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeMarker(TypeMarker marker, long length) {
        if (!marker.hasLengthPrefix()) {
            throw new IllegalArgumentException("Type " + marker + " does not provide length");
        }
        if (!marker.canEncodeLength(length)) {
            throw new IllegalArgumentException("Type " + marker + " cannot store value of length " + length
                    + " (limit is " + marker.getLengthPrefix().getMaxValue() + ")");
        }

        requireEncodableLength(marker, length);

        // some types encode the length of their structure within the type itself using the least significant 4 bits
        if (marker.isNibbleMarker()) {
            this.writeMarkerByte(encodeLengthNibble(marker, (int) length));
            return this;
        }

        this.writeMarkerByte(marker.getValue());
        marker.getLengthPrefix().writeTo(this.delegate, length);
        return this;
    }

    /**
     * Chooses a suitable type marker and writes it to this buffer along with its respective length prefix.
     * <p>
     * This method evaluates markers in the given order. As such, callers should ensure that markers are ordered based on the size of their prefix in order to
     * produce the smallest possible encoded size.
     *
     * @param markers a collection of suitable markers.
     * @param length  arbitrary length.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeMarker(Collection<TypeMarker> markers, long length) {
        if (markers.isEmpty()) {
            throw new IllegalArgumentException("Marker collection cannot be empty");
        }

        for (var marker : markers) {
            if (!marker.hasLengthPrefix() || !marker.canEncodeLength(length)) {
                continue;
            }

            return this.writeMarker(marker, length);
        }

        var maxLengths = markers.stream()
                .filter(TypeMarker::hasLengthPrefix)
                .map(marker -> marker.getLengthPrefix().getMaxValue() + " (" + marker.name() + ")")
                .collect(Collectors.joining(", "));

        throw new IllegalArgumentException("Length " + length + " exceeds supported maximum lengths of " + maxLengths);
    }

    /**
     * Writes a given Java value to this buffer.
     * <p>
     * Note: This method does not support struct payloads. As such, writing structs requires explicit invocations of
     * {@link #writeStruct(StructRegistry, Object)} with a suitable registry implementation.
     *
     * @param payload a payload.
     * @return a reference to this buffer.
     */
    @SuppressWarnings("unchecked")
    public PackstreamBuf writeValue(Object payload) {
        if (payload == null) {
            return this.writeNull();
        }

        if (payload instanceof byte[] b) {
            return this.writeBytes(Unpooled.wrappedBuffer(b));
        }
        if (payload instanceof ByteBuffer b) {
            return this.writeBytes(Unpooled.wrappedBuffer(b));
        }
        if (payload instanceof ByteBuf b) {
            return this.writeBytes(b);
        }

        if (payload instanceof Boolean b) {
            return this.writeBoolean(b);
        }

        if (payload instanceof Float f) {
            return this.writeFloat((double) f);
        }

        if (payload instanceof Byte b) {
            return this.writeInt((long) b);
        }
        if (payload instanceof Short s) {
            return this.writeInt((long) s);
        }
        if (payload instanceof Integer i) {
            return this.writeInt((long) i);
        }
        if (payload instanceof Long l) {
            return this.writeInt(l);
        }

        if (payload instanceof List l) {
            return this.writeList(l);
        }
        if (payload instanceof Map m) {
            return this.writeMap(m);
        }

        if (payload instanceof String s) {
            return this.writeString(s);
        }

        throw new IllegalArgumentException(
                "Unsupported value of type " + payload.getClass().getName() + ": " + payload);
    }

    /**
     * Retrieves a null value from this buffer.
     *
     * @return a reference to this buffer.
     * @throws UnexpectedTypeMarkerException when a non-null marker is encountered.
     */
    public PackstreamBuf readNull() throws UnexpectedTypeMarkerException {
        this.readExpectedMarker(NULL);
        return this;
    }

    /**
     * Skips a null value within this buffer.
     * @return a reference to this buffer.
     * @throws UnexpectedTypeException when a non-null marker is encountered.
     */
    public PackstreamBuf skipNull() throws UnexpectedTypeException {
        return this.readNull();
    }

    /**
     * Writes a null value to this buffer.
     *
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeNull() {
        return this.writeMarker(NULL);
    }

    /**
     * Retrieves a boolean value from this buffer.
     *
     * @return a boolean payload.
     * @throws UnexpectedTypeException when a non-boolean marker is encountered.
     */
    public boolean readBoolean() throws UnexpectedTypeException {
        var marker = this.readMarker();

        if (marker.getType() != BOOLEAN) {
            throw new UnexpectedTypeException(BOOLEAN, marker);
        }

        return marker == TRUE;
    }

    /**
     * Skips a boolean value within this buffer.
     * @return a reference to this buffer.
     * @throws UnexpectedTypeException when a non-boolean marker is encountered.
     */
    public PackstreamBuf skipBoolean() throws UnexpectedTypeException {
        var marker = this.readMarker();

        if (marker.getType() != BOOLEAN) {
            throw new UnexpectedTypeException(BOOLEAN, marker);
        }

        return this;
    }

    /**
     * Writes a boolean value to this buffer.
     *
     * @param payload a boolean value.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeBoolean(boolean payload) {
        if (payload) {
            return this.writeMarker(TRUE);
        }

        return this.writeMarker(FALSE);
    }

    /**
     * Reads an integer of arbitrary size from this buffer.
     *
     * @return an integer value.
     * @throws UnexpectedTypeException when non-integer marker is encountered.
     */
    public long readInt() throws UnexpectedTypeException {
        var m = this.readMarkerByte();
        var marker = TypeMarker.byEncoded(m);

        if (marker.getType() != INT) {
            throw new UnexpectedTypeException(INT, marker);
        }

        switch (marker) {
            case TINY_INT:
                return (byte) m;
            case INT8:
                return this.delegate.readByte();
            case INT16:
                return this.delegate.readShort();
            case INT32:
                return this.delegate.readInt();
            default:
                return this.delegate.readLong();
        }
    }

    /**
     * Skips an integer value of arbitrary size within this buffer.
     * @return a reference to this buffer.
     * @throws UnexpectedTypeException when non-integer marker is encountered.
     */
    public PackstreamBuf skipInt() throws UnexpectedTypeException {
        var m = this.readMarkerByte();
        var marker = TypeMarker.byEncoded(m);

        if (marker.getType() != INT) {
            throw new UnexpectedTypeException(INT, marker);
        }

        switch (marker) {
            case TINY_INT -> {}
            case INT8 -> this.delegate.skipBytes(1);
            case INT16 -> this.delegate.skipBytes(2);
            case INT32 -> this.delegate.skipBytes(4);
            default -> this.delegate.skipBytes(8);
        }

        return this;
    }

    /**
     * Writes an integer value of arbitrary size to this buffer.
     *
     * @param value an integer value.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeInt(long value) {
        if (value >= TINY_INT_MIN && value <= TINY_INT_MAX) {
            return this.writeTinyInt((byte) value);
        }

        // INT8 is primarily used to extend the range of negative values as the remaining values not covered by
        // TINY_INT are used to mark all other types.
        if (value >= INT8_MIN && value <= TINY_INT_MIN) {
            return this.writeInt8((byte) value);
        }

        if (value >= INT16_MIN && value <= INT16_MAX) {
            return this.writeInt16((short) value);
        }

        if (value >= INT32_MIN && value <= INT32_MAX) {
            return this.writeInt32((int) value);
        }

        return this.writeInt64(value);
    }

    /**
     * Reads the raw value of a tiny integer from this buffer.
     *
     * @return an integer value.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectation.
     */
    public byte readTinyInt() throws UnexpectedTypeMarkerException {
        return (byte) this.readExpectedMarker(TINY_INT);
    }

    /**
     * Writes a raw tiny integer value to this buffer.
     *
     * @param value an integer value.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeTinyInt(byte value) {
        if (value < TINY_INT_MIN) {
            throw new IllegalArgumentException("Value is out of type bounds: " + value);
        }

        return this.writeMarkerByte(encodeLengthNibble(TINY_INT, value));
    }

    /**
     * Reads the raw value of an 8-bit integer from this buffer.
     *
     * @return an integer value.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectation.
     */
    public byte readInt8() throws UnexpectedTypeMarkerException {
        this.readExpectedMarker(INT8);
        return this.delegate.readByte();
    }

    /**
     * Writes a raw 8-bit integer value to this buffer.
     *
     * @param value an integer value.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeInt8(byte value) {
        this.writeMarker(INT8);
        this.delegate.writeByte(value);
        return this;
    }

    /**
     * Reads a 16-bit integer value from this buffer.
     *
     * @return an integer value.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectation.
     */
    public short readInt16() throws UnexpectedTypeMarkerException {
        this.readExpectedMarker(INT16);
        return this.delegate.readShort();
    }

    /**
     * Writes a 16-bit integer to this buffer.
     *
     * @param value an integer value.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeInt16(short value) {
        this.writeMarker(INT16);
        this.delegate.writeShort(value);
        return this;
    }

    /**
     * Reads a 32-bit integer from this buffer.
     *
     * @return an integer value.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectation.
     */
    public int readInt32() throws UnexpectedTypeMarkerException {
        this.readExpectedMarker(INT32);
        return this.delegate.readInt();
    }

    /**
     * Writes a 32-bit integer to this buffer.
     *
     * @param value an integer value.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeInt32(int value) {
        this.writeMarker(INT32);
        this.delegate.writeInt(value);
        return this;
    }

    /**
     * Reads a 64-bit integer from this buffer.
     *
     * @return an integer value.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectation.
     */
    public long readInt64() throws UnexpectedTypeMarkerException {
        this.readExpectedMarker(INT64);
        return this.delegate.readLong();
    }

    /**
     * Writes a 64-bit integer to this buffer.
     *
     * @param value an integer value.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeInt64(long value) {
        this.writeMarker(INT64);
        this.delegate.writeLong(value);
        return this;
    }

    /**
     * Retrieves a 64-bit float value from this buffer.
     *
     * @return a float payload.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectation.
     */
    public double readFloat() throws UnexpectedTypeMarkerException {
        this.readExpectedMarker(FLOAT64);
        return this.delegate.readDouble();
    }

    /**
     * Skips a 64-bit float value within this buffer.
     * @return a reference to this buffer.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectation.
     */
    public PackstreamBuf skipFloat() throws UnexpectedTypeMarkerException {
        this.readExpectedMarker(FLOAT64);
        this.delegate.skipBytes(8);
        return this;
    }

    /**
     * Writes a 64-bit float value to this buffer.
     *
     * @param payload a float payload.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeFloat(double payload) {
        this.writeMarker(FLOAT64);
        this.delegate.writeDouble(payload);
        return this;
    }

    /**
     * Retrieves a byte array of arbitrary length from this buffer.
     * <p>
     * Note: The returned payload is a slice of this buffer and will thus be released along with this buffer unless explicitly retained. It is generally
     * recommended to consume the contents where possible.
     *
     * @param limit identifies the maximum array length.
     * @return a slice consisting of the array payload.
     * @throws PackstreamReaderException when the limit is exceeded or the value type does not meet the expectation.
     */
    public ByteBuf readBytes(long limit) throws PackstreamReaderException {
        var marker = this.readMarker();
        if (marker.getType() != Type.BYTES) {
            throw new UnexpectedTypeException(Type.BYTES, marker);
        }

        var length = marker.getLengthPrefix().readFrom(this.delegate);
        if (limit > 0 && length > limit) {
            throw new LimitExceededException(limit, length);
        }

        return this.readBytesValue(length);
    }

    /**
     * Skips a byte array of arbitrary length within this buffer.
     *
     * @param limit identifies the maximum array length.
     * @return a reference to this buffer.
     * @throws PackstreamReaderException when the limit is exceeded or the value does not meet the expectation.
     */
    public PackstreamBuf skipBytes(long limit) throws PackstreamReaderException {
        var marker = this.readMarker();
        if (marker.getType() != Type.BYTES) {
            throw new UnexpectedTypeException(Type.BYTES, marker);
        }

        var length = marker.getLengthPrefix().readFrom(this.delegate);
        if (limit > 0 && length > limit) {
            throw new LimitExceededException(limit, length);
        }

        while (length > 0) {
            var part = (int) length;

            this.delegate.skipBytes(part);
            length -= part;
        }

        return this;
    }

    /**
     * Writes a byte array of arbitrary length to this buffer.
     *
     * @param bytes a slice consisting of the desired payload.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeBytes(ByteBuf bytes) {
        this.writeMarker(BYTES_TYPES, bytes.readableBytes());
        this.delegate.writeBytes(bytes);
        return this;
    }

    /**
     * Reads a byte array of arbitrary length from this buffer.
     * <p>
     * Note: The returned payload is a slice of this buffer and will thus be released along with this buffer unless explicitly retained. It is generally
     * recommended to consume the contents where possible.
     *
     * @return a slice consisting of the desired payload.
     * @throws PackstreamReaderException when the limit is exceeded or the value type does not meet the expectation.
     */
    public ByteBuf readBytes() throws PackstreamReaderException {
        return this.readBytes(-1);
    }

    /**
     * Retrieves a slice of arbitrary length from this buffer.
     * <p>
     * Note: The returned payload is a slice of this buffer and will thus be released along with this buffer unless explicitly retained. It is generally
     * recommended to consume the contents where possible.
     *
     * @param length the desired length.
     * @return a slice consisting of the payload.
     * @throws LimitExceededException when the slice size exceeds the maximum permitted value.
     */
    private ByteBuf readBytesValue(long length) throws LimitExceededException {
        // JVM does not support arrays beyond 2^31-1 elements
        if (length > Integer.MAX_VALUE) {
            throw new LimitExceededException(Integer.MAX_VALUE, length);
        }

        return this.delegate.readSlice((int) length);
    }

    /**
     * Retrieves an 8-bit prefixed slice of arbitrary length from this buffer.
     *
     * @return a slice consisting of the payload.
     * @throws LimitExceededException        when the slice size exceeds the maximum permitted value.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectation.
     */
    public ByteBuf readBytes8() throws LimitExceededException, UnexpectedTypeMarkerException {
        this.readExpectedMarker(BYTES8);

        var length = LengthPrefix.UINT8.readFrom(this.delegate);

        return this.readBytesValue(length);
    }

    /**
     * Writes an 8-bit prefixed slice of arbitrary length from this buffer.
     *
     * @param bytes an arbitrary payload.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the given slice exceeds the limit of this value type.
     */
    public PackstreamBuf writeBytes8(ByteBuf bytes) {
        this.writeMarker(BYTES8, bytes.readableBytes());
        this.delegate.writeBytes(bytes);
        return this;
    }

    /**
     * Retrieves a 16-bit prefixed slice of arbitrary length from this buffer.
     *
     * @return a slice consisting of the payload.
     * @throws LimitExceededException        when the slice size exceeds the maximum permitted value.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectation.
     */
    public ByteBuf readBytes16() throws LimitExceededException, UnexpectedTypeMarkerException {
        this.readExpectedMarker(BYTES16);

        var length = LengthPrefix.UINT16.readFrom(this.delegate);

        return this.readBytesValue(length);
    }

    /**
     * Writes a 16-bit prefixed slice of arbitrary length to this buffer.
     *
     * @param bytes an arbitrary payload.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the given slice exceeds the limit of this value type.
     */
    public PackstreamBuf writeBytes16(ByteBuf bytes) {
        this.writeMarker(BYTES16, bytes.readableBytes());
        this.delegate.writeBytes(bytes);
        return this;
    }

    /**
     * Retrieves a 32-bit prefixed slice of arbitrary length from this buffer.
     *
     * @return a slice consisting of the payload.
     * @throws LimitExceededException        when the slice size exceeds the maximum permitted value.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectation.
     */
    public ByteBuf readBytes32() throws LimitExceededException, UnexpectedTypeMarkerException {
        this.readExpectedMarker(BYTES32);

        var length = LengthPrefix.UINT32.readFrom(this.delegate);

        return this.readBytesValue(length);
    }

    /**
     * Writes a 32-bit prefixed slice of arbitrary length to this buffer.
     *
     * @param bytes an arbitrary payload.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the given slice exceeds the limit of this value type.
     */
    public PackstreamBuf writeBytes32(ByteBuf bytes) {
        this.writeMarker(BYTES32, bytes.readableBytes());
        this.delegate.writeBytes(bytes);
        return this;
    }

    /**
     * Retrieves a string value of arbitrary length from this buffer.
     *
     * @param limit an upper size limit for values (no limit if negative).
     * @return a string payload.
     * @throws LimitExceededException  when the string size exceeds the maximum permitted value.
     * @throws UnexpectedTypeException when the value type does not meet the expectation.
     */
    public String readString(long limit) throws UnexpectedTypeException, LimitExceededException {
        var length = this.readLengthPrefixMarker(STRING, limit);
        return this.readStringValue(length);
    }

    /**
     * Skips a string value of arbitrary length within this buffer.
     * @param limit an upper size limit for values (no limit if negative).
     * @return a reference to this buffer.
     * @throws UnexpectedTypeException when the string size exceeded the maximum permitted value.
     * @throws LimitExceededException when the value type does not meet the expectation.
     */
    public PackstreamBuf skipString(long limit) throws UnexpectedTypeException, LimitExceededException {
        var length = this.readLengthPrefixMarker(STRING, limit);
        while (length > 0) {
            var part = (int) length;

            this.delegate.skipBytes(part);
            length -= part;
        }
        return this;
    }

    /**
     * Retrieves a string value of arbitrary length from this buffer.
     *
     * @return a string payload.
     * @throws LimitExceededException  when the string size exceeds the maximum permitted value.
     * @throws UnexpectedTypeException when the value type does not meet the expectation.
     */
    public String readString() throws PackstreamReaderException {
        return this.readString(-1);
    }

    /**
     * Retrieves and decodes a string payload of a given known length.
     *
     * @param length the desired string length.
     * @return a string payload.
     * @throws LimitExceededException when the encoded string size exceeds the maximum permitted value.
     */
    private String readStringValue(long length) throws LimitExceededException {
        var heap = this.readBytesValue(length);
        return heap.toString(STRING_CHARSET);
    }

    /**
     * Writes a string value of arbitrary length to this buffer.
     *
     * @param payload a string value.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeString(String payload) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        var heap = payload.getBytes(STRING_CHARSET);

        this.writeMarker(STRING_TYPES, heap.length);
        this.delegate.writeBytes(heap);
        return this;
    }

    /**
     * Retrieves a 4-bit prefixed string value of arbitrary length from this buffer.
     *
     * @return a string value.
     * @throws LimitExceededException        when the encoded string size exceeds the maximum permitted value.
     * @throws UnexpectedTypeMarkerException when the value type does not match the expectation.
     */
    public String readTinyString() throws LimitExceededException, UnexpectedTypeMarkerException {
        var length = this.readExpectedMarker(TINY_STRING);
        return this.readStringValue(length);
    }

    /**
     * Writes a 4-bit prefixed string value of arbitrary length to this buffer.
     *
     * @param payload a string value.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the value exceeds the bounds of this type.
     */
    public PackstreamBuf writeTinyString(String payload) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeTinyString(payload.getBytes(STRING_CHARSET));
    }

    /**
     * Writes a 4-bit prefixed string value of arbitrary length to this buffer.
     *
     * @param payload an encoded string value.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the value exceeds the bounds of this type.
     */
    private PackstreamBuf writeTinyString(byte[] payload) {
        this.writeMarker(TINY_STRING, payload.length);
        this.delegate.writeBytes(payload);
        return this;
    }

    /**
     * Reads an 8-bit prefixed string value of arbitrary length from this buffer.
     *
     * @return a string value.
     * @throws LimitExceededException        when the value exceeds the bounds of this type.
     * @throws UnexpectedTypeMarkerException when the value type does not match the expectation.
     */
    public String readString8() throws LimitExceededException, UnexpectedTypeMarkerException {
        this.readExpectedMarker(STRING8);

        var length = LengthPrefix.UINT8.readFrom(this.delegate);

        return this.readStringValue(length);
    }

    /**
     * Writes an 8-bit prefixed string value of arbitrary length to this buffer.
     *
     * @param payload a string value.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the value exceeds the bounds of this type.
     */
    public PackstreamBuf writeString8(String payload) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeString8(payload.getBytes(STRING_CHARSET));
    }

    /**
     * Writes an 8-bit prefixed string value of arbitrary length to this buffer.
     *
     * @param payload an encoded string value.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the value exceeds the bounds of this type.
     */
    private PackstreamBuf writeString8(byte[] payload) {
        this.writeMarker(STRING8, payload.length);
        this.delegate.writeBytes(payload);
        return this;
    }

    /**
     * Reads a 16-bit prefixed string value of arbitrary length from this buffer.
     *
     * @return a string value.
     * @throws LimitExceededException        when the value exceeds the bounds of this type.
     * @throws UnexpectedTypeMarkerException when the value type does not match the expectations.
     */
    public String readString16() throws LimitExceededException, UnexpectedTypeMarkerException {
        this.readExpectedMarker(STRING16);

        var length = LengthPrefix.UINT16.readFrom(this.delegate);

        return this.readStringValue(length);
    }

    /**
     * Writes a 16-bit prefixed string value of arbitrary length to this buffer.
     *
     * @param payload a string value.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the value exceeds the bounds of this type.
     */
    public PackstreamBuf writeString16(String payload) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeString16(payload.getBytes(STRING_CHARSET));
    }

    /**
     * Writes a 16-bit prefixed string value of arbitrary length to this buffer.
     *
     * @param payload an encoded string value.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the value exceeds the bounds of this type.
     */
    private PackstreamBuf writeString16(byte[] payload) {
        this.writeMarker(STRING16, payload.length);
        this.delegate.writeBytes(payload);
        return this;
    }

    /**
     * Reads a 32-bit prefixed string value of arbitrary length from this buffer.
     *
     * @return a string value.
     * @throws LimitExceededException        when the value exceeds the bounds of this type.
     * @throws UnexpectedTypeMarkerException when the value type does not match the expectations.
     */
    public String readString32() throws LimitExceededException, UnexpectedTypeMarkerException {
        this.readExpectedMarker(STRING32);

        var length = LengthPrefix.UINT32.readFrom(this.delegate);

        return this.readStringValue(length);
    }

    /**
     * Writes a 32-bit prefixed string value of arbitrary length to this buffer.
     *
     * @param payload an encoded string value.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the value exceeds the bounds of this type.
     */
    public PackstreamBuf writeString32(String payload) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeString32(payload.getBytes(STRING_CHARSET));
    }

    /**
     * Writes a 32-bit prefixed string value of arbitrary length to this buffer.
     *
     * @param payload an encoded string value.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the value exceeds the bounds of this type.
     */
    private PackstreamBuf writeString32(byte[] payload) {
        this.writeMarker(STRING32, payload.length);
        this.delegate.writeBytes(payload);
        return this;
    }

    private <O> List<O> readListValue(long length, Reader<O> reader) throws PackstreamReaderException {
        // Collection API does not permit more than 2^31-1 items in a given list
        if (length > Integer.MAX_VALUE) {
            throw new LimitExceededException(Integer.MAX_VALUE, length);
        }

        var elements = new ArrayList<O>();
        for (var i = 0; i < length; ++i) {
            elements.add(reader.read(this));
        }
        return elements;
    }

    /**
     * Retrieves a list of arbitrary element types from this buffer.
     *
     * @param limit  a maximum number of elements within the list (none if the value is negative).
     * @param reader a value reader.
     * @param <O>    a value type.
     * @return a list of arbitrary content.
     * @throws LimitExceededException        when the given limit of elements or another value limit is exceeded.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectations.
     * @throws PackstreamReaderException     when a reader fails to decode a list element.
     */
    public <O> List<O> readList(long limit, Reader<O> reader) throws PackstreamReaderException {
        var length = this.readLengthPrefixMarker(LIST, limit);
        return this.readListValue(length, reader);
    }

    /**
     * Skips a list of arbitrary element types within this buffer.
     * @param limit a maximum number of elements within the list (none if the value is negative).
     * @return a list of arbitrary content.
     * @throws LimitExceededException when the given limit of elements or another value limit is exceeded.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectations.
     * @throws PackstreamReaderException when a reader fails to decode a list element.
     */
    public PackstreamBuf skipList(long limit) throws PackstreamReaderException {
        var length = this.readLengthPrefixMarker(LIST, limit);
        for (var i = 0L; i < length; ++i) {
            this.skip();
        }
        return this;
    }

    /**
     * Retrieves a list of arbitrary element types from this buffer.
     *
     * @param reader a value reader.
     * @param <O>    a value type.
     * @return a list of arbitrary content.
     * @throws LimitExceededException        when the given limit of elements or another value limit is exceeded.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectations.
     * @throws PackstreamReaderException     when a reader fails to decode a list element.
     */
    public <O> List<O> readList(Reader<O> reader) throws PackstreamReaderException {
        return this.readList(-1, reader);
    }

    /**
     * Writes the contents of a list to this buffer.
     *
     * @param payload a list of arbitrary elements.
     * @param writer  a writer implementation.
     * @param <I>     an element type.
     * @return a reference to this buffer.
     */
    private <I> PackstreamBuf writeListValue(Collection<I> payload, Writer<I> writer) {
        payload.forEach(element -> writer.write(this, element));
        return this;
    }

    /**
     * Writes a list header for an arbitrarily sized list to this buffer.
     *
     * @param size a list size.
     * @return a reference to this buffer.
     */
    public PackstreamBuf writeListHeader(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("size cannot be negative");
        }

        return this.writeMarker(LIST_TYPES, size);
    }

    /**
     * Writes a list of arbitrary size and element type to this buffer.
     *
     * @param payload a list of arbitrary elements.
     * @param writer  a writer implementation.
     * @param <I>     an element type.
     * @return a reference to this buffer.
     */
    public <I> PackstreamBuf writeList(Collection<I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeListHeader(payload.size()).writeListValue(payload, writer);
    }

    /**
     * Writes a list of arbitrary size and element type to this buffer.
     *
     * @param payload a list of arbitrary elements.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when one of the elements cannot be encoded.
     */
    public PackstreamBuf writeList(List<Object> payload) {
        return this.writeList(payload, PackstreamBuf::writeValue);
    }

    public <O> List<O> readTinyList(Reader<O> reader) throws PackstreamReaderException {
        var length = this.readExpectedMarker(TINY_LIST);
        return this.readListValue(length, reader);
    }

    public <I> PackstreamBuf writeTinyList(Collection<I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeMarker(TINY_LIST, payload.size()).writeListValue(payload, writer);
    }

    public <O> List<O> readList8(Reader<O> reader) throws PackstreamReaderException {
        this.readExpectedMarker(LIST8);

        var length = LengthPrefix.UINT8.readFrom(this.delegate);

        return this.readListValue(length, reader);
    }

    public <I> PackstreamBuf writeList8(Collection<I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeMarker(LIST8, payload.size()).writeListValue(payload, writer);
    }

    public <O> List<O> readList16(Reader<O> reader) throws PackstreamReaderException {
        this.readExpectedMarker(LIST16);

        var length = LengthPrefix.UINT16.readFrom(this.delegate);

        return this.readListValue(length, reader);
    }

    public <I> PackstreamBuf writeList16(Collection<I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeMarker(LIST16, payload.size()).writeListValue(payload, writer);
    }

    public <O> List<O> readList32(Reader<O> reader) throws PackstreamReaderException {
        this.readExpectedMarker(LIST32);

        var length = LengthPrefix.UINT32.readFrom(this.delegate);

        return this.readListValue(length, reader);
    }

    public <I> PackstreamBuf writeList32(Collection<I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeMarker(LIST32, payload.size()).writeListValue(payload, writer);
    }

    /**
     * Retrieves the contents of a map of a given length.
     *
     * @param length the amount of elements within this map.
     * @param reader a reader implementation.
     * @param <O>    a value type.
     * @return a map value.
     * @throws LimitExceededException        when the given limit of elements or another value limit is exceeded.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectations.
     * @throws PackstreamReaderException     when a reader fails to decode a list element.
     */
    private <O> Map<String, O> readMapValue(long length, Reader<O> reader) throws PackstreamReaderException {
        // Collection API does not permit more than 2^31-1 items in a given map
        if (length > Integer.MAX_VALUE) {
            throw new LimitExceededException(Integer.MAX_VALUE, length);
        }

        var elements = new HashMap<String, O>();
        for (var i = 0; i < length; ++i) {
            var key = this.readString();
            if (elements.containsKey(key)) {
                throw new PackstreamReaderException("Duplicate map key: \"" + key + "\"");
            }

            var value = reader.read(this);

            elements.put(key, value);
        }
        return elements;
    }

    /**
     * Writes the contents of a map to this buffer.
     *
     * @param payload a map value.
     * @param writer  a writer implementation.
     * @param <I>     an element type.
     * @return a reference to this buffer.
     */
    private <I> PackstreamBuf writeMapValue(Map<String, I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        payload.forEach((key, value) -> {
            this.writeString(key);
            writer.write(this, value);
        });
        return this;
    }

    /**
     * Writes a map header with a given amount of elements.
     *
     * @param length an amount of elements within the map.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the given collection size exceeds the maximum permitted value.
     */
    public PackstreamBuf writeMapHeader(long length) {
        // Collection API does not permit more than 2^31-1 items in a given map
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("length exceeds limit of " + Integer.MAX_VALUE);
        }

        return this.writeMarker(MAP_TYPES, length);
    }

    /**
     * Retrieves a map with a given maximum size from this buffer.
     *
     * @param limit  a maximum number of elements within the map (none if negative).
     * @param reader a reader implementation.
     * @param <O>    a value type.
     * @return a map value.
     * @throws LimitExceededException    when the given limit of elements or another value limit is exceeded.
     * @throws UnexpectedTypeException   when the value type does not meet the expectations.
     * @throws PackstreamReaderException when a reader fails to decode a map entry.
     */
    public <O> Map<String, O> readMap(long limit, Reader<O> reader) throws PackstreamReaderException {
        var length = this.readLengthPrefixMarker(MAP, limit);
        if (limit > 0 && length > limit) {
            throw new LimitExceededException(limit, length);
        }

        return this.readMapValue(length, reader);
    }

    public PackstreamBuf skipMap(long limit) throws PackstreamReaderException {
        var length = this.readLengthPrefixMarker(MAP, limit);
        if (limit > 0 && length > limit) {
            throw new LimitExceededException(limit, length);
        }

        for (var i = 0; i < length; ++i) {
            this.skipString(-1);
            this.skip();
        }

        return this;
    }

    /**
     * Retrieves a map consisting of arbitrary elements.
     *
     * @param reader a reader implementation.
     * @param <O>    an element type.
     * @return a map value.
     * @throws LimitExceededException        when the given limit of elements or another value limit is exceeded.
     * @throws UnexpectedTypeMarkerException when the value type does not meet the expectations.
     * @throws PackstreamReaderException     when a reader fails to decode a list element.
     */
    public <O> Map<String, O> readMap(Reader<O> reader) throws PackstreamReaderException {
        return this.readMap(-1, reader);
    }

    /**
     * Writes a map of a given type to this buffer.
     *
     * @param payload a map value.
     * @param writer  a writer implementation.
     * @param <I>     an element type.
     * @return a reference to this buffer.
     */
    public <I> PackstreamBuf writeMap(Map<String, I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeMapHeader(payload.size()).writeMapValue(payload, writer);
    }

    /**
     * Writes a map of a given type to this buffer.
     *
     * @param payload a map value.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when an unsupported value is encountered within the map.
     */
    public PackstreamBuf writeMap(Map<String, Object> payload) {
        return this.writeMap(payload, PackstreamBuf::writeValue);
    }

    public <O> Map<String, O> readTinyMap(Reader<O> reader) throws PackstreamReaderException {
        var length = this.readExpectedMarker(TINY_MAP);
        return this.readMapValue(length, reader);
    }

    public <I> PackstreamBuf writeTinyMap(Map<String, I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeMarker(TINY_MAP, payload.size()).writeMapValue(payload, writer);
    }

    public <O> Map<String, O> readMap8(Reader<O> reader) throws PackstreamReaderException {
        this.readExpectedMarker(MAP8);

        var length = LengthPrefix.UINT8.readFrom(this.delegate);
        return this.readMapValue(length, reader);
    }

    public <I> PackstreamBuf writeMap8(Map<String, I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeMarker(MAP8, payload.size()).writeMapValue(payload, writer);
    }

    public <O> Map<String, O> readMap16(Reader<O> reader) throws PackstreamReaderException {
        this.readExpectedMarker(MAP16);

        var length = LengthPrefix.UINT16.readFrom(this.delegate);
        return this.readMapValue(length, reader);
    }

    public <I> PackstreamBuf writeMap16(Map<String, I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeMarker(MAP16, payload.size()).writeMapValue(payload, writer);
    }

    public <O> Map<String, O> readMap32(Reader<O> reader) throws PackstreamReaderException {
        this.readExpectedMarker(MAP32);

        var length = LengthPrefix.UINT32.readFrom(this.delegate);
        return this.readMapValue(length, reader);
    }

    public <I> PackstreamBuf writeMap32(Map<String, I> payload, Writer<I> writer) {
        if (payload == null) {
            throw new NullPointerException("payload cannot be null");
        }

        return this.writeMarker(MAP32, payload.size()).writeMapValue(payload, writer);
    }

    /**
     * Retrieves the next struct header without advancing the reader index.
     *
     * @return a struct header.
     * @throws LimitExceededException  when the struct header size has been exceeded.
     * @throws UnexpectedTypeException when an unexpected value type is encountered.
     */
    public StructHeader peekStructHeader() throws LimitExceededException, UnexpectedTypeException {
        var originalMarkerLocation = this.delegate.readerIndex();

        try {
            this.delegate.markReaderIndex();
            return this.readStructHeader();
        } finally {
            this.delegate.resetReaderIndex().readerIndex(originalMarkerLocation);
        }
    }

    /**
     * Retrieves a struct header from this buffer.
     *
     * @return a struct header.
     * @throws LimitExceededException  when a value limit is exceeded.
     * @throws UnexpectedTypeException when the value type does not match the expectations.
     */
    public StructHeader readStructHeader() throws LimitExceededException, UnexpectedTypeException {
        var length = this.readLengthPrefixMarker(STRUCT);
        var tag = this.delegate.readUnsignedByte();

        return new StructHeader(length, tag);
    }

    /**
     * Writes a struct header to this buffer.
     *
     * @param header a header.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when the struct exceeds the valid bounds.
     */
    public PackstreamBuf writeStructHeader(StructHeader header) {
        this.writeMarker(STRUCT_TYPES, header.length());
        this.delegate.writeByte(header.tag());
        return this;
    }

    /**
     * Retrieves a struct from this buffer.
     *
     * @param registry a struct type registry.
     * @param <CTX> a context type.
     * @param <O>      a struct POJO base type.
     * @return a struct POJO.
     * @throws LimitExceededException    when a value limit is exceeded.
     * @throws UnexpectedTypeException   when the value type does not match the expectations.
     * @throws PackstreamReaderException when the structure cannot be decoded.
     */
    public <CTX, O> O readStruct(CTX ctx, StructRegistry<CTX, O> registry) throws PackstreamReaderException {
        var header = this.readStructHeader();

        return registry.getReader(header)
                .orElseThrow(() -> new UnexpectedStructException(header))
                .read(ctx, this, header);
    }

    /**
     * Skips a single struct value within this buffer.
     * @return a reference to this buffer.
     * @throws PackstreamReaderException when a value limit is exceeded or the value type does not match expectations.
     */
    public PackstreamBuf skipStruct() throws PackstreamReaderException {
        var header = this.readStructHeader();

        for (var i = 0; i < header.length(); ++i) {
            this.skip();
        }

        return this;
    }

    /**
     * Writes a struct to this buffer.
     *
     * @param registry a struct type registry.
     * @param payload  a struct POJO payload.
     * @param <CTX>    a context type.
     * @param <S>      a base struct POJO type.
     * @param <P>      a payload POJO type.
     * @return a reference to this buffer.
     * @throws IllegalArgumentException when an unknown struct type or an invalid value is passed.
     */
    public <CTX, S, P extends S> PackstreamBuf writeStruct(CTX ctx, StructRegistry<CTX, S> registry, P payload) {
        if (registry == null) {
            throw new NullPointerException("registry cannot be null");
        }

        var writer = registry.getWriter(payload)
                .orElseThrow(() -> new IllegalArgumentException("Illegal struct: " + payload));

        var tag = writer.getTag(payload);
        var length = writer.getLength(payload);

        var header = new StructHeader(length, tag);
        this.writeStructHeader(header);

        writer.write(ctx, this, payload);
        return this;
    }

    @Override
    public int refCnt() {
        return this.delegate.refCnt();
    }

    @Override
    public ReferenceCounted retain() {
        this.delegate.retain();
        return this;
    }

    @Override
    public ReferenceCounted retain(int i) {
        this.delegate.retain(i);
        return this;
    }

    @Override
    public ReferenceCounted touch() {
        this.delegate.touch();
        return this;
    }

    @Override
    public ReferenceCounted touch(Object o) {
        this.delegate.touch(o);
        return this;
    }

    @Override
    public boolean release() {
        return this.delegate.release();
    }

    @Override
    public boolean release(int i) {
        return this.delegate.release(i);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PackstreamBuf that = (PackstreamBuf) o;
        return this.delegate.equals(that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.delegate);
    }
}
