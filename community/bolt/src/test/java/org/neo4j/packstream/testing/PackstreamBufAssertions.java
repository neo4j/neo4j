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
package org.neo4j.packstream.testing;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.testing.assertions.ByteBufAssertions;
import org.neo4j.packstream.error.reader.LimitExceededException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public class PackstreamBufAssertions extends AbstractAssert<PackstreamBufAssertions, PackstreamBuf> {

    private final StructRegistry<Object, Value> structRegistry;

    PackstreamBufAssertions(PackstreamBuf buf, StructRegistry<Object, Value> structRegistry) {
        super(buf, PackstreamBufAssertions.class);
        this.structRegistry = structRegistry;
    }

    PackstreamBufAssertions(PackstreamBuf buf) {
        this(buf, PackstreamTestValueReader.DEFAULT_STRUCT_REGISTRY);
    }

    public static PackstreamBufAssertions assertThat(PackstreamBuf value) {
        return new PackstreamBufAssertions(value);
    }

    public static PackstreamBufAssertions assertThat(ByteBuf value) {
        return assertThat(PackstreamBuf.wrap(value));
    }

    public static PackstreamBufAssertions assertThat(
            PackstreamBuf value, StructRegistry<Object, Value> structRegistry) {
        return new PackstreamBufAssertions(value, structRegistry);
    }

    public static PackstreamBufAssertions assertThat(ByteBuf value, StructRegistry<Object, Value> structRegistry) {
        return assertThat(PackstreamBuf.wrap(value), structRegistry);
    }

    public static InstanceOfAssertFactory<PackstreamBuf, PackstreamBufAssertions> packstreamBuf() {
        return new InstanceOfAssertFactory<>(PackstreamBuf.class, PackstreamBufAssertions::assertThat);
    }

    public static InstanceOfAssertFactory<ByteBuf, PackstreamBufAssertions> wrap() {
        return new InstanceOfAssertFactory<>(ByteBuf.class, PackstreamBufAssertions::assertThat);
    }

    private void fail(UnexpectedTypeException ex) {
        failWithActualExpectedAndMessage(
                ex.getActual(),
                ex.getExpected(),
                "Expected Packstream value of type <%s> but got <%s>",
                ex.getExpected(),
                ex.getActual());
    }

    private void fail(LimitExceededException ex) {
        failWithActualExpectedAndMessage(
                ex.getActual(),
                ex.getLimit(),
                "Expected Packstream value to not exceed limit of <%d> but got <%d>: %s",
                ex.getLimit(),
                ex.getLimit(),
                ex.getMessage());
    }

    private void fail(PackstreamReaderException ex) {
        throw new AssertionError("Malformed Packstream structure", ex);
    }

    public ByteBufAssertions asBuffer() {
        return ByteBufAssertions.assertThat(this.actual.getTarget());
    }

    public PackstreamBufAssertions containsLengthPrefixMarker(Type type, long expected) {
        long actual;
        try {
            actual = this.actual.readLengthPrefixMarker(type);
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        }

        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected %s value with length <%d> but got <%d>", type, expected, actual);
        }

        return this;
    }

    public PackstreamBufAssertions containsInt(long expected) {
        long actual;
        try {
            actual = this.actual.readInt();
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        }

        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected integer value <%d> but got <%d>", expected, actual);
        }

        return this;
    }

    public PackstreamBufAssertions containsAInt() {
        try {
            this.actual.readInt();
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        }
        return this;
    }

    public PackstreamBufAssertions containsString(Consumer<String> assertions) {
        String actual;
        try {
            actual = this.actual.readString();
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        } catch (PackstreamReaderException ex) {
            fail(ex);
            return this;
        }

        assertions.accept(actual);
        return this;
    }

    public PackstreamBufAssertions containsString(String expected) {
        String actual;
        try {
            actual = this.actual.readString();
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        } catch (PackstreamReaderException ex) {
            fail(ex);
            return this;
        }

        if (!expected.equals(actual)) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected string value <\"%s\"> but got <\"%s\">", expected, actual);
        }

        return this;
    }

    public PackstreamBufAssertions containsStruct(Consumer<StructHeader> assertions) {
        StructHeader header;
        try {
            header = this.actual.readStructHeader();
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        }

        assertions.accept(header);
        return this;
    }

    public PackstreamBufAssertions containsStruct(int tag) {
        StructHeader header;
        try {
            header = this.actual.readStructHeader();
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        }

        if (header.tag() != (short) tag) {
            failWithActualExpectedAndMessage(
                    header.tag(),
                    (short) tag,
                    "Expected message with tag <0x%02X> but got <0x%02X>",
                    tag,
                    header.tag());
        }

        return this;
    }

    public PackstreamBufAssertions containsStruct(int tag, long length) {
        StructHeader header;
        try {
            header = this.actual.readStructHeader();
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        }

        if (header.tag() != (short) tag) {
            failWithActualExpectedAndMessage(
                    header.tag(),
                    (short) tag,
                    "Expected message with tag <0x%02X> but got <0x%02X>",
                    tag,
                    header.tag());
        }
        if (header.length() != length) {
            failWithActualExpectedAndMessage(
                    header.length(), length, "Expected message with length <%d> but got <%d>", length, header.length());
        }

        return this;
    }

    public PackstreamBufAssertions containsListHeader(long expected) {
        long actual;
        try {
            actual = this.actual.readLengthPrefixMarker(Type.LIST);
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        }

        if (actual != expected) {
            failWithActualExpectedAndMessage(
                    actual, expected, "Expected list with length <%d> but got <%d>", expected, actual);
        }

        return this;
    }

    public PackstreamBufAssertions containsList(Consumer<List<Object>> assertions) {
        List<Object> value;
        try {
            value = this.actual.readList(
                    ignore -> PackstreamTestValueReader.readValue(this.actual, this.structRegistry));
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        } catch (PackstreamReaderException ex) {
            fail(ex);
            return this;
        }

        assertions.accept(value);
        return this;
    }

    public PackstreamBufAssertions containsListValue(Consumer<ListValue> assertions) {
        ListValue value;
        try {
            value = PackstreamTestValueReader.readListValue(this.actual, this.structRegistry);
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        } catch (PackstreamReaderException ex) {
            fail(ex);
            return this;
        }

        assertions.accept(value);
        return this;
    }

    public PackstreamBufAssertions containsMap(Consumer<Map<String, Object>> assertions) {
        Map<String, Object> value;
        try {
            value = this.actual.readMap(
                    ignore -> PackstreamTestValueReader.readValue(this.actual, this.structRegistry));
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        } catch (PackstreamReaderException ex) {
            fail(ex);
            return this;
        }

        assertions.accept(value);
        return this;
    }

    public PackstreamBufAssertions containsMapValue(Consumer<MapValue> assertions) {
        MapValue value;
        try {
            value = PackstreamTestValueReader.readMapValue(this.actual, this.structRegistry);
        } catch (UnexpectedTypeException ex) {
            fail(ex);
            return this;
        } catch (LimitExceededException ex) {
            fail(ex);
            return this;
        } catch (PackstreamReaderException ex) {
            fail(ex);
            return this;
        }

        assertions.accept(value);
        return this;
    }
}
