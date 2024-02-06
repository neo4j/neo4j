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
package org.neo4j.values.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

/**
 * Pretty printer for AnyValues.
 * <p>
 * Used to format AnyValue as a json-like string, as following:
 * <ul>
 * <li>nodes: <code>(id=42 :LABEL {prop1: ["a", 13]})</code></li>
 * <li>edges: <code>-[id=42 :TYPE {prop1: ["a", 13]}]-</code></li>
 * <li>paths: <code>(id=1 :L)-[id=42 :T {k: "v"}]->(id=2)-...</code></li>
 * <li>points are serialized to geojson</li>
 * <li>maps: <code>{foo: 42, bar: "baz"}</code></li>
 * <li>lists and arrays: <code>["aa", "bb", "cc"]</code></li>
 * <li>Numbers: <code>2.7182818285</code></li>
 * <li>Strings: <code>"this is a string"</code></li>
 * </ul>
 */
public class PrettyPrinter implements AnyValueWriter<RuntimeException> {
    private final Deque<Writer> stack = new ArrayDeque<>();
    private final String quoteMark;
    private final StringBuilder builder;
    private final EntityMode entityMode;
    private final Counter counter;
    private final int maxLength;

    public PrettyPrinter() {
        this(EntityMode.FULL);
    }

    public PrettyPrinter(EntityMode entityMode) {
        this("\"", entityMode, Integer.MAX_VALUE);
    }

    public PrettyPrinter(String quoteMark, EntityMode entityMode) {
        this(quoteMark, entityMode, Integer.MAX_VALUE);
    }

    public PrettyPrinter(String quoteMark, EntityMode entityMode, int maxLength) {
        this.quoteMark = quoteMark;
        this.entityMode = entityMode;
        this.maxLength = maxLength;
        this.counter = new Counter(maxLength);
        this.builder = new StringBuilder(64);
        this.stack.push(new ValueWriter(builder, quoteMark, counter));
    }

    public void reset() {
        stack.clear();
        stack.push(new ValueWriter(builder, quoteMark, counter));
        builder.setLength(0);
        counter.setCount(maxLength);
    }

    @Override
    public EntityMode entityMode() {
        return entityMode;
    }

    @Override
    public void writeNodeReference(long nodeId) {
        append("(id=");
        append(String.valueOf(nodeId));
        append(")");
    }

    @Override
    public void writeNode(String elementId, long nodeId, TextArray labels, MapValue properties, boolean isDeleted)
            throws RuntimeException {
        append("(elementId=");
        append(elementId);
        String sep = " ";
        for (int i = 0; i < labels.length(); i++) {
            append(sep);
            append(":");
            append(labels.stringValue(i));
            sep = "";
        }
        if (properties.size() > 0) {
            append(" ");
            properties.writeTo(this);
        }

        append(")");
    }

    @Override
    public void writeRelationshipReference(long relId) {
        append("-[id=");
        append(String.valueOf(relId));
        append("]-");
    }

    @Override
    public void writeRelationship(
            String elementId,
            long relId,
            String startNodeElementId,
            long startNodeId,
            String endNodeElementId,
            long endNodeId,
            TextValue type,
            MapValue properties,
            boolean isDeleted)
            throws RuntimeException {
        append("-[elementId=");
        append(elementId);
        append(" :");
        append(type.stringValue());
        if (properties.size() > 0) {
            append(" ");
            properties.writeTo(this);
        }
        append("]-");
    }

    @Override
    public void beginMap(int size) {
        assert !stack.isEmpty();
        stack.peek().nest();
        stack.push(new MapWriter(builder, quoteMark, counter));
    }

    @Override
    public void endMap() {
        assert !stack.isEmpty();
        stack.pop().done();
        if (!stack.isEmpty()) {
            stack.peek().next();
        }
    }

    @Override
    public void beginList(int size) {
        assert !stack.isEmpty();
        stack.peek().nest();
        stack.push(new ListWriter(builder, quoteMark, counter));
    }

    @Override
    public void endList() {
        assert !stack.isEmpty();
        stack.pop().done();
        if (!stack.isEmpty()) {
            stack.peek().next();
        }
    }

    @Override
    public void writePathReference(long[] nodes, long[] relationships) {
        if (nodes.length == 0) {
            return;
        }
        // Path guarantees that nodes.length = edges.length = 1
        writeNodeReference(nodes[0]);
        for (int i = 0; i < relationships.length; i++) {
            writeRelationshipReference(relationships[i]);
            append(">");
            writeNodeReference(nodes[i + 1]);
        }
    }

    @Override
    public void writePathReference(VirtualNodeValue[] nodes, VirtualRelationshipValue[] relationships)
            throws RuntimeException {
        if (nodes.length == 0) {
            return;
        }
        // Path guarantees that nodes.length = edges.length = 1
        writeNodeReference(nodes[0].id());
        for (int i = 0; i < relationships.length; i++) {
            writeRelationshipReference(relationships[i].id());
            append(">");
            writeNodeReference(nodes[i + 1].id());
        }
    }

    @Override
    public void writePathReference(List<VirtualNodeValue> nodes, List<VirtualRelationshipValue> relationships)
            throws RuntimeException {
        if (nodes.size() == 0) {
            return;
        }
        // Path guarantees that nodes.length = edges.length = 1
        writeNodeReference(nodes.get(0).id());
        for (int i = 0; i < relationships.size(); i++) {
            writeRelationshipReference(relationships.size());
            append(">");
            writeNodeReference(nodes.get(i + 1).id());
        }
    }

    @Override
    public void writePath(NodeValue[] nodes, RelationshipValue[] relationships) {
        if (nodes.length == 0) {
            return;
        }
        // Path guarantees that nodes.length = edges.length = 1
        nodes[0].writeTo(this);
        for (int i = 0; i < relationships.length; i++) {
            relationships[i].writeTo(this);
            append(">");
            nodes[i + 1].writeTo(this);
        }
    }

    @Override
    public void writePoint(CoordinateReferenceSystem crs, double[] coordinate) throws RuntimeException {
        append("{geometry: {type: \"Point\", coordinates: ");
        append(Arrays.toString(coordinate));
        append(", crs: {type: link, properties: {href: \"");
        append(crs.getHref());
        append("\", code: ");
        append(Integer.toString(crs.getCode()));
        append("}}}}");
    }

    @Override
    public void writeDuration(long months, long days, long seconds, int nanos) throws RuntimeException {
        append("{duration: {months: ");
        append(Long.toString(months));
        append(", days: ");
        append(Long.toString(days));
        append(", seconds: ");
        append(Long.toString(seconds));
        append(", nanos: ");
        append(Long.toString(nanos));
        append("}}");
    }

    @Override
    public void writeDate(LocalDate localDate) throws RuntimeException {
        append("{date: ");
        appendQuoted(localDate.toString());
        append("}");
    }

    @Override
    public void writeLocalTime(LocalTime localTime) throws RuntimeException {
        append("{localTime: ");
        appendQuoted(localTime.toString());
        append("}");
    }

    @Override
    public void writeTime(OffsetTime offsetTime) throws RuntimeException {
        append("{time: ");
        appendQuoted(offsetTime.toString());
        append("}");
    }

    @Override
    public void writeLocalDateTime(LocalDateTime localDateTime) throws RuntimeException {
        append("{localDateTime: ");
        appendQuoted(localDateTime.toString());
        append("}");
    }

    @Override
    public void writeDateTime(ZonedDateTime zonedDateTime) throws RuntimeException {
        append("{datetime: ");
        appendQuoted(zonedDateTime.toString());
        append("}");
    }

    @Override
    public void writeNull() {
        append("<null>");
    }

    @Override
    public void writeBoolean(boolean value) {
        append(Boolean.toString(value));
    }

    @Override
    public void writeInteger(byte value) {
        append(Byte.toString(value));
    }

    @Override
    public void writeInteger(short value) {
        append(Short.toString(value));
    }

    @Override
    public void writeInteger(int value) {
        append(Integer.toString(value));
    }

    @Override
    public void writeInteger(long value) {
        append(Long.toString(value));
    }

    @Override
    public void writeFloatingPoint(float value) {
        append(Float.toString(value));
    }

    @Override
    public void writeFloatingPoint(double value) {
        append(Double.toString(value));
    }

    @Override
    public void writeString(String value) {
        appendQuoted(value);
    }

    @Override
    public void writeString(char value) {
        writeString(Character.toString(value));
    }

    @Override
    public void beginArray(int size, ArrayType arrayType) {
        assert !stack.isEmpty();
        stack.peek().nest();
        stack.push(new ListWriter(builder, quoteMark, counter));
    }

    @Override
    public void endArray() {
        assert !stack.isEmpty();
        stack.pop().done();
        if (!stack.isEmpty()) {
            stack.peek().next();
        }
    }

    @Override
    public void writeByteArray(byte[] value) {
        String sep = "";
        append("[");
        for (byte b : value) {
            append(sep);
            append(Byte.toString(b));
            sep = ", ";
        }
        append("]");
    }

    public String value() {
        assert stack.size() == 1;
        stack.getLast().done();
        return builder.toString();
    }

    public void valueInto(StringBuilder target) {
        assert stack.size() == 1;
        stack.getLast().done();
        target.append(builder);
    }

    private void append(String value) {
        if (counter.isDone()) {
            return;
        }
        assert !stack.isEmpty();
        stack.peek().append(value);
    }

    private void appendQuoted(String value) {
        if (counter.isDone()) {
            return;
        }
        assert !stack.isEmpty();
        stack.peek().appendQuoted(value);
    }

    private interface Writer {
        void append(String value);

        void appendQuoted(String value);

        void next();

        void done();

        void nest();
    }

    private abstract static class BaseWriter implements Writer {
        protected final StringBuilder builder;
        protected Counter counter;
        private final String quoteMark;

        protected BaseWriter(StringBuilder builder, String quoteMark, Counter counter) {
            this.builder = builder;
            this.quoteMark = quoteMark;
            this.counter = counter;
        }

        @Override
        public void appendQuoted(String value) {
            write(quoteMark);
            this.append(value);
            write(quoteMark);
        }

        @Override
        public void next() {}

        @Override
        public void done() {}

        @Override
        public void nest() {}

        protected void write(String value) {
            int remaining = counter.remaining();
            if (remaining <= 0) {
                return;
            }
            if (remaining < value.length()) {
                builder.append(value, 0, remaining).append("...");
                counter.decrement(remaining);
            } else {
                builder.append(value);
                counter.decrement(value.length());
            }
        }
    }

    private static class ValueWriter extends BaseWriter {
        private ValueWriter(StringBuilder builder, String quoteMark, Counter counter) {
            super(builder, quoteMark, counter);
        }

        @Override
        public void append(String value) {
            write(value);
        }
    }

    private static class MapWriter extends BaseWriter {
        private boolean writeKey = true;
        private String sep = "";

        MapWriter(StringBuilder builder, String quoteMark, Counter counter) {
            super(builder, quoteMark, counter);
            write("{");
        }

        @Override
        public void append(String value) {
            if (writeKey) {
                write(sep);
                write(value);
                write(": ");
            } else {
                write(value);
            }
            writeKey = !writeKey;
            sep = ", ";
        }

        @Override
        public void appendQuoted(String value) {
            if (writeKey) {
                append(value);
            } else {
                super.appendQuoted(value);
            }
        }

        @Override
        public void next() {
            writeKey = true;
        }

        @Override
        public void done() {
            write("}");
        }
    }

    private class ListWriter extends BaseWriter {
        private String sep = "";

        ListWriter(StringBuilder builder, String quoteMark, Counter counter) {
            super(builder, quoteMark, counter);
            write("[");
        }

        @Override
        public void append(String value) {
            write(sep);
            write(value);
            sep = ", ";
        }

        @Override
        public void appendQuoted(String value) {
            write(sep);
            write(quoteMark);
            write(value);
            write(quoteMark);
            sep = ", ";
        }

        @Override
        public void done() {
            write("]");
        }

        @Override
        public void nest() {
            write(sep);
        }
    }

    private static final class Counter {
        private int count;

        Counter(int count) {
            this.count = count;
        }

        void setCount(int n) {
            count = n;
        }

        void decrement(int n) {
            count -= n;
        }

        int remaining() {
            return count;
        }

        public boolean isDone() {
            return count <= 0;
        }
    }
}
