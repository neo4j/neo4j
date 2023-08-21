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
package org.neo4j.bolt.protocol.io.pipeline;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.io.writer.StructWriter;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

public class WriterPipeline {
    private final Connection connection;

    private final ReentrantLock lock = new ReentrantLock();
    private volatile ChainElement head;
    private volatile ChainElement tail;

    public WriterPipeline(Connection connection) {
        this.connection = connection;
    }

    public PipelineContext forBuffer(PackstreamBuf buf) {
        return new Context(buf);
    }

    public WriterPipeline addLast(StructWriter writer) {
        this.lock.lock();

        try {
            var newElement = new ChainElement(this.tail, writer);

            if (this.head == null) {
                this.head = newElement;
            }
            if (this.tail != null) {
                this.tail.next = newElement;
                newElement.prev = this.tail;
            }
            this.tail = newElement;
        } finally {
            this.lock.unlock();
        }

        return this;
    }

    public WriterPipeline addFirst(StructWriter writer) {
        this.lock.lock();

        try {
            var newElement = new ChainElement(null, this.head, writer);

            if (this.tail == null) {
                this.tail = newElement;
            }
            if (this.head != null) {
                this.head.prev = newElement;
                newElement.next = this.head;
            }
            this.head = newElement;
        } finally {
            this.lock.unlock();
        }

        return this;
    }

    public WriterPipeline remove(StructWriter writer) {
        this.lock.lock();

        try {
            var current = this.head;
            while (current != null) {
                if (current.writer == writer) {
                    var prev = current.prev;
                    var next = current.next;

                    if (prev == null) {
                        this.head = next;
                    } else {
                        prev.next = next;
                    }
                    if (next == null) {
                        this.tail = prev;
                    } else {
                        next.prev = prev;
                    }

                    break;
                }

                current = current.next;
            }
        } finally {
            this.lock.unlock();
        }

        return this;
    }

    public StructWriter removeFirst() {
        this.lock.lock();

        ChainElement first;
        try {
            first = this.head;
            if (first == null) {
                return null;
            }

            this.head = first.next;
            if (first.next == null) {
                this.tail = null;
            }
        } finally {
            this.lock.unlock();
        }

        return first.writer;
    }

    public StructWriter removeLast() {
        this.lock.lock();

        ChainElement last;
        try {
            last = this.tail;
            if (tail == null) {
                return null;
            }

            this.tail = last.prev;
            if (last.prev == null) {
                this.head = null;
            }
        } finally {
            this.lock.unlock();
        }

        return last.writer;
    }

    private static class ChainElement {
        private volatile ChainElement next;
        private volatile ChainElement prev;
        private final StructWriter writer;

        public ChainElement(ChainElement prev, ChainElement next, StructWriter writer) {
            this.prev = prev;
            this.next = next;
            this.writer = writer;
        }

        public ChainElement(ChainElement prev, StructWriter writer) {
            this(prev, null, writer);
        }
    }

    private class Context implements WriterContext {
        private final PackstreamBuf buf;
        private final PipelineAnyValueWriter valueWriter;

        private ChainElement current;

        public Context(PackstreamBuf buf) {
            this.buf = buf;
            this.valueWriter = new PipelineAnyValueWriter(buf, this);
        }

        @Override
        public Connection connection() {
            return connection;
        }

        @Override
        public PackstreamBuf buffer() {
            return this.buf;
        }

        private void fire(String eventName, Consumer<StructWriter> consumer) {
            var prev = this.current;
            var next = prev.next;
            if (next == null) {
                throw new IllegalStateException("Event " + eventName + " has reached end of pipeline");
            }

            this.current = next;
            try {
                consumer.accept(next.writer);
            } finally {
                this.current = prev;
            }
        }

        @Override
        public void writeValue(AnyValue value) {
            value.writeTo(this.valueWriter);
        }

        private void write(Consumer<StructWriter> consumer) {
            var origin = this.current;
            var next = head;

            this.current = next;
            try {
                consumer.accept(next.writer);
            } finally {
                this.current = origin;
            }
        }

        @Override
        public void writePoint(CoordinateReferenceSystem crs, double[] coords) {
            this.write(writer -> writer.writePoint(this, crs, coords));
        }

        @Override
        public void writeDuration(long months, long days, long seconds, int nanos) {
            this.write(writer -> writer.writeDuration(this, months, days, seconds, nanos));
        }

        @Override
        public void writeDate(LocalDate localDate) {
            this.write(writer -> writer.writeDate(this, localDate));
        }

        @Override
        public void writeTime(LocalTime localTime) {
            this.write(writer -> writer.writeLocalTime(this, localTime));
        }

        @Override
        public void writeTime(OffsetTime offsetTime) {
            this.write(writer -> writer.writeTime(this, offsetTime));
        }

        @Override
        public void writeLocalDateTime(LocalDateTime localDateTime) {
            this.write(writer -> writer.writeLocalDateTime(this, localDateTime));
        }

        @Override
        public void writeDateTime(OffsetDateTime offsetDateTime) {
            this.write(writer -> writer.writeDateTime(this, offsetDateTime));
        }

        @Override
        public void writeDateTime(ZonedDateTime zonedDateTime) {
            this.write(writer -> writer.writeDateTime(this, zonedDateTime));
        }

        @Override
        public void writeNode(String elementId, long nodeId, TextArray labels, MapValue properties, boolean isDeleted) {
            this.write(writer -> writer.writeNode(this, elementId, nodeId, labels, properties, isDeleted));
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
                boolean isDeleted) {
            this.write(writer -> writer.writeRelationship(
                    this,
                    elementId,
                    relId,
                    startNodeElementId,
                    startNodeId,
                    endNodeElementId,
                    endNodeId,
                    type,
                    properties,
                    isDeleted));
        }

        @Override
        public void writeUnboundRelationship(String elementId, long relId, String type, MapValue properties) {
            this.write(writer -> writer.writeUnboundRelationship(this, elementId, relId, type, properties));
        }

        @Override
        public void writePath(NodeValue[] nodes, RelationshipValue[] relationships) {
            this.write(writer -> writer.writePath(this, nodes, relationships));
        }

        @Override
        public void firePoint(CoordinateReferenceSystem crs, double[] coords) {
            this.fire("point", writer -> writer.writePoint(this, crs, coords));
        }

        @Override
        public void fireDuration(long months, long days, long seconds, int nanos) {
            this.fire("duration", writer -> writer.writeDuration(this, months, days, seconds, nanos));
        }

        @Override
        public void fireDate(LocalDate localDate) {
            this.fire("date", writer -> writer.writeDate(this, localDate));
        }

        @Override
        public void fireLocalTime(LocalTime localTime) {
            this.fire("local_time", writer -> writer.writeLocalTime(this, localTime));
        }

        @Override
        public void fireTime(OffsetTime offsetTime) {
            this.fire("time", writer -> writer.writeTime(this, offsetTime));
        }

        @Override
        public void fireLocalDateTime(LocalDateTime localDateTime) {
            this.fire("local_date_time", writer -> writer.writeLocalDateTime(this, localDateTime));
        }

        @Override
        public void fireDateTime(OffsetDateTime offsetDateTime) {
            this.fire("offset_date_time", writer -> writer.writeDateTime(this, offsetDateTime));
        }

        @Override
        public void fireDateTime(ZonedDateTime zonedDateTime) {
            this.fire("date_time", writer -> writer.writeDateTime(this, zonedDateTime));
        }

        @Override
        public void fireNode(String elementId, long nodeId, TextArray labels, MapValue properties, boolean isDeleted) {
            this.fire("node", writer -> writer.writeNode(this, elementId, nodeId, labels, properties, isDeleted));
        }

        @Override
        public void fireRelationship(
                String elementId,
                long relId,
                String startNodeElementId,
                long startNodeId,
                String endNodeElementId,
                long endNodeId,
                TextValue type,
                MapValue properties,
                boolean isDeleted) {
            this.fire(
                    "relationship",
                    writer -> writer.writeRelationship(
                            this,
                            elementId,
                            relId,
                            startNodeElementId,
                            startNodeId,
                            endNodeElementId,
                            endNodeId,
                            type,
                            properties,
                            isDeleted));
        }

        @Override
        public void fireUnboundRelationship(String elementId, long relId, String type, MapValue properties) {
            this.fire(
                    "unbound_relationship",
                    writer -> writer.writeUnboundRelationship(this, elementId, relId, type, properties));
        }

        @Override
        public void firePath(NodeValue[] nodes, RelationshipValue[] relationships) {
            this.fire("path", writer -> writer.writePath(this, nodes, relationships));
        }
    }
}
