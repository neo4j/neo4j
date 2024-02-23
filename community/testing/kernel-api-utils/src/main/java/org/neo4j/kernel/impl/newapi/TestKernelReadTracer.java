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
package org.neo4j.kernel.impl.newapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.neo4j.internal.kernel.api.KernelReadTracer;

public class TestKernelReadTracer implements KernelReadTracer {
    static final TraceEvent ON_ALL_NODES_SCAN = new TraceEvent(TraceEventKind.AllNodesScan);

    private final List<TraceEvent> traceEvents;

    public TestKernelReadTracer() {
        traceEvents = new ArrayList<>();
    }

    @Override
    public void onNode(long nodeReference) {
        traceEvents.add(nodeEvent(nodeReference));
    }

    @Override
    public void onAllNodesScan() {
        traceEvents.add(ON_ALL_NODES_SCAN);
    }

    @Override
    public void onLabelScan(int label) {
        traceEvents.add(labelScanEvent(label));
    }

    @Override
    public void onRelationshipTypeScan(int type) {
        traceEvents.add(relationshipTypeScanEvent(type));
    }

    @Override
    public void onIndexSeek() {
        traceEvents.add(indexSeekEvent());
    }

    @Override
    public void onRelationship(long relationshipReference) {
        traceEvents.add(relationshipEvent(relationshipReference));
    }

    @Override
    public void onProperty(int propertyKey) {
        traceEvents.add(propertyEvent(propertyKey));
    }

    @Override
    public void onHasLabel(int label) {
        traceEvents.add(hasLabelEvent(label));
    }

    @Override
    public void onHasLabel() {
        traceEvents.add(hasLabelEvent());
    }

    @Override
    public void dbHit() {
        throw new UnsupportedOperationException();
    }

    void assertEvents(TraceEvent... expected) {
        assertEvents(Arrays.asList(expected));
    }

    void assertEvents(List<TraceEvent> expected) {
        assertThat(traceEvents).isEqualTo(expected);
        clear();
    }

    void clear() {
        traceEvents.clear();
    }

    enum TraceEventKind {
        Node,
        AllNodesScan,
        LabelScan,
        RelationshipTypeScan,
        IndexSeek,
        Relationship,
        Property,
        HasLabel
    }

    public static class TraceEvent {
        final TraceEventKind kind;
        final long hash;

        private TraceEvent(TraceEventKind kind) {
            this(kind, 1);
        }

        private TraceEvent(TraceEventKind kind, long hash) {
            this.kind = kind;
            this.hash = hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TraceEvent that = (TraceEvent) o;
            return hash == that.hash && kind == that.kind;
        }

        @Override
        public int hashCode() {
            return Objects.hash(kind, hash);
        }

        @Override
        public String toString() {
            return String.format("%s[%d]", kind, hash);
        }
    }

    public static TraceEvent nodeEvent(long nodeReference) {
        return new TraceEvent(TraceEventKind.Node, nodeReference);
    }

    public static TraceEvent labelScanEvent(int label) {
        return new TraceEvent(TraceEventKind.LabelScan, label);
    }

    public static TraceEvent relationshipTypeScanEvent(int type) {
        return new TraceEvent(TraceEventKind.RelationshipTypeScan, type);
    }

    public static TraceEvent indexSeekEvent() {
        return new TraceEvent(TraceEventKind.IndexSeek, 1);
    }

    public static TraceEvent relationshipEvent(long relationshipReference) {
        return new TraceEvent(TraceEventKind.Relationship, relationshipReference);
    }

    public static TraceEvent propertyEvent(int propertyKey) {
        return new TraceEvent(TraceEventKind.Property, propertyKey);
    }

    public static TraceEvent hasLabelEvent(int label) {
        return new TraceEvent(TraceEventKind.HasLabel, label);
    }

    public static TraceEvent hasLabelEvent() {
        return new TraceEvent(TraceEventKind.HasLabel);
    }
}
