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
package org.neo4j.internal.recordstorage;

import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.kernel.impl.store.record.Record.isNull;

import java.io.PrintStream;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;

/**
 * A generic utility for visiting relationships for a node. The relationships are visited as they are stored, i.e. is sparse then the single
 * relationship chain and if dense the groups followed by each their relationship chains.
 * It is written such that it's generic in how it reads records and a visitor gets callbacks as the records are traversed.
 */
public class RelationshipChainVisitor {
    private final LongFunction<NodeRecord> nodeStore;
    private final LongFunction<RelationshipRecord> relationshipStore;
    private final LongFunction<RelationshipGroupRecord> groupStore;

    public RelationshipChainVisitor(
            LongFunction<NodeRecord> nodeStore,
            LongFunction<RelationshipRecord> relationshipStore,
            LongFunction<RelationshipGroupRecord> groupStore) {
        this.nodeStore = nodeStore;
        this.relationshipStore = relationshipStore;
        this.groupStore = groupStore;
    }

    public RelationshipChainVisitor(NeoStores neoStores) {
        this(
                recordLoader(neoStores.getNodeStore()),
                recordLoader(neoStores.getRelationshipStore()),
                recordLoader(neoStores.getRelationshipGroupStore()));
    }

    /**
     * Convenience method for quickly printing a node's relationship chain w/ relationship groups if it's dense.
     * @param neoStores store to read from.
     * @param out where to print.
     * @param nodeId the node id to print.
     */
    public static void printRelationshipChain(NeoStores neoStores, PrintStream out, long nodeId) {
        new RelationshipChainVisitor(neoStores).visit(nodeId, printer(out));
    }

    private static <R extends AbstractBaseRecord> LongFunction<R> recordLoader(RecordStore<R> store) {
        return id -> {
            try (var cursor = store.openPageCursorForReading(id, CursorContext.NULL_CONTEXT)) {
                return store.getRecordByCursor(
                        id, store.newRecord(), RecordLoad.NORMAL, cursor, EmptyMemoryTracker.INSTANCE);
            }
        };
    }

    public void visit(long nodeId, Visitor visitor) {
        NodeRecord node = nodeStore.apply(nodeId);
        visitor.node(node);
        if (node.isDense()) {
            visitGroups(nodeId, node.getNextRel(), visitor);
        } else {
            visitRelationships(nodeId, node.getNextRel(), visitor);
        }
    }

    private void visitGroups(long nodeId, long groupId, Visitor visitor) {
        long prev = NULL_REFERENCE.longValue();
        while (!isNull(groupId)) {
            RelationshipGroupRecord group = groupStore.apply(groupId);
            group.setPrev(prev);
            visitor.group(group);
            visitGroupChain(nodeId, group.getFirstOut(), RelationshipDirection.OUTGOING, visitor);
            visitGroupChain(nodeId, group.getFirstIn(), RelationshipDirection.INCOMING, visitor);
            visitGroupChain(nodeId, group.getFirstLoop(), RelationshipDirection.LOOP, visitor);
            prev = groupId;
            groupId = group.getNext();
        }
    }

    private void visitGroupChain(
            long nodeId, long firstRelationshipId, RelationshipDirection direction, Visitor visitor) {
        if (!isNull(firstRelationshipId)) {
            visitor.groupChain(direction);
            visitRelationships(nodeId, firstRelationshipId, visitor);
        }
    }

    private void visitRelationships(long nodeId, long relationshipId, Visitor visitor) {
        while (!isNull(relationshipId)) {
            RelationshipRecord relationship = relationshipStore.apply(relationshipId);
            visitor.relationship(relationship);
            relationshipId = relationship.getNextRel(nodeId);
        }
    }

    public interface Visitor {
        default void node(NodeRecord node) {}

        default void group(RelationshipGroupRecord group) {}

        default void groupChain(RelationshipDirection direction) {}

        default void relationship(RelationshipRecord relationship) {}
    }

    public static Visitor printer(PrintStream out) {
        return new Visitor() {
            @Override
            public void node(NodeRecord node) {
                out.println("Relationship chains for " + node + ":");
            }

            @Override
            public void group(RelationshipGroupRecord group) {
                out.println("  " + group + ":");
            }

            @Override
            public void groupChain(RelationshipDirection direction) {
                out.println("    " + direction + ":");
            }

            @Override
            public void relationship(RelationshipRecord relationship) {
                out.println("      " + relationship);
            }
        };
    }

    public static Visitor relationshipCollector(Consumer<RelationshipRecord> collector) {
        return new Visitor() {
            @Override
            public void relationship(RelationshipRecord relationship) {
                collector.accept(relationship);
            }
        };
    }
}
