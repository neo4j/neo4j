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
package org.neo4j.cypher.operations;

import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.virtual.VirtualValues.pathReference;

import java.util.ArrayList;
import java.util.function.Consumer;
import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.RelationshipVisitor;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * Builder for building paths from generated code, used when the length of the path is not known at compile time.
 * <p>
 * NOTE: this class is designed to be easy-to-use from generated code rather than from code typed by more or less
 * anthropic beings, so refactor with some care. The PathValueBuilder is not allowed to be reused, once the path is
 * built the instance should be discarded with.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class PathValueBuilder implements Consumer<RelationshipVisitor> {
    private boolean hasBeenBuilt = false;
    private final ArrayList<VirtualNodeValue> nodes = new ArrayList<>();
    private final ArrayList<VirtualRelationshipValue> rels = new ArrayList<>();
    private final DbAccess dbAccess;
    private final RelationshipScanCursor cursor;
    private boolean seenNoValue;

    public PathValueBuilder(DbAccess dbAccess, RelationshipScanCursor cursor) {
        this.dbAccess = dbAccess;
        this.cursor = cursor;
    }

    /**
     * Creates a PathValue or NO_VALUE if any NO_VALUES has been encountered.
     *
     * @return a PathValue or NO_VALUE if any NO_VALUES has been encountered
     */
    public AnyValue build() {
        if (hasBeenBuilt) {
            throw new IllegalStateException("This PathValueBuilder has already been used and reuse is not allowed.");
        }
        hasBeenBuilt = true;
        return seenNoValue ? NO_VALUE : pathReference(nodes, rels);
    }

    /**
     * Reads a node from the given offset of a (Trail group variable) List and adds it to the path
     *
     * @param value the list to get relationship from
     * @param offset in the list
     */
    @CalledFromGeneratedCode
    public void addNodeFromList(AnyValue value, int offset) {
        if (notNoValue(value)) {
            if (value instanceof ListValue listValue) {
                addNode(listValue.value(offset));
            } else {
                throw new CypherTypeException("Expected list but found: " + value);
            }
        }
    }

    /**
     * Adds node to the path
     *
     * @param value the node to add
     */
    @CalledFromGeneratedCode
    public void addNode(AnyValue value) {
        if (notNoValue(value)) {
            addNode((VirtualNodeValue) value);
        }
    }

    /**
     * Reads a relationship from the given offset of a (Trail group variable) List and adds it to the path
     *
     * @param value the list to get relationship from
     * @param offset in the list
     */
    @CalledFromGeneratedCode
    public void addRelationshipFromList(AnyValue value, int offset) {
        if (notNoValue(value)) {
            if (value instanceof ListValue listValue) {
                addRelationship(listValue.value(offset));
            } else {
                throw new CypherTypeException("Expected list but found: " + value);
            }
        }
    }

    @CalledFromGeneratedCode
    public void addRelationship(AnyValue value) {
        if (notNoValue(value)) {
            addRelationship((VirtualRelationshipValue) value);
        }
    }

    @CalledFromGeneratedCode
    public void addRelationship(VirtualRelationshipValue value) {
        rels.add(value);
    }

    /**
     * Adds node to the path
     *
     * @param nodeValue the node to add
     */
    @CalledFromGeneratedCode
    public void addNode(VirtualNodeValue nodeValue) {
        nodes.add(nodeValue);
    }

    /**
     * Adds incoming relationship to the path
     *
     * @param value the incoming relationship to add
     */
    @CalledFromGeneratedCode
    public void addIncoming(AnyValue value) {
        if (notNoValue(value)) {
            addIncoming((VirtualRelationshipValue) value);
        }
    }

    /**
     * Adds incoming relationship to the path
     *
     * @param relationship the incoming relationship to add
     */
    @CalledFromGeneratedCode
    public void addIncoming(VirtualRelationshipValue relationship) {
        nodes.add(VirtualValues.node(relationship.startNodeId(this)));
        rels.add(relationship);
    }

    /**
     * Adds outgoing relationship to the path
     *
     * @param value the outgoing relationship to add
     */
    @CalledFromGeneratedCode
    public void addOutgoing(AnyValue value) {
        if (notNoValue(value)) {
            addOutgoing((VirtualRelationshipValue) value);
        }
    }

    /**
     * Adds outgoing relationship to the path
     *
     * @param relationship the outgoing relationship to add
     */
    @CalledFromGeneratedCode
    public void addOutgoing(VirtualRelationshipValue relationship) {
        nodes.add(VirtualValues.node(relationship.endNodeId(this)));
        rels.add(relationship);
    }

    private void add(VirtualRelationshipValue relationship, VirtualNodeValue nextNode) {
        rels.add(relationship);
        nodes.add(nextNode);
    }

    /**
     * Adds undirected relationship to the path
     *
     * @param value the undirected relationship to add
     */
    @CalledFromGeneratedCode
    public void addUndirected(AnyValue value) {
        if (notNoValue(value)) {
            addUndirected((VirtualRelationshipValue) value);
        }
    }

    /**
     * Adds undirected relationship to the path
     *
     * @param relationship the undirected relationship to add
     */
    @CalledFromGeneratedCode
    public void addUndirected(VirtualRelationshipValue relationship) {
        var previous = nodes.get(nodes.size() - 1);
        long start = relationship.startNodeId(this);
        long end = relationship.endNodeId(this);
        if (previous.id() == start) {
            add(relationship, VirtualValues.node(end));
        } else {
            add(relationship, VirtualValues.node(start));
        }
    }

    /**
     * Adds multiple incoming relationships to the path
     *
     * @param value  the incoming relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleIncoming(AnyValue value, AnyValue target) {
        if (notNoValue(value) && notNoValue(target)) {
            addMultipleIncoming((ListValue) value, (VirtualNodeValue) target);
        }
    }

    /**
     * Adds multiple incoming relationships to the path
     *
     * @param relationships the incoming relationships to add
     * @param target        the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleIncoming(ListValue relationships, VirtualNodeValue target) {
        if (relationships.isEmpty()) {
            // nothing to do here
            return;
        }

        AnyValue last = relationships.last();

        relationships.forEach(r -> {
            if (r == last) {
                if (notNoValue(last)) {
                    nodes.add(target);
                    rels.add(((VirtualRelationshipValue) last));
                }
            } else {
                addIncoming(r);
            }
        });
    }

    /**
     * Adds multiple incoming relationships to the path
     *
     * @param value the incoming relationships to add
     */
    @CalledFromGeneratedCode
    public void addMultipleIncoming(AnyValue value) {
        if (notNoValue(value)) {
            addMultipleIncoming((ListValue) value);
        }
    }

    /**
     * Adds multiple incoming relationships to the path
     *
     * @param relationships the incoming relationships to add
     */
    @CalledFromGeneratedCode
    public void addMultipleIncoming(ListValue relationships) {
        if (relationships.isEmpty()) {
            // nothing to do here
            return;
        }

        relationships.forEach(this::addIncoming);
    }

    /**
     * Adds multiple outgoing relationships to the path
     *
     * @param value  the outgoing relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleOutgoing(AnyValue value, AnyValue target) {
        if (notNoValue(value) && notNoValue(target)) {
            addMultipleOutgoing((ListValue) value, (VirtualNodeValue) target);
        }
    }

    /**
     * Adds multiple outgoing relationships to the path
     *
     * @param relationships the outgoing relationships to add
     * @param target        the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleOutgoing(ListValue relationships, VirtualNodeValue target) {
        if (relationships.isEmpty()) {
            // nothing to do here
            return;
        }

        AnyValue last = relationships.last();

        relationships.forEach(r -> {
            if (r == last) {
                if (notNoValue(last)) {
                    rels.add(((VirtualRelationshipValue) last));
                    nodes.add(target);
                }
            } else {
                addOutgoing(r);
            }
        });
    }

    /**
     * Adds multiple outgoing relationships to the path
     *
     * @param value the outgoing relationships to add
     */
    @CalledFromGeneratedCode
    public void addMultipleOutgoing(AnyValue value) {
        if (notNoValue(value)) {
            addMultipleOutgoing((ListValue) value);
        }
    }

    /**
     * Adds multiple outgoing relationships to the path
     *
     * @param relationships the outgoing relationships to add
     */
    @CalledFromGeneratedCode
    public void addMultipleOutgoing(ListValue relationships) {
        if (relationships.isEmpty()) {
            // nothing to do here
            return;
        }

        relationships.forEach(this::addOutgoing);
    }

    /**
     * Adds multiple undirected relationships to the path
     *
     * @param value  the undirected relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleUndirected(AnyValue value, AnyValue target) {
        if (notNoValue(value) && notNoValue(target)) {
            addMultipleUndirected((ListValue) value, (VirtualNodeValue) target);
        }
    }

    /**
     * Adds multiple undirected relationships to the path
     *
     * @param relationships the undirected relationships to add
     * @param target        the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleUndirected(ListValue relationships, VirtualNodeValue target) {
        if (relationships.isEmpty()) {
            // nothing to add
            return;
        }

        AnyValue last = relationships.value(relationships.size() - 1);

        relationships.forEach(r -> {
            if (r == last) {
                if (notNoValue(last)) {
                    rels.add(((VirtualRelationshipValue) last));
                    nodes.add(target);
                }
            } else {
                addUndirected(r);
            }
        });
    }

    /**
     * Adds multiple undirected relationships to the path
     *
     * @param value the undirected relationships to add
     */
    @CalledFromGeneratedCode
    public void addMultipleUndirected(AnyValue value) {
        if (notNoValue(value)) {
            addMultipleUndirected((ListValue) value);
        }
    }

    /**
     * Adds multiple undirected relationships to the path
     *
     * @param relationships the undirected relationships to add
     */
    @CalledFromGeneratedCode
    public void addMultipleUndirected(ListValue relationships) {
        if (relationships.isEmpty()) {
            // nothing to add
            return;
        }

        relationships.forEach(this::addUndirected);
    }

    public void addNoValue() {
        this.seenNoValue = true;
    }

    @Override
    public void accept(RelationshipVisitor relationshipVisitor) {
        dbAccess.singleRelationship(relationshipVisitor.id(), cursor);
        // This ignores that a relationship might have been deleted here, this is weird but it is backwards compatible
        cursor.next();
        relationshipVisitor.visit(cursor.sourceNodeReference(), cursor.targetNodeReference(), cursor.type());
    }

    private boolean notNoValue(AnyValue value) {
        if (!seenNoValue && value == NO_VALUE) {
            seenNoValue = true;
        }
        return !seenNoValue;
    }
}
