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

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.indexOf;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Arrays;
import java.util.function.Consumer;

import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.EntityNotFoundException;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.EntityCursor;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.newapi.Cursors;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.RelationshipVisitor;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * Utilities for working with cursors from within generated code
 */
@SuppressWarnings({"Duplicates"})
public final class CursorUtils {
    /**
     * Do not instantiate this class
     */
    private CursorUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * Fetches a given property from a node
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return The value of the given property
     * @throws EntityNotFoundException If the node was deleted in transaction.
     */
    public static Value nodeGetProperty(
            Read read, NodeCursor nodeCursor, long node, PropertyCursor propertyCursor, int prop)
            throws EntityNotFoundException {
        assert node >= NO_SUCH_NODE;
        return nodeGetProperty(read, nodeCursor, node, propertyCursor, prop, true);
    }

    /**
     * Fetches a given property from a node
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @param throwOnDeleted if <code>true</code> and exception will be thrown if node has been deleted
     * @return The value of the given property
     * @throws EntityNotFoundException If the node was deleted in transaction.
     */
    public static Value nodeGetProperty(
            Read read,
            NodeCursor nodeCursor,
            long node,
            PropertyCursor propertyCursor,
            int prop,
            boolean throwOnDeleted)
            throws EntityNotFoundException {
        assert node >= NO_SUCH_NODE;

        if (node == NO_SUCH_NODE) {
            return NO_VALUE;
        }
        if (prop == NO_SUCH_PROPERTY_KEY) {
            return NO_VALUE;
        }
        read.singleNode(node, nodeCursor);
        if (!nodeCursor.next()) {
            if (throwOnDeleted && read.nodeDeletedInTransaction(node)) {
                throw new EntityNotFoundException(
                        String.format("Node with id %d has been deleted in this transaction", node));
            } else {
                return NO_VALUE;
            }
        }
        return nodeGetProperty(nodeCursor, propertyCursor, prop);
    }

    /**
     * Fetches a given property from a node, where the node has already been loaded.
     *
     * @param nodeCursor The node cursor which currently points to the node to get the property from.
     * @param propertyCursor The property cursor to use to read the property.
     * @param prop The property key id
     * @return The value of the property, otherwise {@link Values#NO_VALUE} if not found.
     */
    public static Value nodeGetProperty(NodeCursor nodeCursor, PropertyCursor propertyCursor, int prop) {
        if (prop == NO_SUCH_PROPERTY_KEY) {
            return NO_VALUE;
        }
        nodeCursor.properties(propertyCursor, PropertySelection.selection(prop));
        return propertyCursor.next() ? propertyCursor.propertyValue() : NO_VALUE;
    }

    /**
     * Returns a set of property values (or NoValue) from the specified entity cursor.
     *
     * Note, NoValue will be used for tokens that does not exist, including TokenConstants.NO_TOKEN.
     */
    public static Value[] entityGetProperties(EntityCursor entityCursor, PropertyCursor propertyCursor, int[] tokens) {
        assert entityCursor.reference() != StatementConstants.NO_SUCH_ENTITY;

        final Value[] values = emptyPropertyArray(tokens.length);
        entityCursor.properties(propertyCursor, PropertySelection.selection(tokens));
        while (propertyCursor.next()) {
            final int index = indexOf(tokens, propertyCursor.propertyKey());
            values[index] = propertyCursor.propertyValue();
        }
        return values;
    }

    public static Value[] emptyPropertyArray(int len) {
        Value[] values = new Value[len];
        Arrays.fill(values, NO_VALUE);
        return values;
    }

    /**
     * Checks if a given node has the given property
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return <code>true</code> if node has property otherwise <code>false</code>
     */
    public static boolean nodeHasProperty(
            Read read, NodeCursor nodeCursor, long node, PropertyCursor propertyCursor, int prop)
            throws EntityNotFoundException {
        if (prop == NO_SUCH_PROPERTY_KEY) {
            return false;
        }
        read.singleNode(node, nodeCursor);
        if (!nodeCursor.next()) {
            return false;
        }
        return nodeHasProperty(nodeCursor, propertyCursor, prop);
    }

    /**
     * Checks if a given node has the given property, where the node has already been loaded.
     *
     * @param nodeCursor The node cursor which currently points to the node to check property existence for.
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return {@code true} if node has property otherwise {@code false}.
     */
    public static boolean nodeHasProperty(NodeCursor nodeCursor, PropertyCursor propertyCursor, int prop) {
        if (prop == NO_SUCH_PROPERTY_KEY) {
            return false;
        }
        nodeCursor.properties(propertyCursor, PropertySelection.onlyKeysSelection(prop));
        return propertyCursor.next();
    }

    /**
     * Checks if given node has a given label.
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param label The id of the label
     * @return {@code true} if the node has the label, otherwise {@code false}
     */
    public static boolean nodeHasLabel(Read read, NodeCursor nodeCursor, long node, int label) {
        if (label == NO_SUCH_LABEL) {
            return false;
        }
        read.singleNode(node, nodeCursor);
        if (!nodeCursor.next()) {
            return false;
        }

        return nodeCursor.hasLabel(label);
    }

    /**
     * Checks if a given node has all the given labels
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param labels The labels to check for
     * @return {@code true} if the node has all the labels, otherwise {@code false}
     */
    public static boolean nodeHasLabels(Read read, NodeCursor nodeCursor, long node, int[] labels) {
        read.singleNode(node, nodeCursor);
        if (!nodeCursor.next()) {
            return false;
        }

        return nodeHasLabels(nodeCursor, labels);
    }

    /**
     * Checks if a given node has all the given labels
     * @param nodeCursor A node cursor positioned on a particular node
     * @param labels The labels to check for
     * @return {@code true} if the node has all the labels, otherwise {@code false}
     */
    public static boolean nodeHasLabels(NodeCursor nodeCursor, int[] labels) {
        for (int label : labels) {
            if (label == NO_SUCH_LABEL) {
                return false;
            }
            if (!nodeCursor.hasLabel(label)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if given node has any label at all.
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @return {@code true} if the node has the label, otherwise {@code false}
     */
    public static boolean nodeHasALabel(Read read, NodeCursor nodeCursor, long node) {
        read.singleNode(node, nodeCursor);
        if (!nodeCursor.next()) {
            return false;
        }

        return nodeCursor.hasLabel();
    }

    /**
     * Checks if given node has any label at all.
     *
     * @param nodeCursor The node cursor to use
     * @return {@code true} if the node has the label, otherwise {@code false}
     */
    public static boolean nodeHasALabel(NodeCursor nodeCursor) {
        return nodeCursor.hasLabel();
    }

    /**
     * Returns true if any of the specified labels are set on the node with id `node`.
     */
    public static boolean nodeHasAnyLabel(Read read, NodeCursor nodeCursor, long node, int[] labels) {
        read.singleNode(node, nodeCursor);
        if (!nodeCursor.next()) {
            return false;
        }

        return nodeHasAnyLabel(nodeCursor, labels);
    }

    /**
     * Returns true if any of the specified labels are set on the node that `cursor` is pointing at.
     */
    public static boolean nodeHasAnyLabel(NodeCursor cursor, int[] labels) {
        var nodeLabels = cursor.labels();

        for (int label : labels) {
            if (nodeLabels.contains(label)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if given relationship has a given type.
     *
     * @param read The current Read instance
     * @param relationshipCursor The relationship cursor to use
     * @param relationship The id of the relationship
     * @param type The id of the type
     * @return {@code true} if the relationship has the type, otherwise {@code false}
     */
    @CalledFromGeneratedCode
    public static boolean relationshipHasType(
            Read read, RelationshipScanCursor relationshipCursor, long relationship, int type) {
        if (type == NO_SUCH_RELATIONSHIP_TYPE) {
            return false;
        }
        read.singleRelationship(relationship, relationshipCursor);
        if (!relationshipCursor.next()) {
            return false;
        }

        return relationshipCursor.type() == type;
    }

    /**
     * Checks if given relationship has a given type.
     *
     * @param read The current Read instance
     * @param relationshipCursor The relationship cursor to use
     * @param relationship The id of the relationship
     * @param types The types to check for
     * @return {@code true} if the relationship has the type, otherwise {@code false}
     */
    public static boolean relationshipHasTypes(
            Read read, RelationshipScanCursor relationshipCursor, long relationship, int[] types) {
        assert types.length > 0;
        int typeToLookFor = types[0];
        for (int i = 1; i < types.length; i++) {
            if (types[i] != typeToLookFor) {
                return false;
            }
        }
        if (typeToLookFor == NO_SUCH_RELATIONSHIP_TYPE) {
            return false;
        }

        read.singleRelationship(relationship, relationshipCursor);
        if (!relationshipCursor.next()) {
            return false;
        }

        return relationshipCursor.type() == typeToLookFor;
    }

    public static boolean relationshipHasTypes(
            Read read, RelationshipScanCursor relationshipCursor, VirtualRelationshipValue relationship, int[] types) {
        assert types.length > 0;
        int typeToLookFor = types[0];
        for (int i = 1; i < types.length; i++) {
            if (types[i] != typeToLookFor) {
                return false;
            }
        }
        if (typeToLookFor == NO_SUCH_RELATIONSHIP_TYPE) {
            return false;
        }

        return new VirtualRelationshipReader(read, relationshipCursor, relationship, true).hasType(typeToLookFor);
    }

    @CalledFromGeneratedCode
    public static boolean relationshipHasTypes(RelationshipScanCursor relationshipCursor, int[] types) {
        assert types.length > 0;
        int typeToLookFor = types[0];
        for (int i = 1; i < types.length; i++) {
            if (types[i] != typeToLookFor) {
                return false;
            }
        }
        if (typeToLookFor == NO_SUCH_RELATIONSHIP_TYPE) {
            return false;
        }

        return relationshipCursor.type() == typeToLookFor;
    }

    public static RelationshipTraversalCursor nodeGetRelationships(
            Read read,
            CursorFactory cursors,
            NodeCursor node,
            long nodeId,
            Direction direction,
            int[] types,
            CursorContext cursorContext) {
        read.singleNode(nodeId, node);
        if (!node.next()) {
            return Cursors.emptyTraversalCursor(read);
        }
        return switch (direction) {
            case OUTGOING -> RelationshipSelections.outgoingCursor(cursors, node, types, cursorContext);
            case INCOMING -> RelationshipSelections.incomingCursor(cursors, node, types, cursorContext);
            case BOTH -> RelationshipSelections.allCursor(cursors, node, types, cursorContext);
        };
    }

    /**
     * Fetches a given property from a relationship
     *
     * @param read The current Read instance
     * @param relationshipCursor The relationship cursor to use
     * @param relationship The id of the relationship
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return The value of the given property
     * @throws EntityNotFoundException If the node cannot be find.
     */
    public static Value relationshipGetProperty(
            Read read,
            RelationshipScanCursor relationshipCursor,
            long relationship,
            PropertyCursor propertyCursor,
            int prop)
            throws EntityNotFoundException {
        return relationshipGetProperty(read, relationshipCursor, relationship, propertyCursor, prop, true);
    }

    /**
     * Fetches a given property from a relationship
     *
     * @param read The current Read instance
     * @param relationshipCursor The relationship cursor to use
     * @param relationship The id of the relationship
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @param throwOnDeleted if <code>true</code> and exception will be thrown if node has been deleted
     * @return The value of the given property
     * @throws EntityNotFoundException If the node cannot be find.
     */
    public static Value relationshipGetProperty(
            Read read,
            RelationshipScanCursor relationshipCursor,
            long relationship,
            PropertyCursor propertyCursor,
            int prop,
            boolean throwOnDeleted)
            throws EntityNotFoundException {
        assert relationship >= NO_SUCH_RELATIONSHIP;

        if (relationship == NO_SUCH_RELATIONSHIP) {
            return NO_VALUE;
        }
        if (prop == NO_SUCH_PROPERTY_KEY) {
            return NO_VALUE;
        }
        read.singleRelationship(relationship, relationshipCursor);
        if (!relationshipCursor.next()) {
            if (throwOnDeleted && read.relationshipDeletedInTransaction(relationship)) {
                throw new EntityNotFoundException(
                        String.format("Relationship with id %d has been deleted in this transaction", relationship));
            } else {
                return NO_VALUE;
            }
        }
        relationshipCursor.properties(propertyCursor, PropertySelection.selection(prop));
        return propertyCursor.next() ? propertyCursor.propertyValue() : NO_VALUE;
    }

    /**
     * Fetches a given property from a relationship
     *
     * @param read The current Read instance
     * @param relationshipCursor The relationship cursor to use
     * @param relationship The id of the relationship
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @param throwOnDeleted if <code>true</code> and exception will be thrown if node has been deleted
     * @return The value of the given property
     * @throws EntityNotFoundException If the node cannot be find.
     */
    public static Value relationshipGetProperty(
            Read read,
            RelationshipScanCursor relationshipCursor,
            VirtualRelationshipValue relationship,
            PropertyCursor propertyCursor,
            int prop,
            boolean throwOnDeleted)
            throws EntityNotFoundException {
        assert relationship.id() >= NO_SUCH_RELATIONSHIP;

        if (relationship.id() == NO_SUCH_RELATIONSHIP) {
            return NO_VALUE;
        }
        if (prop == NO_SUCH_PROPERTY_KEY) {
            return NO_VALUE;
        }
        return new VirtualRelationshipReader(read, relationshipCursor, relationship, throwOnDeleted)
                .property(propertyCursor, prop);
    }

    /**
     * Fetches a given property from a relationship, where the relationship has already been loaded.
     *
     * @param relationshipCursor relationship cursor which currently points to the relationship to get the property from.
     * @param propertyCursor the property cursor to use to read the property.
     * @param prop property key id
     * @return the value of the property, otherwise {@link Values#NO_VALUE} if not found.
     */
    public static Value relationshipGetProperty(
            RelationshipDataAccessor relationshipCursor, PropertyCursor propertyCursor, int prop) {
        if (prop == NO_SUCH_PROPERTY_KEY) {
            return NO_VALUE;
        }
        relationshipCursor.properties(propertyCursor, PropertySelection.selection(prop));
        return propertyCursor.next() ? propertyCursor.propertyValue() : NO_VALUE;
    }

    /**
     * Checks if a given relationship has the given property
     *
     * @param read The current Read instance
     * @param relationshipCursor The relationship cursor to use
     * @param relationship The id of the relationship
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return <code>true</code> if relationship has property otherwise <code>false</code>
     */
    public static boolean relationshipHasProperty(
            Read read,
            RelationshipScanCursor relationshipCursor,
            long relationship,
            PropertyCursor propertyCursor,
            int prop)
            throws EntityNotFoundException {
        if (prop == NO_SUCH_PROPERTY_KEY) {
            return false;
        }
        read.singleRelationship(relationship, relationshipCursor);
        if (!relationshipCursor.next()) {
            return false;
        }
        relationshipCursor.properties(propertyCursor, PropertySelection.onlyKeysSelection(prop));
        return propertyCursor.next();
    }

    public static boolean relationshipHasProperty(
            Read read,
            RelationshipScanCursor relationshipCursor,
            VirtualRelationshipValue relationship,
            PropertyCursor propertyCursor,
            int prop)
            throws EntityNotFoundException {
        if (prop == NO_SUCH_PROPERTY_KEY) {
            return false;
        }
        return new VirtualRelationshipReader(read, relationshipCursor, relationship, true)
                .hasProperty(propertyCursor, prop);
    }

    /**
     * Checks if a given relationship has the given property, where the relationship has already been loaded.
     *
     * @param relationshipCursor The relationship cursor which currently points to the relationship to check property existence for.
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return {@code true} if relationship has property otherwise {@code false}.
     */
    @CalledFromGeneratedCode
    public static boolean relationshipHasProperty(
            RelationshipDataAccessor relationshipCursor, PropertyCursor propertyCursor, int prop) {
        if (prop == NO_SUCH_PROPERTY_KEY) {
            return false;
        }
        relationshipCursor.properties(propertyCursor, PropertySelection.onlyKeysSelection(prop));
        return propertyCursor.next();
    }

    @CalledFromGeneratedCode
    public static int[] relationshipPropertyIds(
            Read read,
            VirtualRelationshipValue relationship,
            RelationshipScanCursor cursor,
            PropertyCursor propertyCursor) {
        return new VirtualRelationshipReader(read, cursor, relationship).propertyIds(propertyCursor);
    }

    @CalledFromGeneratedCode
    public static MapValue relationshipAsMap(
            Read read,
            TokenRead tokenRead,
            VirtualRelationshipValue relationship,
            RelationshipScanCursor cursor,
            PropertyCursor propertyCursor,
            MapValueBuilder seenProperties,
            IntSet seenPropertyTokens)
            throws PropertyKeyIdNotFoundKernelException {
        return new VirtualRelationshipReader(read, cursor, relationship).asMap(tokenRead, propertyCursor, seenProperties, seenPropertyTokens);
    }

    @CalledFromGeneratedCode
    public static AnyValue propertyGet(
            String key,
            AnyValue container,
            Read read,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof VirtualNodeValue node) {
            return nodeGetProperty(read, nodeCursor, node.id(), propertyCursor, dbAccess.propertyKey(key));
        } else if (container instanceof VirtualRelationshipValue rel) {
            return relationshipGetProperty(
                    read, relationshipScanCursor, rel, propertyCursor, dbAccess.propertyKey(key), true);
        } else if (container instanceof MapValue map) {
            return map.get(key);
        } else if (container instanceof TemporalValue<?, ?> temporal) {
            return temporal.get(key);
        } else if (container instanceof DurationValue duration) {
            return duration.get(key);
        } else if (container instanceof PointValue point) {
            return point.get(key);
        } else {
            throw new CypherTypeException(format("Type mismatch: expected a map but was %s", container), null);
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue[] propertiesGet(
            String[] keys,
            AnyValue container,
            Read read,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (container == NO_VALUE) {
            return emptyPropertyArray(keys.length);
        } else if (container instanceof VirtualNodeValue node) {
            return propertiesGet(propertyKeys(keys, dbAccess), node.id(), read, nodeCursor, propertyCursor);
        } else if (container instanceof VirtualRelationshipValue rel) {
            return propertiesGet(propertyKeys(keys, dbAccess), rel, read, relationshipScanCursor, propertyCursor);
        } else {
            return propertiesGet(keys, container);
        }
    }

    public static AnyValue[] propertiesGet(String[] keys, AnyValue container) {
        if (container instanceof MapValue map) {
            return propertiesGet(keys, map);
        } else if (container instanceof TemporalValue<?, ?> temporal) {
            return propertiesGet(keys, temporal);
        } else if (container instanceof DurationValue duration) {
            return propertiesGet(keys, duration);
        } else if (container instanceof PointValue point) {
            return propertiesGet(keys, point);
        } else {
            throw new CypherTypeException(format("Type mismatch: expected a map but was %s", container), null);
        }
    }

    public static Value[] propertiesGet(
            int[] keys, long node, Read read, NodeCursor nodeCursor, PropertyCursor propertyCursor) {
        read.singleNode(node, nodeCursor);
        if (nodeCursor.next()) {
            return entityGetProperties(nodeCursor, propertyCursor, keys);
        } else if (read.nodeDeletedInTransaction(node)) {
            throw new EntityNotFoundException(
                    String.format("Node with id %d has been deleted in this transaction", node));
        } else {
            return emptyPropertyArray(keys.length);
        }
    }

    public static Value[] propertiesGet(
            int[] keys, long rel, Read read, RelationshipScanCursor relCursor, PropertyCursor propertyCursor) {
        read.singleRelationship(rel, relCursor);
        if (relCursor.next()) {
            return entityGetProperties(relCursor, propertyCursor, keys);
        } else if (read.relationshipDeletedInTransaction(rel)) {
            throw new EntityNotFoundException(
                    String.format("Relationship with id %d has been deleted in this transaction", rel));
        } else {
            return emptyPropertyArray(keys.length);
        }
    }

    public static Value[] propertiesGet(
            int[] keys,
            VirtualRelationshipValue rel,
            Read read,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor) {
        return new VirtualRelationshipReader(read, relCursor, rel, true).properties(keys, propertyCursor);
    }

    public static int[] propertyKeys(String[] keys, DbAccess dbAccess) {
        int[] tokens = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            tokens[i] = dbAccess.propertyKey(keys[i]);
        }
        return tokens;
    }

    public static AnyValue[] propertiesGet(String[] keys, MapValue map) {
        var result = new AnyValue[keys.length];
        for (int i = 0; i < keys.length; i++) {
            result[i] = map.get(keys[i]);
        }
        return result;
    }

    public static AnyValue[] propertiesGet(String[] keys, TemporalValue<?, ?> map) {
        var result = new AnyValue[keys.length];
        for (int i = 0; i < keys.length; i++) {
            result[i] = map.get(keys[i]);
        }
        return result;
    }

    public static AnyValue[] propertiesGet(String[] keys, DurationValue map) {
        var result = new AnyValue[keys.length];
        for (int i = 0; i < keys.length; i++) {
            result[i] = map.get(keys[i]);
        }
        return result;
    }

    public static AnyValue[] propertiesGet(String[] keys, PointValue map) {
        var result = new AnyValue[keys.length];
        for (int i = 0; i < keys.length; i++) {
            result[i] = map.get(keys[i]);
        }
        return result;
    }

    public static VirtualRelationshipValue relationshipById(RelationshipDataAccessor cursor) {
        return VirtualValues.relationship(
                cursor.relationshipReference(),
                cursor.sourceNodeReference(),
                cursor.targetNodeReference(),
                cursor.type());
    }

    /**
     * Helper class for efficient reading of data from a VirtualRelationshipValue.
     * <p>
     * This class should only be short-lived and should not be part of any long-lived state and
     * should only be used to invoke a single method, e.g.,
     *
     * <pre>{@code
     *  new VirtualRelationshipReader(read, cursor, rel).property(propCursor, prop);
     * }
     * </pre>
     */
    static final class VirtualRelationshipReader implements Consumer<RelationshipVisitor> {

        private final Read read;

        // NOTE: storing RelationshipScanCursor as state can be dangerous and this is one reason why we shouldn't keep
        // instances of this class around. The cursor needs to be kept as a field in order to implement `accept`.
        private final RelationshipScanCursor cursor;
        private final VirtualRelationshipValue relationship;
        private final boolean throwOnDeleted;

        private boolean isSet;

        VirtualRelationshipReader(Read read, RelationshipScanCursor cursor, VirtualRelationshipValue relationship) {
            this(read, cursor, relationship, true);
        }

        VirtualRelationshipReader(
                Read read,
                RelationshipScanCursor cursor,
                VirtualRelationshipValue relationship,
                boolean throwOnDeleted) {
            this.read = read;
            this.cursor = cursor;
            this.relationship = relationship;
            this.throwOnDeleted = throwOnDeleted;
            this.isSet = false;
        }

        @Override
        public void accept(RelationshipVisitor relationshipVisitor) {
            read.singleRelationship(relationship.id(), cursor);
            if (cursor.next()) {
                relationshipVisitor.visit(cursor.sourceNodeReference(), cursor.targetNodeReference(), cursor.type());
                this.isSet = true;
            }
        }

        private boolean next() {
            long start = relationship.startNodeId(this);
            long end = relationship.endNodeId(this);
            int type = relationship.relationshipTypeId(this);
            if (!isSet) {
                read.singleRelationship(relationship.id(), start, type, end, cursor);
                if (!cursor.next()) {
                    if (throwOnDeleted && read.relationshipDeletedInTransaction(relationship.id())) {
                        throw new EntityNotFoundException(String.format(
                                "Relationship with id %d has been deleted in this transaction", relationship.id()));
                    } else {
                        return false;
                    }
                }
            }
            return true;
        }

        public Value property(PropertyCursor propertyCursor, int prop) {
            if (next()) {
                cursor.properties(propertyCursor, PropertySelection.selection(prop));
                return propertyCursor.next() ? propertyCursor.propertyValue() : NO_VALUE;
            } else {
                return NO_VALUE;
            }
        }

        public Value[] properties(int[] keys, PropertyCursor propertyCursor) {
            if (next()) {
                return entityGetProperties(cursor, propertyCursor, keys);
            } else {
                return emptyPropertyArray(keys.length);
            }
        }

        public boolean hasProperty(PropertyCursor propertyCursor, int prop) {
            if (next()) {
                cursor.properties(propertyCursor, PropertySelection.onlyKeysSelection(prop));
                return propertyCursor.next();
            } else {
                return false;
            }
        }

        public boolean hasType(int typeToLookFor) {
            if (next()) {
                return cursor.type() == typeToLookFor;
            } else {
                return false;
            }
        }

        public int[] propertyIds(PropertyCursor propertyCursor) {
            if (next()) {
                var res = new IntArrayList();
                cursor.properties(propertyCursor);
                while (propertyCursor.next()) {
                    res.add(propertyCursor.propertyKey());
                }
                return res.toArray();
            } else {
                return EMPTY_INT_ARRAY;
            }
        }

        public MapValue asMap(TokenRead tokenRead, PropertyCursor propertyCursor, MapValueBuilder builder, IntSet seenTokens)
                throws PropertyKeyIdNotFoundKernelException {
            if (next()) {
                cursor.properties(propertyCursor, PropertySelection.ALL_PROPERTIES.excluding(seenTokens::contains));
                while (propertyCursor.next()) {
                    builder.add(
                            tokenRead.propertyKeyName(propertyCursor.propertyKey()), propertyCursor.propertyValue());
                }
                return builder.build();
            } else {
                return VirtualValues.EMPTY_MAP;
            }
        }
    }
}
