/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.core;

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.indexOf;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.helpers.Nodes;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.PropertySelection;

public abstract class AbstractNodeEntity implements Node {

    @Override
    public ResourceIterable<Relationship> getRelationships() {
        return getRelationships(Direction.BOTH);
    }

    @Override
    public ResourceIterable<Relationship> getRelationships(RelationshipType... types) {
        return getRelationships(Direction.BOTH, types);
    }

    @Override
    public boolean hasRelationship() {
        return hasRelationship(Direction.BOTH);
    }

    @Override
    public boolean hasRelationship(RelationshipType... types) {
        return hasRelationship(Direction.BOTH, types);
    }

    protected Relationship getSingleRelationship(
            ResourceIterator<Relationship> relationships, RelationshipType type, Direction dir) {
        if (!relationships.hasNext()) {
            return null;
        }

        Relationship rel = relationships.next();
        while (relationships.hasNext()) {
            throw new NotFoundException("More than one relationship[" + type + ", " + dir + "] found for " + this);
        }
        return rel;
    }

    protected Iterable<RelationshipType> getRelationshipTypes(NodeCursor nodes) {
        try {
            singleNode(nodes);
            Degrees degrees = nodes.degrees(ALL_RELATIONSHIPS);
            List<RelationshipType> types = new ArrayList<>();
            for (int type : degrees.types()) {
                // only include this type if there are any relationships with this type
                if (degrees.totalDegree(type) > 0) {
                    types.add(RelationshipType.withName(tokenRead().relationshipTypeName(type)));
                }
            }

            return types;
        } catch (KernelException e) {
            throw new NotFoundException("Relationship name not found.", e);
        }
    }

    protected static int[] relTypeIds(TokenRead token, RelationshipType... types) {
        int[] ids = new int[types.length];
        int outIndex = 0;
        for (RelationshipType type : types) {
            int id = token.relationshipType(type.name());
            if (id != NO_SUCH_RELATIONSHIP_TYPE) {
                ids[outIndex++] = id;
            }
        }

        if (outIndex != ids.length) {
            // One or more relationship types do not exist, so we can exclude them right away.
            ids = Arrays.copyOf(ids, outIndex);
        }
        return ids;
    }

    protected boolean hasProperty(String key, NodeCursor nodes, PropertyCursor properties) {
        if (null == key) {
            return false;
        }

        int propertyKey = tokenRead().propertyKey(key);
        if (propertyKey == TokenRead.NO_TOKEN) {
            return false;
        }

        singleNode(nodes);
        nodes.properties(properties, PropertySelection.selection(propertyKey));
        return properties.next();
    }

    protected Object getProperty(String key, NodeCursor nodes, PropertyCursor properties) {
        if (null == key) {
            throw new IllegalArgumentException("(null) property key is not allowed");
        }
        int propertyKey = tokenRead().propertyKey(key);
        if (propertyKey == TokenRead.NO_TOKEN) {
            throw new NotFoundException(format("No such property, '%s'.", key));
        }

        singleNode(nodes);
        nodes.properties(properties, PropertySelection.selection(propertyKey));
        if (!properties.next()) {
            throw new NotFoundException(format("No such property, '%s'.", key));
        }
        return properties.propertyValue().asObjectCopy();
    }

    protected Object getProperty(String key, Object defaultValue, NodeCursor nodes, PropertyCursor properties) {
        if (null == key) {
            throw new IllegalArgumentException("(null) property key is not allowed");
        }
        int propertyKey = tokenRead().propertyKey(key);
        if (propertyKey == TokenRead.NO_TOKEN) {
            return defaultValue;
        }
        singleNode(nodes);
        nodes.properties(properties, PropertySelection.selection(propertyKey));
        return properties.next() ? properties.propertyValue().asObjectCopy() : defaultValue;
    }

    protected Iterable<String> getPropertyKeys(NodeCursor nodes, PropertyCursor properties) {
        List<String> keys = new ArrayList<>();
        try {
            singleNode(nodes);
            TokenRead token = tokenRead();
            nodes.properties(properties, ALL_PROPERTIES);
            while (properties.next()) {
                keys.add(token.propertyKeyName(properties.propertyKey()));
            }
        } catch (PropertyKeyIdNotFoundKernelException e) {
            throw new IllegalStateException("Property key retrieved through kernel API should exist.", e);
        }
        return keys;
    }

    protected Map<String, Object> getProperties(NodeCursor nodes, PropertyCursor properties, String... keys) {
        Objects.requireNonNull(keys, "Properties keys should be not null array.");

        if (keys.length == 0) {
            return Collections.emptyMap();
        }

        int[] propertyIds = propertyIds(tokenRead(), keys);
        Map<String, Object> result = new HashMap<>(propertyIds.length);
        singleNode(nodes);
        nodes.properties(properties, PropertySelection.selection(propertyIds));
        while (properties.next()) {
            result.put(
                    keys[indexOf(propertyIds, properties.propertyKey())],
                    properties.propertyValue().asObjectCopy());
        }
        return result;
    }

    public Map<String, Object> getAllProperties(NodeCursor nodes, PropertyCursor properties) {
        Map<String, Object> result = new HashMap<>();
        try {
            TokenRead token = tokenRead();
            singleNode(nodes);
            nodes.properties(properties, ALL_PROPERTIES);
            while (properties.next()) {
                result.put(
                        token.propertyKeyName(properties.propertyKey()),
                        properties.propertyValue().asObjectCopy());
            }
        } catch (PropertyKeyIdNotFoundKernelException e) {
            throw new IllegalStateException("Property key retrieved through kernel API should exist.", e);
        }
        return result;
    }

    protected int getDegree(NodeCursor nodes) {
        singleNode(nodes);
        return Nodes.countAll(nodes);
    }

    protected int getDegree(RelationshipType type, NodeCursor nodes) {
        int typeId = tokenRead().relationshipType(type.name());
        if (typeId == NO_TOKEN) { // This type doesn't even exist. Return 0
            return 0;
        }

        singleNode(nodes);
        return Nodes.countAll(nodes, typeId);
    }

    protected int getDegree(Direction direction, NodeCursor nodes) {
        singleNode(nodes);
        return switch (direction) {
            case OUTGOING -> Nodes.countOutgoing(nodes);
            case INCOMING -> Nodes.countIncoming(nodes);
            case BOTH -> Nodes.countAll(nodes);
        };
    }

    protected int getDegree(RelationshipType type, Direction direction, NodeCursor nodes) {
        int typeId = tokenRead().relationshipType(type.name());
        if (typeId == NO_TOKEN) { // This type doesn't even exist. Return 0
            return 0;
        }

        singleNode(nodes);
        return switch (direction) {
            case OUTGOING -> Nodes.countOutgoing(nodes, typeId);
            case INCOMING -> Nodes.countIncoming(nodes, typeId);
            case BOTH -> Nodes.countAll(nodes, typeId);
        };
    }

    protected boolean hasLabel(Label label, NodeCursor nodes) {
        int labelId = tokenRead().nodeLabel(label.name());
        if (labelId == NO_SUCH_LABEL) {
            return false;
        }
        singleNode(nodes);
        return nodes.hasLabel(labelId);
    }

    public Iterable<Label> getLabels(NodeCursor nodes) {
        try {
            singleNode(nodes);
            TokenSet tokenSet = nodes.labels();
            TokenRead tokenRead = tokenRead();
            List<Label> list = new ArrayList<>(tokenSet.numberOfTokens());
            for (int i = 0; i < tokenSet.numberOfTokens(); i++) {
                list.add(label(tokenRead.nodeLabelName(tokenSet.token(i))));
            }
            return list;

        } catch (LabelNotFoundKernelException e) {
            throw new IllegalStateException("Label retrieved through kernel API should exist.", e);
        }
    }

    private int[] propertyIds(TokenRead tokenRead, String... keys) {
        // Find ids, note we are betting on that the number of keys
        // is small enough not to use a set here.
        int[] propertyIds = new int[keys.length];
        for (int i = 0; i < propertyIds.length; i++) {
            String key = keys[i];
            if (key == null) {
                throw new NullPointerException(String.format("Key %d was null", i));
            }
            propertyIds[i] = tokenRead.propertyKey(key);
        }

        return propertyIds;
    }

    protected abstract TokenRead tokenRead();

    protected abstract void singleNode(NodeCursor nodes);
}
