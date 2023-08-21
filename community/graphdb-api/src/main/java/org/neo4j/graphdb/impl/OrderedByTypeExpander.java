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
package org.neo4j.graphdb.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.internal.helpers.collection.NestingResourceIterator;
import org.neo4j.internal.helpers.collection.ResourceClosingIterator;

public final class OrderedByTypeExpander extends StandardExpander.RegularExpander {
    private final Collection<DirectedRelationshipType> orderedTypes;

    public OrderedByTypeExpander() {
        this(Collections.emptyList());
    }

    private OrderedByTypeExpander(Collection<DirectedRelationshipType> orderedTypes) {
        super(Collections.emptyMap());
        this.orderedTypes = orderedTypes;
    }

    @Override
    public StandardExpander add(RelationshipType type, Direction direction) {
        Collection<DirectedRelationshipType> newTypes = new ArrayList<>(orderedTypes);
        newTypes.add(new DirectedRelationshipType(type, direction));
        return new OrderedByTypeExpander(newTypes);
    }

    @Override
    public StandardExpander remove(RelationshipType type) {
        Collection<DirectedRelationshipType> newTypes = new ArrayList<>();
        for (DirectedRelationshipType directedType : orderedTypes) {
            if (!type.name().equals(directedType.type.name())) {
                newTypes.add(directedType);
            }
        }
        return new OrderedByTypeExpander(newTypes);
    }

    @Override
    void buildString(StringBuilder result) {
        result.append(orderedTypes);
    }

    @Override
    public StandardExpander reverse() {
        Collection<DirectedRelationshipType> newTypes = new ArrayList<>(orderedTypes.size());
        for (DirectedRelationshipType directedType : orderedTypes) {
            newTypes.add(directedType.reverse());
        }
        return new OrderedByTypeExpander(newTypes);
    }

    @Override
    RegularExpander createNew(Map<Direction, RelationshipType[]> newTypes) {
        throw new UnsupportedOperationException();
    }

    @Override
    ResourceIterator<Relationship> doExpand(final Path path, BranchState state) {
        final Node node = path.endNode();
        return new NestingResourceIterator<>(orderedTypes.iterator()) {
            @Override
            protected ResourceIterator<Relationship> createNestedIterator(DirectedRelationshipType directedType) {
                RelationshipType type = directedType.type();
                Direction dir = directedType.direction();
                ResourceIterable<Relationship> relationships =
                        (dir == Direction.BOTH) ? node.getRelationships(type) : node.getRelationships(dir, type);
                return ResourceClosingIterator.fromResourceIterable(relationships);
            }
        };
    }

    private record DirectedRelationshipType(RelationshipType type, Direction direction) {
        DirectedRelationshipType reverse() {
            return new DirectedRelationshipType(type, direction.reverse());
        }
    }
}
