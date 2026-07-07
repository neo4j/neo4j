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
package org.neo4j.kernel.impl.util;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.CompositeDatabaseValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public class DefaultValueMapper extends ValueMapper.JavaMapper {
    private static final String COMPOSITE_UNSUPPORTED_OPERATION_MESSAGE =
            "Graph access operations are not supported on composite databases.";

    private final InternalTransaction transaction;

    public DefaultValueMapper(InternalTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public Node mapNode(VirtualNodeValue value) {
        if (value instanceof WrappingEntity<?> wrappingEntity) {
            // this is the back door through which "virtual nodes" slip
            return (Node) wrappingEntity.getEntity();
        }
        if (value instanceof CompositeDatabaseValue.CompositeGraphDirectNodeValue compositeNode) {
            return new CompositeMaterializedNode(compositeNode, this, COMPOSITE_UNSUPPORTED_OPERATION_MESSAGE);
        }
        return mapNode(value.id());
    }

    @Override
    public Relationship mapRelationship(VirtualRelationshipValue value) {
        if (value instanceof WrappingEntity<?> wrappingEntity) {
            // this is the back door through which "virtual relationships" slip
            return (Relationship) wrappingEntity.getEntity();
        }
        if (value instanceof CompositeDatabaseValue.CompositeDirectRelationshipValue compositeRelationship) {
            return new CompositeMaterializedRelationship(
                    compositeRelationship, this, COMPOSITE_UNSUPPORTED_OPERATION_MESSAGE);
        }
        return mapRelationship(value.id());
    }

    @Override
    public Path mapPath(VirtualPathValue value) {
        if (value instanceof WrappingPath wrappedPath) {
            return wrappedPath.path();
        }
        return new CoreAPIPath(value);
    }

    private Node mapNode(long value) {
        return new NodeEntity(transaction, value);
    }

    private Relationship mapRelationship(long value) {
        return new RelationshipEntity(transaction, value);
    }

    private class CoreAPIPath extends BaseCoreAPIPath {

        CoreAPIPath(VirtualPathValue value) {
            super(value);
        }

        @Override
        protected Node mapNode(long value) {
            return DefaultValueMapper.this.mapNode(value);
        }

        @Override
        protected Relationship mapRelationship(long value) {
            return DefaultValueMapper.this.mapRelationship(value);
        }
    }
}
