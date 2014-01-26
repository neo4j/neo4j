/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.spi.v2_1;

import org.neo4j.cypher.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

import java.util.Iterator;

public class StatementContext implements org.neo4j.cypher.internal.compiler.v2_1.spi.StatementContext {
    private final Statement statement;
    private final GraphDatabaseService graph;

    public StatementContext(Statement statement, GraphDatabaseService graph) {
        this.statement = statement;
        this.graph = graph;
    }

    @Override
    public PrimitiveLongIterator FAKEgetAllNodes() {
        return new PrimitiveLongIterator() {

            final Iterator<Node> allNodes = graph.getAllNodes().iterator();

            @Override
            public boolean hasNext() {
                return allNodes.hasNext();
            }

            @Override
            public long next() {
                return allNodes.next().getId();
            }
        };
    }

    @Override
    public PrimitiveLongIterator FAKEgetNodesRelatedBy(final long fromNodeId, final Direction dir) {
        final org.neo4j.graphdb.Direction dir1 = asKernelDirection(dir);
        return new PrimitiveLongIterator() {
            final Node fromNode = graph.getNodeById(fromNodeId);
            final Iterator<Relationship> allNodes = fromNode.getRelationships(dir1).iterator();

            @Override
            public boolean hasNext() {
                return allNodes.hasNext();
            }

            @Override
            public long next() {
                return allNodes.next().getOtherNode(fromNode).getId();
            }
        };
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel(int labelToken) {
        return statement.readOperations().nodesGetForLabel(labelToken);
    }

    private org.neo4j.graphdb.Direction asKernelDirection(Direction dir) {
        if (dir == Direction.OUTGOING$.MODULE$) return org.neo4j.graphdb.Direction.OUTGOING;
        if (dir == Direction.INCOMING$.MODULE$) return org.neo4j.graphdb.Direction.INCOMING;
        return org.neo4j.graphdb.Direction.BOTH;
    }
}
