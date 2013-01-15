/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.kernelimpl;

import org.neo4j.cypher.internal.spi.QueryContext;
import org.neo4j.cypher.internal.spi.gdsimpl.GDSBackedQueryContext;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.StatementContext;

public class KernelBackedQueryContext implements QueryContext {

    private final StatementContext ctx;
    private final GDSBackedQueryContext gdsQueryContext;

    public KernelBackedQueryContext(GraphDatabaseService graph, StatementContext ctx) {

        if (graph == null)
            this.gdsQueryContext = null;
        else
            this.gdsQueryContext = new GDSBackedQueryContext(graph);
        this.ctx = ctx;
    }

    @Override
    public Operations<Node> nodeOps() {
        return gdsQueryContext.nodeOps();
    }

    @Override
    public Operations<Relationship> relationshipOps() {
        return gdsQueryContext.relationshipOps();
    }

    @Override
    public Node createNode() {
        return gdsQueryContext.createNode();
    }

    @Override
    public Relationship createRelationship(Node start, Node end, String relType) {
        return gdsQueryContext.createRelationship(start, end, relType);
    }

    @Override
    public Iterable<Relationship> getRelationshipsFor(Node node, Direction dir, String... types) {
        return gdsQueryContext.getRelationshipsFor(node, dir, types);
    }

    @Override
    public void addLabelsToNode(Node node, Iterable<Long> labelIds) {
        for (Long labelId : labelIds) {
            ctx.addLabelToNode(labelId, node.getId());
        }
    }

    @Override
    public Long getOrCreateLabelId(String labelName) {
        return ctx.getOrCreateLabelId(labelName);
    }

    @Override
    public void close() {
        gdsQueryContext.close();
        ctx.close();
    }
}
