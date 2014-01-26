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
package org.neo4j.cypher.internal.compiler.v2_1.runtime;

import org.neo4j.cypher.Direction;
import org.neo4j.cypher.internal.compiler.v2_1.spi.StatementContext;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public class ExpandToNodeOp implements Operator {
    private final StatementContext ctx;
    private final int sourceIdx;
    private final int dstIdx;
    private final Operator sourceOp;
    private final Registers registers;
    private final Direction dir;
    private PrimitiveLongIterator currentNodes;

    public ExpandToNodeOp(StatementContext ctx, int sourceIdx, int dstIdx, Operator sourceOp, Registers registers, Direction dir) {
        this.ctx = ctx;
        this.sourceIdx = sourceIdx;
        this.dstIdx = dstIdx;
        this.sourceOp = sourceOp;
        this.registers = registers;
        this.dir = dir;
        this.currentNodes = IteratorUtil.emptyPrimitiveLongIterator();
    }

    @Override
    public void open() {
        sourceOp.open();
    }

    @Override
    public boolean next() {
        while (!currentNodes.hasNext() && sourceOp.next()) {
            long fromNodeId = registers.getLongRegister(sourceIdx);
            currentNodes = ctx.FAKEgetNodesRelatedBy(fromNodeId, dir);
        }

        if (!currentNodes.hasNext())
            return false;

        long nextNode = currentNodes.next();
        registers.setLongRegister(dstIdx, nextNode);

        return true;
    }

    @Override
    public void close() {
        sourceOp.close();
    }
}
