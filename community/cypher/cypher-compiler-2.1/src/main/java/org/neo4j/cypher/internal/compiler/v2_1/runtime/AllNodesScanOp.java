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


import org.neo4j.cypher.internal.compiler.v2_1.spi.StatementContext;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public class AllNodesScanOp implements Operator {

    private final EntityRegister nodeRegister;
    private final PrimitiveLongIterator allNodes;

    public AllNodesScanOp( StatementContext ctx, EntityRegister nodeRegister ) {
        this.nodeRegister = nodeRegister;
        this.allNodes = ctx.FAKE_nodeGetAll();
    }

    @Override
    public void open() {
    }

    @Override
    public boolean next() {
        if ( !allNodes.hasNext() )
        {
            return false;
        }

        nodeRegister.setEntity( allNodes.next() );

        return true;
    }

    @Override
    public void close() {
    }
}
